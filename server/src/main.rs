mod executor;
mod gui;

use clap::Parser;
use colored::Colorize;
use crossterm::event::EventStream;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use futures::SinkExt;
use futures::StreamExt;
use futures::channel::mpsc::UnboundedReceiver;
use indexmap::IndexMap;
use jiff::Timestamp;
use library::MapSender;
use library::TermMode;
use library::is_closing_key;
use library::messages::ClientInitialMessage;
use library::messages::ClientMessage;
use library::messages::CustomCodec;
use library::messages::Decode;
use library::messages::Encode;
use library::messages::ServerMessage;
use library::threads::{FFmpegProgress, StderrMessage, stderr_stream, stdout_stream};
use library::{CustomSender, LogLine};
use nokhwa::utils::CameraFormat;
use send_ctrlc::tokio::InterruptibleChild;
use send_ctrlc::{Interruptible, InterruptibleCommand};
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs::File, net::IpAddr, path::PathBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::runtime::Runtime;
use tokio::task::{JoinHandle, JoinSet};
use tokio::{net::UdpSocket, select};
use tokio_util::codec::FramedRead;
use tokio_util::udp::UdpFramed;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// Multicast group to join
    #[arg(long, short, default_value = "224.1.1.1")]
    group: IpAddr,
    /// Local IPv4 interface to bind for multicast, use IP of the adapter connected to LAN.
    /// Specific fix for Windows, otherwise leave as default.
    #[arg(long, short = 'i', default_value = "0.0.0.0")]
    interface: Ipv4Addr,
    /// Communication port (must be the same in clients)
    #[arg(long, short, default_value_t = 5007)]
    port: u16,
    /// Communication port for TCP stream
    #[arg(long, short, default_value_t = 5008)]
    tcp_port: u16,
    /// How many ports for UDP streams
    #[arg(long, default_value_t = 10)]
    udp_ports: u8,
    /// Interval between updates from clients
    #[arg(long, short, value_parser = humantime::parse_duration, default_value = "5s")]
    update_interval: Duration,
    /// Log additional data
    #[arg(long, short)]
    debug: bool,
    /// Output directory
    #[arg(long, short, default_value = "output")]
    output: PathBuf,
    /// Enable Terminal User Interface (TUI) alongside the GUI
    #[arg(long)]
    pub tui: bool,
}

#[derive(Debug)]
enum ClientStatus {
    Connected {
        sender: tokio::sync::mpsc::Sender<InfraThreadMessage>,
        port: u16,
        directory: PathBuf,
    },
    Waiting,
}

#[derive(Debug)]
struct ClientInfo {
    host: String,
    port: u16,
    format: CameraFormat,
    status: ClientStatus,
}

fn print_clients(clients: &IndexMap<IpAddr, ClientInfo>) {
    let length = clients
        .values()
        .map(|x| x.host.len())
        .max()
        .unwrap_or_default()
        .max("host".len());
    let separator = format!("+-----------------+-{}-+", "-".repeat(length));

    println!();
    println!("{separator}");
    println!("| address         | {:length$} |", "host");
    println!("{separator}");
    for (address, info) in clients {
        println!(
            "| {} | {} |",
            format!("{:<15}", address).bright_green().bold(),
            format!("{:<length$}", info.host).bright_blue().bold()
        )
    }
    println!("{separator}");
    println!();
}

struct ClientData {
    format: CameraFormat,
    address: SocketAddr,
    udp_port: u16,
    name: String,
    directory: PathBuf,
}

enum InfraThreadMessage {
    Close,
}

struct FFmpegThread {
    command: InterruptibleChild,
    stdout: JoinHandle<flume::Sender<ServerEvent>>,
    stderr: JoinHandle<()>,
}

impl FFmpegThread {
    fn new(address: SocketAddr, directory: &Path, output: flume::Sender<ServerEvent>) -> Self {
        let mut command = build_ffmpeg_command(address.port(), directory);

        let stdout = {
            let output = output.clone();
            let stdout = command.stdout.take().unwrap();
            tokio::spawn(async move {
                let mut s = Box::pin(stdout_stream(stdout));

                while let Some(x) = s.next().await {
                    match x {
                        Ok((width, height, data)) => {
                            let _ = output.send(ServerEvent::Client(
                                address.ip(),
                                ServerClientEvent::Frame {
                                    width,
                                    height,
                                    data,
                                },
                            ));
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                println!("{e:#?}");
                            }
                        }
                    }
                }

                output
            })
        };
        let stderr = {
            let stderr = command.stderr.take().unwrap();
            tokio::spawn(async move {
                let mut s = Box::pin(stderr_stream(stderr));

                while let Some(x) = s.next().await {
                    match x {
                        Ok(StderrMessage::Log(line)) => {
                            let _ = output.send(ServerEvent::Client(
                                address.ip(),
                                ServerClientEvent::Log(line),
                            ));
                        }
                        Ok(StderrMessage::Progress(progress)) => {
                            let _ = output.send(ServerEvent::Client(
                                address.ip(),
                                ServerClientEvent::Progress(progress),
                            ));
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                println!("{e:#?}");
                            }
                        }
                    }
                }
            })
        };

        Self {
            command,
            stdout,
            stderr,
        }
    }

    async fn close(self) -> flume::Sender<ServerEvent> {
        command_terminate_then_kill(self.command).await;
        self.stderr.await.unwrap();
        self.stdout.await.unwrap()
    }
}

fn build_ffmpeg_command(port: u16, directory: &Path) -> InterruptibleChild {
    let ts_file = directory.join("output.ts");

    Command::new("ffmpeg")
        .args(["-hide_banner", "-v", "level+info"])
        .args(["-nostats", "-stats_period", "1", "-progress", "pipe:2"])
        .args([
            // "-probesize",
            // "100000",
            "-analyzeduration",
            "1000000",
            "-fflags",
            "nobuffer",
            "-flags",
            "low_delay",
            "-re",
            "-i",
        ])
        .arg(format!("udp://0.0.0.0:{}", port))
        .args(["-q:v", "2"])
        .arg(ts_file)
        .args(["-vf", "scale=-1:270,transpose=1,transpose=1"])
        .args([
            "-r",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "bmp",
            "-pix_fmt",
            "bgra",
            "pipe:1",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn_interruptible()
        .unwrap()
}

async fn command_terminate_then_kill(mut command: InterruptibleChild) -> std::io::Result<()> {
    command.terminate()?;

    // TODO: temporary fix for block
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        match command.try_wait() {
            Err(_) | Ok(Some(_)) => return Ok(()),
            _ => {}
        }
    }

    command.kill().await
}

async fn connection_thread(
    client: ClientData,
    settings: Args,
    mut input: tokio::sync::mpsc::Receiver<InfraThreadMessage>,
    stream: TcpStream,
    mut socket: impl CustomSender<ServerMessage>,
    output: flume::Sender<ServerEvent>,
) -> std::io::Result<()> {
    let mut reader = FramedRead::new(stream, ClientMessage::decoder());

    std::fs::create_dir(&client.directory)?;

    let mut log = File::create(client.directory.join("output.log"))?;

    let mut interval = tokio::time::interval(settings.update_interval * 10);
    interval.tick().await;
    let mut interval_2 = tokio::time::interval(settings.update_interval * 5);
    interval_2.tick().await;

    let ffmpeg = FFmpegThread::new(
        (client.address.ip(), client.udp_port).into(),
        &client.directory,
        output.clone(),
    );

    loop {
        select! {
            Some(message) = input.recv() => {
                match message {
                    InfraThreadMessage::Close => {
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                // println!("Client not sending anything, closing...");
                // break;
            }
            _ = interval_2.tick() => {
                let _ = socket.send(ServerMessage::Heartbeat).await;
            }
            Some(Ok(amount)) = reader.next() => {
                match amount {
                    ClientMessage::Log(message) => {
                        output.send(ServerEvent::Client(client.address.ip(), ServerClientEvent::Log(message)));
                    }
                    ClientMessage::Progress(message) => {
                        output.send(ServerEvent::Client(client.address.ip(), ServerClientEvent::Progress(message)));
                    }
                    ClientMessage::Close => break,
                }
            }
        }
    }

    ffmpeg.close().await;
    Ok(())
}

async fn loop_iteration(
    settings: &Args,
    clients: &mut IndexMap<IpAddr, ClientInfo>,
    recently_denied: &mut std::collections::HashMap<IpAddr, u16>,
    join_set: &mut JoinSet<IpAddr>,
    data_socket: &Arc<UdpSocket>,
    listener: &mut TcpListener,
    output: &mut flume::Sender<ServerEvent>,
    gui_rx: &mut UnboundedReceiver<InterfaceMessage>,
) -> Option<bool> {
    let mut data_socket_2 = UdpFramed::new(
        data_socket.clone(),
        CustomCodec::<ServerMessage, ClientInitialMessage>::new(),
    );

    select! {
        Ok(cmd) = gui_rx.recv() => {
            match cmd {
                InterfaceMessage::PrintClients => print_clients(clients),
                InterfaceMessage::DisconnectClient(address) => {
                    if let Some(client) = clients.get_mut(&address)
                        && let ClientStatus::Connected { sender, .. } = &client.status {
                            println!("Closing connection to `{}`", client.host);
                            let msg = ServerMessage::Close { force: false };
                            let _ = data_socket_2.send((msg, (address, client.port).into())).await;
                            let _ = sender.send(InfraThreadMessage::Close).await;

                            client.status = ClientStatus::Waiting;

                            let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Disconnected));
                        }
                }
                InterfaceMessage::AllowClient(address) => {
                    if let Some(port) = recently_denied.remove(&address) {
                        let _ = data_socket_2.send((ServerMessage::Allow, (address, port).into())).await;
                    }
                }
                InterfaceMessage::DenyClient(address) => {
                    if let Some (client) = clients.shift_remove(&address){
                        let msg = ServerMessage::Close { force: true };
                        let _ = data_socket_2.send((msg, (address, client.port).into())).await;
                        recently_denied.insert(address, client.port);
                        let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Denied));
                    }
                }
                InterfaceMessage::AcceptClient(address) => {
                    let Some(udp_port) = ((settings.port + 1)..(settings.port + 1 + settings.udp_ports as u16))
                        .find(|port| !clients.iter().any(|(_, x)| matches!(x.status, ClientStatus::Connected { port: x, .. } if x == *port)))
                     else {
                            eprintln!("No UDP port left for {address}");
                            return None;
                        };

                    if let Some(client) = clients.get_mut(&address) {

                        if let Err(e) = data_socket_2.send((
                            ServerMessage::Open { tcp_port: settings.tcp_port , udp_port, interval: settings.update_interval },
                            (address, client.port).into(),
                        )).await {
                            eprintln!("Cannot send Open to {address}: {e}");
                            return None;
                        }

                        let (infra_thread_tx, infra_thread_rx) = tokio::sync::mpsc::channel(10);
                        let directory = settings.output.join(format!(
                            "{:?} - {}",
                            Timestamp::now().as_second(),
                            client.host,
                        ));

                        {
                            //let mut denied = false;
                            let settings = settings.clone();

                            let stream = match tokio::time::timeout(Duration::from_secs(5), listener.accept()).await {
                                Ok(Ok((stream, _))) => stream,
                                Ok(Err(e)) => {
                                    eprintln!("Accept failed: {e}");
                                    return None;
                                }
                                Err(_) => {
                                    eprintln!("Client {address} did not connect, removing it");
                                    clients.shift_remove(&address);
                                    let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Denied));
                                    return None;
                                }
                            };

                            let socket = data_socket.clone();
                            let output = output.clone();
                            let address = SocketAddr::from((address, client.port));
                            let client_data = ClientData {
                                format: client.format,
                                address,
                                udp_port,
                                name: client.host.clone(),
                                directory: directory.clone(),
                            };

                            let socket = MapSender::new(
                                UdpFramed::new(socket, ServerMessage::encoder()),
                                move |x| (x, address),
                            );

                            join_set.spawn(async move {
                                if let Err(e) = connection_thread(client_data, settings, infra_thread_rx, stream, socket, output).await {
                                    eprintln!("{e}");
                                };
                                address.ip()
                            });

                        }

                        let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Accepted));

                        client.status = ClientStatus::Connected {
                            sender: infra_thread_tx,
                            port: udp_port,
                            directory,
                        };
                    }
                }
                InterfaceMessage::OpenFolder(address) => {
                    if let Some(ClientStatus::Connected { directory, ..}) = clients.get(&address).map(|x| &x.status) {
                        if let Err(e) = open::that_detached(directory) {
                            eprintln!("Cannot open folder: {e}");
                        }
                    }
                }
                InterfaceMessage::Shutdown { force } => {
                    return Some(force);
                }
            }
        }
        Some(Ok((ClientInitialMessage { port, format, host }, address))) = StreamExt::next(&mut data_socket_2) => {
            let address = address.ip();

            if !clients.contains_key(&address) {
                if let Some(denied_port) = recently_denied.get_mut(&address) {
                    *denied_port = port;
                    let msg = ServerMessage::Close { force: true };
                    let _ = data_socket_2.send((msg, (address, port).into())).await;
                    return None;
                }
                let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Connected { host: host.to_owned() }));

                println!(
                    "Added address {} as host {}",
                    address.to_string().bright_green().bold(),
                    host.bright_blue().bold()
                );

                let info = ClientInfo {
                    host,
                    port,
                    format,
                    status: ClientStatus::Waiting,
                };
                clients.insert(address, info);
            }
        }
        Some(x) = join_set.join_next() => {
            if let Ok(address) = x
                && let Some(_) = clients.shift_remove(&address) {
                    println!(
                        "Removed address {} from clients", address.to_string().bright_green().bold(),
                    );
                    let _ = output.send(ServerEvent::Client(address, ServerClientEvent::Denied));
                }
        }
        else => return None,
    }

    None
}

pub async fn server_loop(
    args: Args,
    output: &mut flume::Sender<ServerEvent>,
    mut gui_rx: UnboundedReceiver<InterfaceMessage>,
) -> std::io::Result<()> {
    let stdin = std::io::stdin();
    let old_term = if args.tui {
        Some(TermMode::new(stdin)?)
    } else {
        None
    };

    let socket =
        Arc::new(UdpSocket::bind((library::unspecified_from(args.group), args.port)).await?);
    match args.group {
        IpAddr::V4(address) => socket.join_multicast_v4(address, args.interface),
        IpAddr::V6(address) => socket.join_multicast_v6(&address, 0),
    }?;

    let mut clients = IndexMap::new();
    let mut recently_denied = std::collections::HashMap::new();
    let mut join_set = JoinSet::new();
    let mut listener =
        TcpListener::bind((library::unspecified_from(args.group), args.tcp_port)).await?;

    let mut socket_2 = UdpFramed::new(socket.clone(), ServerMessage::encoder());

    loop {
        let force = loop_iteration(
            &args,
            &mut clients,
            &mut recently_denied,
            &mut join_set,
            &socket,
            &mut listener,
            output,
            &mut gui_rx,
        )
        .await;

        if let Some(force) = force {
            for (address, info) in clients {
                if let ClientStatus::Connected { sender, .. } = info.status {
                    println!("Closing connection to `{}`", info.host);
                    socket_2
                        .send((ServerMessage::Close { force }, (address, info.port).into()))
                        .await?;
                    let _ = sender.send(InfraThreadMessage::Close).await;
                }
            }

            join_set.join_all().await;
            break;
        }
    }

    if let Some(old_term) = old_term {
        drop(old_term);
    }

    Ok(())
}

fn map_key(event: KeyEvent) -> Option<InterfaceMessage> {
    match event {
        KeyEvent {
            code: KeyCode::Char('h'),
            modifiers: KeyModifiers::NONE,
            ..
        } => Some(InterfaceMessage::PrintClients),
        KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            ..
        } => Some(InterfaceMessage::Shutdown { force: false }),
        KeyEvent {
            code: KeyCode::Char('Q'),
            modifiers: KeyModifiers::SHIFT,
            ..
        } => Some(InterfaceMessage::Shutdown { force: true }),
        x if is_closing_key(x) => Some(InterfaceMessage::Shutdown { force: false }),
        _ => None,
    }
}

fn main() -> iced::Result {
    let args = Args::parse();
    println!("{args:?}");

    std::fs::create_dir_all(&args.output).unwrap();

    let (gui_tx, gui_rx) = futures::channel::mpsc::unbounded();
    let (mut server_tx, server_rx) = flume::unbounded();

    let runtime = Runtime::new().unwrap();

    if args.tui {
        let gui_tx = gui_tx.clone();
        runtime.spawn(
            EventStream::new()
                .filter_map(|x| async move {
                    match x {
                        Ok(crossterm::event::Event::Key(x)) => map_key(x),
                        _ => None,
                    }
                })
                .map(Ok)
                .forward(gui_tx),
        );
    }

    runtime.spawn(async move {
        let s_result = server_loop(args, &mut server_tx, gui_rx).await;
        if let Err(e) = s_result {
            eprintln!("Server error: {}", e);
        }
        let ms_err = server_tx.send(ServerEvent::Stopped);
        if let Err(ms_err) = ms_err {
            eprintln!("Close Server Event message send errro: {}", ms_err);
        }
    });

    let _guard = runtime.enter();

    gui::run(gui_tx, server_rx)?;

    Ok(())
}

#[derive(Debug, Clone)]
pub enum ServerClientEvent {
    Connected {
        host: String,
    },
    Accepted,
    Disconnected,
    Denied,
    Frame {
        width: u32,
        height: u32,
        data: Vec<u8>,
    },
    Progress(FFmpegProgress),
    Log(LogLine),
}

#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// The server was stopped
    Stopped,
    /// A message specific to a client
    Client(IpAddr, ServerClientEvent),
}

#[derive(Debug, Clone)]
pub enum InterfaceMessage {
    PrintClients,
    AcceptClient(IpAddr),
    DisconnectClient(IpAddr),
    AllowClient(IpAddr),
    DenyClient(IpAddr),
    OpenFolder(IpAddr),
    Shutdown { force: bool },
}
