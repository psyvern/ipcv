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
use ipcv::CustomSender;
use ipcv::MapSender;
use ipcv::TermMode;
use ipcv::is_closing_key;
use ipcv::messages::ClientInitialMessage;
use ipcv::messages::ClientMessage;
use ipcv::messages::CustomCodec;
use ipcv::messages::Decode;
use ipcv::messages::Encode;
use ipcv::messages::ServerMessage;
use jiff::Timestamp;
use nokhwa::utils::CameraFormat;
use send_ctrlc::{Interruptible, InterruptibleChild, InterruptibleCommand};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{BufRead, BufReader, Read};
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{ChildStderr, ChildStdout, Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{fs::File, net::IpAddr, path::PathBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio::{net::UdpSocket, select};
use tokio_util::codec::FramedRead;
use tokio_util::udp::UdpFramed;

#[derive(Clone, Debug)]
struct FFmpegProgress {
    frame: usize,
    fps: f64,
    q: f64,
    bitrate: String,
    total_size: usize,
    time: Duration,
    dup_frames: usize,
    drop_frames: usize,
    speed: f64,
}

impl FFmpegProgress {
    fn from_values(mut values: HashMap<String, String>) -> Option<Self> {
        Some(Self {
            frame: values.remove("frame")?.parse().ok()?,
            fps: values.remove("fps")?.parse().ok()?,
            q: values.remove("stream_0_0_q")?.parse().ok()?,
            bitrate: values.remove("bitrate")?.parse().ok()?,
            total_size: values.remove("total_size")?.parse().ok()?,
            time: Duration::from_micros(values.remove("out_time_us")?.parse().ok()?),
            dup_frames: values.remove("dup_frames")?.parse().ok()?,
            drop_frames: values.remove("drop_frames")?.parse().ok()?,
            speed: values.remove("speed")?.trim_end_matches('x').parse().ok()?,
        })
    }
}

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
    Connected(tokio::sync::mpsc::Sender<InfraThreadMessage>, u16),
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
}

#[derive(Debug, Clone, Copy)]
enum LogLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
struct LogLine {
    level: LogLevel,
    parts: Vec<String>,
    line: String,
}

impl Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[\x1b[{};1m{}\x1b[0m] {}{}",
            match self.level {
                LogLevel::Info => "92",
                LogLevel::Warning => "93",
                LogLevel::Error => "91",
            },
            match self.level {
                LogLevel::Info => " info",
                LogLevel::Warning => " warn",
                LogLevel::Error => "error",
            },
            self.parts
                .iter()
                .map(|x| format!("[{x}] "))
                .collect::<String>(),
            self.line
        )
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "info" => Self::Info,
            "warning" => Self::Warning,
            "error" => Self::Error,
            _ => return Err(s.to_owned()),
        })
    }
}

fn stderr_thread(
    address: IpAddr,
    stderr: ChildStderr,
    output: &flume::Sender<ServerEvent>,
) -> std::io::Result<()> {
    // let mut stderr = tokio::io::BufReader::new(stderr);
    let mut stderr = BufReader::new(stderr);
    let mut line = String::new();
    let mut map = HashMap::new();

    let mut last_line = None::<LogLine>;

    loop {
        line.clear();

        if stderr.read_line(&mut line)? == 0 {
            break;
        }

        let line = line.trim_end_matches('\n');

        if let Some((key, value)) = line.split_once('=')
            && key
                .chars()
                .all(|x| matches!(x, '0'..='9' | '_' | 'a'..='z'))
        {
            map.insert(key.to_owned(), value.trim().to_owned());

            if key == "progress" {
                let progress = FFmpegProgress::from_values(map);
                map = HashMap::new();

                if let Some(progress) = progress {
                    output.send(ServerEvent::ProgressReceived { address, progress });
                }
            }
        } else {
            let mut parts = Vec::new();
            let mut line = line;
            while let Some(new) = line.strip_prefix('[') {
                if let Some((part, new)) = new.split_once("] ") {
                    parts.push(part);
                    line = new;
                } else {
                    break;
                }
            }
            let level: Option<LogLevel> = parts.pop().and_then(|x| x.parse().ok());
            match level {
                Some(level) => {
                    if let Some(line) = last_line {
                        output.send(ServerEvent::LogReceived { address, log: line });
                    }

                    last_line = Some(LogLine {
                        level,
                        parts: parts.into_iter().map(str::to_owned).collect(),
                        line: line.to_owned(),
                    });
                }
                None => {
                    if let Some(last_line) = &mut last_line {
                        last_line.line.push('\n');
                        last_line.line.push_str(line);
                    }
                }
            }
        }
    }

    if let Some(line) = last_line {
        output.send(ServerEvent::LogReceived { address, log: line });
    }

    Ok(())
}

fn stdout_thread(
    address: IpAddr,
    stdout: ChildStdout,
    output: &flume::Sender<ServerEvent>,
) -> std::io::Result<()> {
    let mut stdout = BufReader::new(stdout);

    let mut header = [0u8; 6];
    loop {
        stdout.read_exact(&mut header)?;

        if !(header[0] == b'B' && header[1] == b'M') {
            eprintln!("unexpected header: {header:?}");
            break;
        }

        let size = u32::from_le_bytes([header[2], header[3], header[4], header[5]]) as usize;

        let mut buf = vec![0; size];
        stdout.read_exact(&mut buf[6..])?;

        let offset = u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]) as usize;

        let width = u32::from_le_bytes([buf[18], buf[19], buf[20], buf[21]]);
        let height = u32::from_le_bytes([buf[22], buf[23], buf[24], buf[25]]);
        let data = buf.split_off(offset);
        let data = ipcv::bgra_to_rgba(data);

        output
            .send(ServerEvent::FrameReceived {
                address,
                width,
                height,
                data,
            })
            .unwrap();
    }

    Ok(())
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
        let mut command = build_ffmpeg_command(address, directory);

        let stdout = {
            let output = output.clone();
            let stdout = command.stdout.take().unwrap();
            std::thread::spawn(move || {
                let result = stdout_thread(address.ip(), stdout, &output);
                if let Err(e) = result
                    && e.kind() != std::io::ErrorKind::UnexpectedEof
                {
                    println!("{e:#?}");
                }

                output
            })
        };
        let stderr = {
            let stderr = command.stderr.take().unwrap();
            std::thread::spawn(move || {
                let result = stderr_thread(address.ip(), stderr, &output);
                if let Err(e) = result
                    && e.kind() != std::io::ErrorKind::UnexpectedEof
                {
                    println!("{e:#?}");
                }
            })
        };

        Self {
            command,
            stdout,
            stderr,
        }
    }

    fn close(self) -> flume::Sender<ServerEvent> {
        command_terminate_then_kill(self.command);
        self.stderr.join().unwrap();
        self.stdout.join().unwrap()
    }
}

fn build_ffmpeg_command(address: SocketAddr, directory: &Path) -> send_ctrlc::InterruptibleChild {
    let ts_file = directory.join("output.ts");

    Command::new("ffmpeg")
        .args(["-hide_banner", "-v", "level+warning"])
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
        .arg(format!("udp://{address}"))
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
        // .env("AV_LOG_FORCE_COLOR", "1")
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn_interruptible()
        .unwrap()
}

fn command_terminate_then_kill(mut command: InterruptibleChild) {
    command.terminate();

    // TODO: temporary fix for block
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        match command.try_wait() {
            Err(_) | Ok(Some(_)) => return,
            _ => {}
        }
    }

    command.kill();
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

    let directory = settings.output.join(format!(
        "{:?} - {}",
        Timestamp::now().as_second(),
        client.name
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir(&directory)?;

    let mut log = File::create(directory.join("output.log"))?;

    let mut interval = tokio::time::interval(settings.update_interval * 10);
    interval.tick().await;
    let mut interval_2 = tokio::time::interval(settings.update_interval * 5);
    interval_2.tick().await;

    let ffmpeg = FFmpegThread::new(
        (client.address.ip(), client.udp_port).into(),
        &directory,
        output,
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
                    ClientMessage::Close => break,
                }
            }
        }
    }

    ffmpeg.close();
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
                        && let ClientStatus::Connected(sender, _) = &client.status {
                            println!("Closing connection to `{}`", client.host);
                            let msg = ServerMessage::Close { force: false };
                            let _ = data_socket_2.send((msg, (address, client.port).into())).await;
                            let _ = sender.send(InfraThreadMessage::Close).await;

                            client.status = ClientStatus::Waiting;

                            let _ = output.send(ServerEvent::ClientDisconnected { address });
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
                        let _ = output.send(ServerEvent::ClientDenied { address });
                    }
                }
                InterfaceMessage::AcceptClient(address) => {
                    let Some(udp_port) = ((settings.port + 1)..(settings.port + 1 + settings.udp_ports as u16))
                        .find(|port| !clients.iter().any(|(_, x)| matches!(x.status, ClientStatus::Connected(_, x) if x == *port)))
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
                                    let _ = output.send(ServerEvent::ClientDenied { address });
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

                        let _ = output.send(ServerEvent::ClientAccepted { address });

                        client.status = ClientStatus::Connected(infra_thread_tx, udp_port);
                    }
                }
                InterfaceMessage::OpenFolder(address) => {
                    let path = settings.output.join(address.to_string());
                    if let Err(e) = open::that_detached(path) {
                        eprintln!("Cannot open folder: {e}");
                    }
                }
                InterfaceMessage::Move(_, _) => {}
                InterfaceMessage::Rotate(_) => {}
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
                let _ = output.send(ServerEvent::ClientConnected {
                    address,
                    host: host.to_owned(),
                });

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
                    let _ = output.send(ServerEvent::ClientDenied { address });
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

    let socket = Arc::new(UdpSocket::bind((ipcv::unspecified_from(args.group), args.port)).await?);
    match args.group {
        IpAddr::V4(address) => socket.join_multicast_v4(address, args.interface),
        IpAddr::V6(address) => socket.join_multicast_v6(&address, 0),
    }?;

    let mut clients = IndexMap::new();
    let mut recently_denied = std::collections::HashMap::new();
    let mut join_set = JoinSet::new();
    let mut listener =
        TcpListener::bind((ipcv::unspecified_from(args.group), args.tcp_port)).await?;

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
                if let ClientStatus::Connected(sender, _) = info.status {
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
pub enum ServerEvent {
    Stopped,
    ClientConnected {
        address: IpAddr,
        host: String,
    },
    ClientAccepted {
        address: IpAddr,
    },
    ClientDisconnected {
        address: IpAddr,
    },
    ClientDenied {
        address: IpAddr,
    },
    FrameReceived {
        address: IpAddr,
        width: u32,
        height: u32,
        data: Vec<u8>,
    },
    ProgressReceived {
        address: IpAddr,
        progress: FFmpegProgress,
    },
    LogReceived {
        address: IpAddr,
        log: LogLine,
    },
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
    Move(usize, usize),
    Rotate(IpAddr),
}
