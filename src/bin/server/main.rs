mod gui;

use clap::Parser;
use colored::Colorize;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use image::{ImageBuffer, Rgb};
use indexmap::IndexMap;
use ipcv::ClientInitialMessage;
use ipcv::ClientMessage;
use ipcv::ClientMessageParser;
use ipcv::SenderExt;
use ipcv::ServerMessage;
use ipcv::TermMode;
use ipcv::is_closing_key;
use nokhwa::FormatDecoder;
use nokhwa::pixel_format::RgbFormat;
use nokhwa::utils::CameraFormat;
use std::io::Write;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use std::{fs::File, net::IpAddr, path::PathBuf};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinSet;
use tokio::{net::UdpSocket, select};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// Multicast group to join
    #[arg(long, short, default_value = "224.1.1.1")]
    group: IpAddr,
    /// Communication port (must be the same in clients)
    #[arg(long, short, default_value_t = 5007)]
    port: u16,
    /// Communication port for TCP stream
    #[arg(long, short, default_value_t = 5008)]
    tcp_port: u16,
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
    Connected(CancellationToken),
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

async fn connection_thread(
    format: CameraFormat,
    address: IpAddr,
    port: u16,
    settings: Args,
    token: CancellationToken,
    mut stream: TcpStream,
    socket: Arc<UdpSocket>,
    mut output: impl SenderExt<ServerEvent>,
) -> std::io::Result<()> {
    let mut counter = 0u64;

    let mut reader = ClientMessageParser::default();

    let directory = settings.output.join(address.to_string());
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir(&directory)?;

    let mut log = File::create(directory.join("output.log"))?;

    let mut packet = [0; 4096];

    let mut interval = tokio::time::interval(settings.update_interval * 10);
    interval.tick().await;
    let mut interval_2 = tokio::time::interval(settings.update_interval * 5);
    interval_2.tick().await;

    loop {
        select! {
            biased;
            _ = token.cancelled() => {
                break;
            }
            _ = interval.tick() => {
                println!("Client not sending anything, closing...");
                break;
            }
            _ = interval_2.tick() => {
                let _ = socket.send_to(&ServerMessage::Heartbeat.into_bytes(), (address, port)).await;
            }
            Ok(amount) = stream.read(&mut packet) => {
                interval.reset();
                // println!("{amount}");

                match reader.read(&packet[..amount]) {
                    None => {},
                    Some(ClientMessage::Close) => break,
                    Some(ClientMessage::Frame(bytes)) => {
                        let bytes = RgbFormat::write_output(format.format(), format.resolution(), &bytes).unwrap();
                        let frame = ImageBuffer::<Rgb<u8>, _>::from_vec(format.width(), format.height(), bytes).unwrap();

                        let (_, _, data) = ipcv::generate_preview(&frame, format.width());

                        let _ = output.send(ServerEvent::FrameReceived {
                            address,
                            frame_number: counter,
                            width: format.width(),
                            height: format.height(),
                            data,
                        }).await;

                        frame
                            .save(directory.join(format!("{counter:05}.png")))
                            .unwrap();
                        println!("Received frame {} from {}", counter.to_string().bright_cyan().bold(), address.to_string().bright_green().bold());
                        counter += 1;

                        writeln!(log, "[{:?}] ciaooo {counter:05}", Instant::now())?;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn loop_iteration(
    settings: &Args,
    clients: &mut IndexMap<IpAddr, ClientInfo>,
    join_set: &mut JoinSet<IpAddr>,
    data_socket: &Arc<UdpSocket>,
    listener: &mut TcpListener,
    output: &mut impl SenderExt<ServerEvent>,
    gui_rx: &mut UnboundedReceiver<InterfaceMessage>,
) -> Option<bool> {
    let mut data = [0u8; 4096];

    select! {
        Some(cmd) = gui_rx.recv() => {
            match cmd {
                InterfaceMessage::PrintClients => print_clients(clients),
                InterfaceMessage::DisconnectClient(address) => {
                    if let Some(info) = clients.shift_remove(&address)
                        && let ClientStatus::Connected(token) = info.status {
                            println!("Closing connection to `{}` via GUI", info.host);
                            let msg = ServerMessage::Close { force: false }.into_bytes();
                            let _ = data_socket.send_to(&msg, (address, info.port)).await;
                            token.cancel();
                        }
                }
                InterfaceMessage::AcceptClient(address) => {
                    if let Some(client) = clients.get_mut(&address) {
                        data_socket.send_to(
                            &ServerMessage::Open { port: settings.tcp_port, interval: settings.update_interval }.into_bytes(),
                            (address, client.port),
                        ).await.unwrap();

                        let token = CancellationToken::new();

                        {
                            let settings = settings.clone();
                            let token = token.clone();
                            let (stream, _) = listener.accept().await.unwrap();
                            let socket = data_socket.clone();
                            let output = output.clone();
                            let format = client.format;
                            let port = client.port;

                            join_set.spawn(async move {
                                if let Err(e) = connection_thread(format, address, port, settings, token, stream, socket, output).await {
                                    eprintln!("{e}");
                                };
                                address
                            });
                        }

                        let _ = output.send(ServerEvent::ClientAccepted { address }).await;

                        client.status = ClientStatus::Connected(token);
                    }
                }
                InterfaceMessage::OpenFolder(address) => {
                    let path = settings.output.join(address.to_string());
                    open::that_detached(path).unwrap();
                }
                InterfaceMessage::Move(_, _) => {}
                InterfaceMessage::Shutdown { force } => {
                    return Some(force);
                }
            }
        }
        Ok((size, address)) = data_socket.recv_from(&mut data) => {
            let address = address.ip();
            let data = &data[..size];

            if let Some(ClientInitialMessage { port, format, host }) = ClientInitialMessage::from_bytes(data)
                && !clients.contains_key(&address) {
                    let _ = output.send(ServerEvent::ClientConnected {
                        address,
                        host: host.to_owned(),
                    }).await;

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
                }
        }
        else => return None,
    }

    None
}

pub async fn server_loop(
    args: Args,
    output: &mut impl SenderExt<ServerEvent>,
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
        IpAddr::V4(address) => socket.join_multicast_v4(address, Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(address) => socket.join_multicast_v6(&address, 0),
    }?;

    let mut clients = IndexMap::new();
    let mut join_set = JoinSet::new();
    let mut listener =
        TcpListener::bind((ipcv::unspecified_from(args.group), args.tcp_port)).await?;

    loop {
        let force = loop_iteration(
            &args,
            &mut clients,
            &mut join_set,
            &socket,
            &mut listener,
            output,
            &mut gui_rx,
        )
        .await;

        if let Some(force) = force {
            let msg = ServerMessage::Close { force }.into_bytes();
            for (address, info) in clients {
                if let ClientStatus::Connected(token) = info.status {
                    println!("Closing connection to `{}`", info.host);
                    socket.send_to(&msg, (address, info.port)).await?;
                    token.cancel();
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

pub static ARGS: OnceLock<Args> = OnceLock::new();

fn main() -> iced::Result {
    let args = Args::parse();
    println!("{args:?}");

    ARGS.set(args.clone()).unwrap();

    std::fs::create_dir_all(&args.output).unwrap();

    gui::run()
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
    FrameReceived {
        address: IpAddr,
        frame_number: u64,
        width: u32,
        height: u32,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub enum InterfaceMessage {
    PrintClients,
    AcceptClient(IpAddr),
    DisconnectClient(IpAddr),
    OpenFolder(IpAddr),
    Shutdown { force: bool },
    Move(usize, usize),
}
