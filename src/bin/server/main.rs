mod executor;
mod gui;

use clap::Parser;
use colored::Colorize;
use crossterm::event::EventStream;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use futures::SinkExt;
use futures::StreamExt;
use futures::channel::mpsc::UnboundedReceiver;
use image::{ImageBuffer, Rgb};
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
use nokhwa::FormatDecoder;
use nokhwa::pixel_format::RgbFormat;
use nokhwa::utils::CameraFormat;
use std::io::Write;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs::File, net::IpAddr, path::PathBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio::{net::UdpSocket, select};
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
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
    address: SocketAddr,
    settings: Args,
    token: CancellationToken,
    stream: TcpStream,
    mut socket: impl CustomSender<ServerMessage>,
    output: flume::Sender<ServerEvent>,
) -> std::io::Result<()> {
    let mut counter = 0u64;

    let mut reader = FramedRead::new(stream, ClientMessage::decoder());

    let directory = settings.output.join(address.ip().to_string());
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir(&directory)?;

    let mut log = File::create(directory.join("output.log"))?;

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
                let _ = socket.send(ServerMessage::Heartbeat).await;
            }
            Some(Ok(amount)) = reader.next() => {
                interval.reset();

                match amount {
                    ClientMessage::Close => break,
                    ClientMessage::Frame(bytes) => {
                        let bytes = RgbFormat::write_output(format.format(), format.resolution(), &bytes).unwrap();
                        let frame = ImageBuffer::<Rgb<u8>, _>::from_vec(format.width(), format.height(), bytes).unwrap();

                        let (_, _, data) = ipcv::generate_preview(&frame, format.width());

                        let _ = output.send(ServerEvent::FrameReceived {
                            address: address.ip(),
                            frame_number: counter,
                            width: format.width(),
                            height: format.height(),
                            data,
                        });

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
                    if let Some(info) = clients.shift_remove(&address)
                        && let ClientStatus::Connected(token) = info.status {
                            println!("Closing connection to `{}`", info.host);
                            let msg = ServerMessage::Close { force: false };
                            let _ = data_socket_2.send((msg, (address, info.port).into())).await;
                            token.cancel();

                            let _ = output.send(ServerEvent::ClientDisconnected { address });
                        }
                }
                InterfaceMessage::AcceptClient(address) => {
                    if let Some(client) = clients.get_mut(&address) {
                        data_socket_2.send((
                            ServerMessage::Open { port: settings.tcp_port, interval: settings.update_interval },
                            (address, client.port).into(),
                        )).await.unwrap();

                        let token = CancellationToken::new();

                        {
                            let settings = settings.clone();
                            let token = token.clone();
                            let (stream, _) = listener.accept().await.unwrap();
                            let socket = data_socket.clone();
                            let output = output.clone();
                            let format = client.format;
                            let address = SocketAddr::from((address, client.port));

                            let socket = MapSender::new(
                                UdpFramed::new(socket, ServerMessage::encoder()),
                                move |x| (x, address),
                            );

                            join_set.spawn(async move {
                                if let Err(e) = connection_thread(format, address, settings, token, stream, socket, output).await {
                                    eprintln!("{e}");
                                };
                                address.ip()
                            });
                        }

                        let _ = output.send(ServerEvent::ClientAccepted { address });

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
        Some(Ok((ClientInitialMessage { port, format, host }, address))) = StreamExt::next(&mut data_socket_2) => {
            let address = address.ip();

            if !clients.contains_key(&address) {
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
    let mut join_set = JoinSet::new();
    let mut listener =
        TcpListener::bind((ipcv::unspecified_from(args.group), args.tcp_port)).await?;

    let mut socket_2 = UdpFramed::new(socket.clone(), ServerMessage::encoder());

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
            for (address, info) in clients {
                if let ClientStatus::Connected(token) = info.status {
                    println!("Closing connection to `{}`", info.host);
                    socket_2
                        .send((ServerMessage::Close { force }, (address, info.port).into()))
                        .await?;
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
        let _ = server_loop(args, &mut server_tx, gui_rx).await;
        let _ = server_tx.send(ServerEvent::Stopped);
    });

    let _guard = runtime.enter();
    gui::run(gui_tx, server_rx)
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
