use clap::Parser;
use colored::Colorize;
use crossterm::event::{EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::SinkExt;
use futures::{FutureExt, StreamExt};
use image::{ImageBuffer, Rgb};
use ipcv::ClientMessage;
use ipcv::ClientMessageParser;
use ipcv::ServerMessage;
use ipcv::TermMode;
use ipcv::is_closing_key;
use itertools::Itertools;
use nokhwa::FormatDecoder;
use nokhwa::pixel_format::RgbFormat;
use nokhwa::utils::{FrameFormat, Resolution};
use std::collections::HashMap;
use std::io::Write;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs::File, net::IpAddr, path::PathBuf};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::{net::UdpSocket, select};
use tokio_util::sync::CancellationToken;

#[path = "../gui.rs"]
pub mod gui;
use ipcv::{GuiCommand, ServerEvent};

pub static ARGS: std::sync::OnceLock<Args> = std::sync::OnceLock::new();

const MCAST_GRP: &str = "224.1.1.1";

#[derive(Debug, Clone, Parser)]
pub struct Args {
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
    /// Network interface IP to use
    #[arg(long, default_value = "0.0.0.0")]
    ip: Ipv4Addr,
}

#[derive(Debug)]
struct ClientInfo {
    host: String,
    port: u16,
    width: u32,
    height: u32,
    token: CancellationToken,
}

fn print_clients(clients: &HashMap<IpAddr, ClientInfo>) {
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
    width: u32,
    height: u32,
    format: FrameFormat,
    address: IpAddr,
    settings: Args,
    token: CancellationToken,
    mut stream: TcpStream,
    socket: Arc<UdpSocket>,
    mut output: futures::channel::mpsc::Sender<gui::Message>,
) {
    let mut counter = 0;

    let mut reader = ClientMessageParser::default();

    let directory = settings.output.join(address.to_string());
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();

    let mut log = File::create(directory.join("output.log")).unwrap();

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
                socket.send_to(&ServerMessage::Heartbeat.to_bytes(), (address, settings.port)).await.unwrap();
            }
            Ok(amount) = stream.read(&mut packet) => {
                interval.reset();
                // println!("{amount}");

                match reader.read(&packet[..amount]) {
                    None => {},
                    Some(ClientMessage::Close) => return,
                    Some(ClientMessage::Frame(mut bytes)) => {
                        let bytes = bytes.make_contiguous();
                        let bytes = RgbFormat::write_output(format, Resolution::new(width, height), bytes).unwrap();

                        let frame =
                            ImageBuffer::<Rgb<u8>, _>::from_vec(width, height, bytes).unwrap();

                        let frame_arc = Arc::new(frame);
                        let _ = output.send(gui::Message::ServerEvent(ServerEvent::FrameReceived {
                            address,
                            frame_number: counter as u64,
                            frame: frame_arc,
                        })).await;

                        println!("Received frame {} from {}", counter.to_string().bright_cyan().bold(), address.to_string().bright_green().bold());
                        counter += 1;

                        writeln!(log, "[{:?}] ciaooo {counter:05}", Instant::now()).unwrap();
                    }
                }
            }
        }
    }
}

async fn loop_iteration(
    settings: &Args,
    clients: &mut HashMap<IpAddr, ClientInfo>,
    join_set: &mut JoinSet<IpAddr>,
    data_socket: &Arc<UdpSocket>,
    listener: &mut TcpListener,
    output: &mut futures::channel::mpsc::Sender<gui::Message>,
    key_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crossterm::event::KeyEvent>,
) -> Option<bool> {
    let mut data = [0u8; 4096];

    select! {
        Some(x) = key_rx.recv() => {
            match x {
                KeyEvent { code: KeyCode::Char('h'), modifiers: KeyModifiers::NONE, .. } => {
                    print_clients(clients);
                },
                KeyEvent { code: KeyCode::Char('q'), modifiers: KeyModifiers::NONE, .. } => {
                    return Some(false);
                },
                KeyEvent { code: KeyCode::Char('Q'), modifiers: KeyModifiers::SHIFT, .. } => {
                    return Some(true);
                },
                x if is_closing_key(x) => {
                    return Some(false);
                }
                _ => {}
            }
        }
        Ok((size, address)) = data_socket.recv_from(&mut data) => {
            let address = address.ip();

            let data = &data[..size];

            if let Some(data) = data.strip_prefix(b"open ") {
                let data = String::from_utf8(data.to_vec()).unwrap();
                let (width, height, format, host) = data.splitn(4, " ").next_tuple().unwrap();

                if let Some(_info) = clients.get(&address) {
                    data_socket.send_to(
                        &ServerMessage::Start { port: settings.tcp_port, interval: settings.update_interval }.to_bytes(),
                        (address, settings.port),
                    ).await.unwrap();

                    println!("Reconnected to `{}`", address);
                } else {
                    let width = width.parse().unwrap();
                    let height = height.parse().unwrap();
                    let format = format.parse().unwrap();

                    data_socket.send_to(
                        &ServerMessage::Start { port: settings.tcp_port, interval: settings.update_interval }.to_bytes(),
                        (address, settings.port),
                    ).await.unwrap();

                    let token = CancellationToken::new();

                    {
                        let settings = settings.clone();
                        let token = token.clone();
                        let (stream, _) = listener.accept().await.unwrap();
                        let socket = data_socket.clone();
                        let output_clone = output.clone();

                        join_set.spawn(async move {
                            connection_thread(width, height, format, address, settings, token, stream, socket, output_clone).await;
                            address
                        });
                    }

                    let _ = output.send(gui::Message::ServerEvent(ServerEvent::ClientConnected {
                        address,
                        host: host.to_owned(),
                    })).await;

                    let info = ClientInfo {
                        host: host.to_owned(),
                        port: settings.port,
                        width,
                        height,
                        token,
                    };
                    clients.insert(address, info);

                    println!(
                        "Added address {} as host {}",
                        address.to_string().bright_green().bold(),
                        host.bright_blue().bold()
                    );
                }
            } else {
                if let Some(info) = clients.get(&address) {
                    println!("data from `{}`: {data:?}", info.host);
                } else {
                    println!("data from unknown host: {data:?}");
                }
            }
        }
        Some(x) = join_set.join_next() => {
            if let Ok(address) = x
                && let Some(_) = clients.remove(&address) {
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
    mut output: futures::channel::mpsc::Sender<gui::Message>,
) -> std::io::Result<()> {
    let old_term = if args.tui {
        Some(TermMode::new()?)
    } else {
        None
    };

    let socket = Arc::new(UdpSocket::bind((args.ip, args.port)).await?);
    socket.join_multicast_v4(MCAST_GRP.parse().unwrap(), args.ip)?;

    let mut clients = HashMap::new();
    let mut join_set = JoinSet::new();
    let mut listener = TcpListener::bind((args.ip, args.tcp_port)).await?;

    let _ = output
        .send(gui::Message::ServerEvent(ServerEvent::Started))
        .await;

    let (key_tx, mut key_rx) = tokio::sync::mpsc::unbounded_channel();
    let _keep_alive = key_tx.clone(); // Prevents key_rx from closing if tui is false

    if args.tui {
        tokio::spawn(async move {
            let mut events = EventStream::new();
            while let Some(Ok(event)) = events.next().await {
                if let crossterm::event::Event::Key(k) = event {
                    if key_tx.send(k).is_err() {
                        break;
                    }
                }
            }
        });
    }

    loop {
        let force = loop_iteration(
            &args,
            &mut clients,
            &mut join_set,
            &socket,
            &mut listener,
            &mut output,
            &mut key_rx,
        )
        .await;

        if let Some(force) = force {
            let msg = ServerMessage::Quit { force }.to_bytes();
            for (address, info) in clients {
                println!("Closing connection to `{}`", info.host);
                socket.send_to(&msg, (address, info.port)).await?;
                info.token.cancel();
            }

            join_set.join_all().await;
            break;
        }
    }

    if let Some(term) = old_term {
        drop(term);
    }
    
    let _ = output.send(gui::Message::ServerEvent(ServerEvent::Stopped)).await;
    Ok(())
}

fn main() -> iced::Result {
    let args = Args::parse();
    println!("{args:?}");
    ARGS.set(args).unwrap();

    gui::run()
}
