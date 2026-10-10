use clap::Parser;
use colored::Colorize;
use crossterm::event::{EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::{FutureExt, SinkExt, StreamExt};
use ipcv::{
    TermMode, is_closing_key,
    messages::{ClientInitialMessage, ClientMessage, Decode, Encode, ServerMessage},
};
use nokhwa::{
    CallbackCamera,
    utils::{CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType},
};
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{
    net::{TcpStream, UdpSocket},
    select,
};
use tokio_util::{codec::FramedWrite, udp::UdpFramed};

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Multicast group to connect to
    #[arg(long, short, default_value = "224.1.1.1")]
    group: IpAddr,
    /// Communication port (must be the same in the server)
    #[arg(long, short, default_value_t = 5007)]
    port: u16,
    /// Delay between sending (in seconds)
    #[arg(long, short, default_value_t = 5.0)]
    update_delay: f64,
    /// Log additional data
    #[arg(long, short)]
    debug: bool,
}

async fn wait_for_server(
    group: IpAddr,
    port: u16,
    camera_format: CameraFormat,
) -> std::io::Result<Option<(SocketAddr, Duration, UdpSocket)>> {
    let output = UdpSocket::bind((ipcv::unspecified_from(group), 0)).await?;
    output.set_ttl(2)?;
    output.connect((group, port)).await?;

    let input_socket = UdpSocket::bind((ipcv::unspecified_from(group), 0)).await?;
    // input_socket.set_timeout(5);

    let listening_port = input_socket.local_addr()?.port();

    let initial_message = ClientInitialMessage {
        port: listening_port,
        format: camera_format,
        host: hostname::get()?.to_string_lossy().into_owned(),
    }
    .into_bytes();

    let mut events = EventStream::new();
    let mut data = [0u8; 4096];

    loop {
        output.send(&initial_message).await?;

        let event = events.next().fuse();

        select! {
            Some(Ok(crossterm::event::Event::Key(x))) = event => {
                match x {
                    KeyEvent { code: KeyCode::Char('q'), modifiers: KeyModifiers::NONE, .. } | KeyEvent { code: KeyCode::Char('Q'), modifiers: KeyModifiers::SHIFT, .. } => {
                        break;
                    },
                    x if is_closing_key(x) => {
                        break;
                    }
                    _ => {}
                }
            },
            Ok((size, address)) = input_socket.recv_from(&mut data) => {
                let address = address.ip();

                if let Some(msg) = ServerMessage::from_bytes(&data[..size]) {
                    match msg {
                        ServerMessage::Open { port, interval } => {
                            return Ok(Some((SocketAddr::new(address, port), interval, input_socket)));
                        }
                        ServerMessage::Close { force } => {
                            if force {
                                println!("Connection denied by the server.");
                                return Ok(None);
                            }
                        }
                        _ => {}
                    }
                }
            }

            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }

    Ok(None)
}

async fn client_thread(
    camera: &mut CallbackCamera,
    address: SocketAddr,
    time: Duration,
    // Reuse the socket from wait_for_server
    input_socket: UdpSocket,
) -> std::io::Result<bool> {
    let mut input = UdpFramed::new(input_socket, ServerMessage::decoder());
    let stream = match TcpStream::connect(address).await {
        Ok(stream) => stream,
        Err(e) => {
            println!("Cannot connect to {address} ({e}), waiting again...");
            return Ok(true);
        }
    };
    let mut output = FramedWrite::new(stream, ClientMessage::encoder());
    let mut events = EventStream::new();
    let mut counter = 0;

    let mut interval = tokio::time::interval(time);
    let mut heartbeat = tokio::time::interval(time * 10);
    heartbeat.tick().await;

    Ok(loop {
        select! {
            _ = interval.tick() => {
                let frame = camera.last_frame();
                if let Ok(frame) = frame {
                    let bytes = frame.buffer_bytes();

                    if let Err(e) = output.send(ClientMessage::Frame(bytes)).await {
                        println!("Cannot send frame ({e}), detaching...");
                        break true;
                    }

                    // socket.send(counter.to_string().into_bytes());
                    println!("Sending frame: {}", counter.to_string().bright_cyan().bold());
                    counter += 1;
                }
            }
            _ = heartbeat.tick() => {
                println!("Server did not heartbeat, detaching...");
                let _ = output.send(ClientMessage::Close).await;
                break true;
            }
            Some(Ok(crossterm::event::Event::Key(x))) = events.next().fuse() => {
                match x {
                    KeyEvent { code: KeyCode::Char('q'), modifiers: KeyModifiers::NONE, .. } | KeyEvent { code: KeyCode::Char('Q'), modifiers: KeyModifiers::SHIFT, .. } => {
                        output.send(ClientMessage::Close).await?;
                        break false;
                    },
                    x if is_closing_key(x) => {
                        output.send(ClientMessage::Close).await?;
                        break false;
                    }
                    _ => {}
                }
            },
            Some(Ok((msg, _))) = input.next() => {
                match msg {
                    ServerMessage::Open { .. } => {}
                    ServerMessage::Close { force } => {
                        if force {
                            println!("Server closed, closing...");
                            break false;
                        } else {
                            println!("Server closed, detaching...");
                            break true;
                        }
                    }
                    ServerMessage::Heartbeat => heartbeat.reset(),
                    ServerMessage::UpdateInterval(x) => {
                        interval = tokio::time::interval(x);
                        heartbeat = tokio::time::interval(x * 10);
                        heartbeat.tick().await;
                    }
                }
            }
        }
    })
}

async fn run(args: Args, camera: &mut CallbackCamera) -> std::io::Result<()> {
    let camera_format = camera.camera_format().unwrap();
    println!(
        "Using camera: {}",
        camera.info().human_name().yellow().bold()
    );

    loop {
        let Some((address, delay, input_socket)) =
            wait_for_server(args.group, args.port, camera_format).await?
        else {
            break;
        };

        println!("Connected to {}", address.to_string().bright_green().bold());

        let retry = client_thread(camera, address, delay, input_socket).await?;

        if !retry {
            break;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    println!("{args:?}");

    let stdin = std::io::stdin();
    let old_term = TermMode::new(stdin)?;

    let index = CameraIndex::Index(0);
    let format = RequestedFormat::with_formats(
        RequestedFormatType::AbsoluteHighestFrameRate,
        &[
            FrameFormat::RAWRGB,
            FrameFormat::RAWBGR,
            FrameFormat::YUYV,
            FrameFormat::MJPEG,
        ],
    );
    let mut camera = CallbackCamera::new(index, format, |_| {}).unwrap();
    camera.open_stream().unwrap();
    camera.poll_frame().unwrap();

    let result = run(args, &mut camera).await;

    camera.stop_stream().unwrap();
    drop(old_term);

    result
}
