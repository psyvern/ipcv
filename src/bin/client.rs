use clap::Parser;
use colored::Colorize;
use crossterm::event::{EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::{FutureExt, StreamExt};
use ipcv::{ClientMessage, ServerMessage, TermMode, is_closing_key};
use nokhwa::{
    CallbackCamera,
    utils::{CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType},
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    os::fd::AsRawFd,
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
    select,
};

const MCAST_GRP: &str = "224.1.1.1";

#[derive(Debug, Clone, Parser)]
struct Args {
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
    port: u16,
    camera_format: CameraFormat,
) -> std::io::Result<Option<(SocketAddr, Duration)>> {
    let output = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    output.set_ttl(2)?;
    output.connect((MCAST_GRP, port)).await?;

    let input_socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
    // input_socket.set_timeout(5);

    let initial_message = {
        let mut value = b"open ".to_vec();
        value.extend(camera_format.resolution().width().to_be_bytes());
        value.extend(camera_format.resolution().height().to_be_bytes());
        value.push(match camera_format.format() {
            FrameFormat::MJPEG => 0,
            FrameFormat::YUYV => 1,
            FrameFormat::NV12 => 2,
            FrameFormat::GRAY => 3,
            FrameFormat::RAWRGB => 4,
            FrameFormat::RAWBGR => 5,
        });
        value.extend(hostname::get()?.to_string_lossy().bytes());

        value
    };

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

                if let Some(ServerMessage::Start { port, interval }) = ServerMessage::from_bytes(&data[..size]) {
                    return Ok(Some((SocketAddr::new(address, port), interval)));
                }
            }

            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }

    Ok(None)
}

async fn client_thread(
    camera: &mut CallbackCamera,
    port: u16,
    address: SocketAddr,
    time: Duration,
) -> std::io::Result<bool> {
    let input = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
    let mut output = TcpStream::connect(address).await.unwrap();

    let mut events = EventStream::new();
    let mut data = [0u8; 1024];
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

                    let _ = output.write_all(&ClientMessage::Frame(bytes).into_bytes()).await;

                    // socket.send(counter.to_string().into_bytes());
                    println!("Sending frame: {}", counter.to_string().bright_cyan().bold());
                    counter += 1;
                }
            }
            _ = heartbeat.tick() => {
                println!("Server did not heartbeat, detaching...");
                let _ = output.write_all(&ClientMessage::Close.into_bytes()).await;
                break true;
            }
            Some(Ok(crossterm::event::Event::Key(x))) = events.next().fuse() => {
                match x {
                    KeyEvent { code: KeyCode::Char('q'), modifiers: KeyModifiers::NONE, .. } | KeyEvent { code: KeyCode::Char('Q'), modifiers: KeyModifiers::SHIFT, .. } => {
                        output.write_all(&ClientMessage::Close.into_bytes()).await?;
                        break false;
                    },
                    x if is_closing_key(x) => {
                        output.write_all(&ClientMessage::Close.into_bytes()).await?;
                        break false;
                    }
                    _ => {}
                }

            },
            Ok(_) = input.recv(&mut data) => {
                match ServerMessage::from_bytes(&data[..]) {
                    Some(ServerMessage::Start { .. }) => {}
                    Some(ServerMessage::Quit { force }) => {
                        if force {
                            println!("Server closed, closing...");
                            break false;
                        } else {
                            println!("Server closed, detaching...");
                            break true;
                        }
                    }
                    Some(ServerMessage::Heartbeat) => heartbeat.reset(),
                    Some(ServerMessage::UpdateInterval(x)) => {
                        interval = tokio::time::interval(x);
                        heartbeat = tokio::time::interval(x * 10);
                        heartbeat.tick().await;
                    }
                    None => {}
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
        let Some((address, delay)) = wait_for_server(args.port, camera_format).await? else {
            break;
        };

        println!("Connected to {}", address.to_string().bright_green().bold());

        let retry = client_thread(camera, args.port, address, delay).await?;

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
    let old_term = TermMode::new(stdin.as_raw_fd())?;

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
