use clap::Parser;
use colored::Colorize;
use crossterm::event::{EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::{FutureExt, StreamExt};
use ipcv::{TermMode, is_closing_key};
use nokhwa::{
    CallbackCamera,
    utils::{CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType},
};
use std::{
    net::{Ipv4Addr, SocketAddr},
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

    let initial_message = format!(
        "open {} {} {} {}",
        camera_format.resolution().width(),
        camera_format.resolution().height(),
        camera_format.format(),
        hostname::get()?.to_string_lossy()
    )
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
                let data = &data[..size];
                let address = address.ip();

                let string_data = String::from_utf8(data.to_vec()).unwrap();
                if let Some(string_data) = string_data.strip_prefix("start ") {
                    let (port, delay) = string_data.split_once('\0').unwrap();
                    let port = port.parse().unwrap();
                    let delay = Duration::from_secs_f64(delay.parse().unwrap());

                    return Ok(Some((SocketAddr::new(address, port), delay)));
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

                    let _ = output.write_all(&[0]).await;
                    let _ = output.write_all(&(bytes.len() as u32).to_be_bytes()).await;
                    let _ = output.write_all(&bytes).await;

                    // socket.send(counter.to_string().into_bytes());
                    println!("Sending frame: {}", counter.to_string().bright_cyan().bold());
                    counter += 1;
                }
            }
            _ = heartbeat.tick() => {
                println!("Server did not heartbeat, detaching...");
                let _ = output.write_all(&[1]).await;
                break true;
            }
            Some(Ok(crossterm::event::Event::Key(x))) = events.next().fuse() => {
                match x {
                    KeyEvent { code: KeyCode::Char('q'), modifiers: KeyModifiers::NONE, .. } | KeyEvent { code: KeyCode::Char('Q'), modifiers: KeyModifiers::SHIFT, .. } => {
                        output.write_all(&[1]).await?;
                        break false;
                    },
                    x if is_closing_key(x) => {
                        output.write_all(&[1]).await?;
                        break false;
                    }
                    _ => {}
                }

            },
            Ok(_) = input.recv(&mut data) => {
                heartbeat.reset();

                let string_data = String::from_utf8(data.to_vec()).unwrap();

                if let Some(string_data) = string_data.strip_prefix("quit ") {
                    let force = string_data.starts_with("true");

                    if force {
                        println!("Server closed, closing...");
                        break false;
                    } else {
                        println!("Server closed, detaching...");
                        break true;
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

    let old_term = TermMode::new()?;

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
