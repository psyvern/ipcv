use clap::Parser;
use colored::Colorize;
use crossterm::event::{EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::{FutureExt, StreamExt};
use ipcv::{GuiCommand, TermMode, is_closing_key};
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
    net::{TcpSocket, TcpStream, UdpSocket},
    select,
};

use futures::SinkExt;

#[path = "../client_gui.rs"]
pub mod gui;

pub static ARGS: std::sync::OnceLock<Args> = std::sync::OnceLock::new();

const MCAST_GRP: &str = "224.1.1.1";

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// Communication port (must be the same in the server)
    #[arg(long, short, default_value_t = 5007)]
    port: u16,
    /// Delay between sending (in seconds)
    #[arg(long, short, default_value_t = 0.5)]
    update_delay: f64,
    /// Log additional data
    #[arg(long, short)]
    debug: bool,
    /// Network interface IP to use
    #[arg(long, default_value = "0.0.0.0")]
    ip: Ipv4Addr,
    /// Camera index
    #[arg(long, short, default_value_t = 0)]
    camera_index: u32,
    /// Enable Graphical User Interface (GUI)
    #[arg(long)]
    gui: bool,
}

async fn wait_for_server(
    port: u16,
    ip: Ipv4Addr,
    camera_format: CameraFormat,
) -> std::io::Result<Option<(SocketAddr, Duration)>> {
    let output = UdpSocket::bind((ip, 0)).await?;
    output.set_ttl(2)?;
    output.connect((MCAST_GRP, port)).await?;

    let input_socket = UdpSocket::bind((ip, port)).await?;
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
    ip: Ipv4Addr,
    address: SocketAddr,
    time: Duration,
    gui_output: &mut futures::channel::mpsc::Sender<gui::Message>,
    gui_rx: &mut tokio::sync::mpsc::UnboundedReceiver<GuiCommand>,
) -> std::io::Result<bool> {
    let input = UdpSocket::bind((ip, port)).await?;

    let socket = TcpSocket::new_v4()?;
    socket.bind(SocketAddr::new(std::net::IpAddr::V4(ip), 0))?;
    let mut output = socket.connect(address).await.unwrap();

    let mut events = EventStream::new();
    let mut data = [0u8; 1024];
    let mut counter = 0;

    let mut interval = tokio::time::interval(time);
    let mut heartbeat = tokio::time::interval(time * 10);
    heartbeat.tick().await;

    Ok(loop {
        select! {
            Some(cmd) = gui_rx.recv() => {
                match cmd {
                    GuiCommand::Disconnect | GuiCommand::Shutdown { .. } => {
                        println!("Disconnecting via GUI command...");
                        output.write_all(&[1]).await?;
                        break false;
                    }
                    _ => {} // Other commands like DisconnectClient don't apply to the client
                }
            }
            _ = interval.tick() => {
                let frame = camera.last_frame();
                if let Ok(frame) = frame {
                    let bytes = frame.buffer_bytes();

                    let _ = output.write_all(&[0]).await;
                    let _ = output.write_all(&(bytes.len() as u32).to_be_bytes()).await;
                    let _ = output.write_all(&bytes).await;

                    // socket.send(counter.to_string().into_bytes());
                    println!("Sending frame: {}", counter.to_string().bright_cyan().bold());
                    let _ = gui_output.send(gui::Message::ClientEvent(gui::ClientEvent::FrameSent { count: counter })).await;
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

async fn run(
    args: Args,
    camera: &mut CallbackCamera,
    gui_output: &mut futures::channel::mpsc::Sender<gui::Message>,
    gui_rx: &mut tokio::sync::mpsc::UnboundedReceiver<GuiCommand>,
) -> std::io::Result<()> {
    let camera_format = camera.camera_format().unwrap();
    println!(
        "Using camera: {}",
        camera.info().human_name().yellow().bold()
    );

    loop {
        let Some((address, delay)) = wait_for_server(args.port, args.ip, camera_format).await?
        else {
            break;
        };

        println!("Connected to {}", address.to_string().bright_green().bold());
        let _ = gui_output
            .send(gui::Message::ClientEvent(gui::ClientEvent::Connected {
                server: address.to_string(),
            }))
            .await;

        let retry = client_thread(
            camera, args.port, args.ip, address, delay, gui_output, gui_rx,
        )
        .await?;

        if !retry {
            break;
        }
    }

    Ok(())
}

pub async fn client_loop(
    args: Args,
    mut gui_output: futures::channel::mpsc::Sender<gui::Message>,
    mut gui_rx: tokio::sync::mpsc::UnboundedReceiver<GuiCommand>,
) -> std::io::Result<()> {
    let old_term = TermMode::new()?;

    let index = CameraIndex::Index(args.camera_index);
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

    let result = run(args, &mut camera, &mut gui_output, &mut gui_rx).await;

    camera.stop_stream().unwrap();
    drop(old_term);

    let _ = gui_output
        .send(gui::Message::ClientEvent(gui::ClientEvent::Stopped))
        .await;

    result
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("{args:?}");
    ARGS.set(args.clone()).unwrap();

    if args.gui {
        gui::run()?;
    } else {
        let rt = tokio::runtime::Runtime::new()?;
        let (gui_output, mut rx) = futures::channel::mpsc::channel(100);
        let (_gui_tx, gui_rx) = tokio::sync::mpsc::unbounded_channel();

        rt.spawn(async move { while rx.next().await.is_some() {} });

        rt.block_on(client_loop(args, gui_output, gui_rx))?;
    }

    Ok(())
}
