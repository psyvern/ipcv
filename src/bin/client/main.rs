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
    utils::{
        CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType, Resolution,
    },

};

use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{
    net::{TcpStream, UdpSocket},
    select,
};
use tokio_util::{codec::FramedWrite, udp::UdpFramed};

const FORMATS: &[FrameFormat] = &[
    FrameFormat::RAWRGB,
    FrameFormat::RAWBGR,
    FrameFormat::YUYV,
    FrameFormat::MJPEG,
];

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
    //optional name
    #[arg(long, short)]
    client_name: Option<String>,
}

fn supported_resolutions(camera: &mut CallbackCamera) -> Vec<(u16, u16)> {
    let mut set: BTreeSet<(u16, u16)> = BTreeSet::new();

    if let Ok(fourccs) = camera.compatible_fourcc() {
        for fcc in fourccs {
            if let Ok(map) = camera.compatible_list_by_resolution(fcc) {
                for res in map.keys() {
                    if let (Ok(w), Ok(h)) = (u16::try_from(res.width()), u16::try_from(res.height()))
                    {
                        set.insert((w, h));
                    }
                }
            }
        }
    }

    // max 100 per restare sotto l'MTU del datagramma UDP
    set.into_iter().take(100).collect()
}


async fn wait_for_server(
    group: IpAddr,
    port: u16,
    camera_format: CameraFormat,
    supported: &[(u16, u16)],
    name: Option<&str>,
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
        host: match name {
            Some(n) => n.to_owned(),
            None => hostname::get()?.to_string_lossy().into_owned(),
        },
        supported: supported.to_vec(),
    }
    .into_bytes();

    let mut events = EventStream::new();
    let mut data = [0u8; 4096];

    let mut denied = false;

    loop {
        if !denied {
            output.send(&initial_message).await?;
        }
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
                                denied = true;
                            }
                        },
                        ServerMessage::Allow => {
                          println!("Allowed again by the server");
                          denied = false;
                        },
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
                    let res = frame.resolution();
                    let msg = ClientMessage::Frame {
                        width: res.width(),
                        height: res.height(),
                        format: frame.source_frame_format(),
                        data: frame.buffer_bytes(),
                    };

                    if let Err(e) = output.send(msg).await {
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
                    },
                    ServerMessage::Allow => {}
                    ServerMessage::SetResolution { width, height } => {
                        let current = camera.camera_format().unwrap();
                        let target = CameraFormat::new(
                            Resolution::new(width, height),
                            current.format(),
                            current.frame_rate(),
                        );
                        let req = RequestedFormat::with_formats(
                            RequestedFormatType::Closest(target),
                            FORMATS,
                        );
                        // il cambio è bloccante: block_in_place evita di fermare il runtime
                        match tokio::task::block_in_place(|| camera.set_camera_requset(req)) {
                            Ok(actual) => println!(
                                "Resolution now {}x{}",
                                actual.width(),
                                actual.height()
                            ),
                            Err(e) => println!("Cannot change resolution ({e})"),
                        }
                    }
                }
            }
        }
    })
}

async fn run(args: Args, camera: &mut CallbackCamera) -> std::io::Result<()> {

    println!(
        "Using camera: {}",
        camera.info().human_name().yellow().bold()
    );
    let supported = supported_resolutions(camera);


    loop {
        let camera_format = camera.camera_format().unwrap();
        let Some((address, delay, input_socket)) = wait_for_server(
            args.group,
            args.port,
            camera_format,
            &supported, //
            args.client_name.as_deref(),
        )
        .await?
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
    let format = RequestedFormat::with_formats(RequestedFormatType::AbsoluteHighestFrameRate, FORMATS);
    let mut camera = CallbackCamera::new(index, format, |_| {}).unwrap();
    camera.open_stream().unwrap();
    camera.poll_frame().unwrap();

    let result = run(args, &mut camera).await;

    camera.stop_stream().unwrap();
    drop(old_term);

    result
}
