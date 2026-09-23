use std::{
    io::Stdin,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use bytes::Bytes;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use image::{ImageBuffer, Rgb};
use itertools::Itertools;

pub fn is_closing_key(event: KeyEvent) -> bool {
    matches!(
        event,
        KeyEvent {
            code: KeyCode::Char('c'),
            modifiers: KeyModifiers::CONTROL,
            ..
        }
    )
}

cfg_select! {
    unix => {
        use std::os::fd::{AsRawFd, RawFd};

        pub struct TermMode {
            fd: RawFd,
            original: termios::Termios,
        }

        impl TermMode {
            pub fn new(stdin: Stdin) -> std::io::Result<Self> {
                let fd = stdin.as_raw_fd();
                let original = termios::Termios::from_fd(0)?;

                let mut new = original;
                termios::cfmakeraw(&mut new);
                new.c_oflag |= termios::OPOST;

                termios::tcsetattr(fd, termios::TCSAFLUSH, &new)?;

                Ok(Self { fd, original })
            }
        }

        impl Drop for TermMode {
            fn drop(&mut self) {
                termios::tcsetattr(self.fd, termios::TCSAFLUSH, &self.original)
                    .expect("Can't restore term mode");
            }
        }
    }
    _ => {
        pub struct TermMode;

        impl TermMode {
            pub fn new(_: Stdin) -> std::io::Result<Self> {
                Ok(Self)
            }
        }
    }
}

/// A message sent from the server to the client
#[derive(Debug)]
pub enum ServerMessage {
    /// Open the TCP connection
    Open {
        /// The port of the TCP socket
        port: u16,
        /// How often to send frame updates
        interval: Duration,
    },
    /// Close the connection
    Close {
        /// Whether to also quit the client program
        force: bool,
    },
    /// Change the frame update interval
    UpdateInterval(Duration),
    /// Message to keep the connection alive
    Heartbeat,
}

impl ServerMessage {
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Open { port, interval } => {
                let mut result = vec![0];
                result.extend(port.to_be_bytes());
                result.extend(interval.as_secs_f64().to_be_bytes());

                result
            }
            Self::Close { force } => vec![1, force as u8],
            Self::Heartbeat => vec![2],
            Self::UpdateInterval(interval) => {
                let mut result = vec![3];
                result.extend(interval.as_secs_f64().to_be_bytes());

                result
            }
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<ServerMessage> {
        let mut data = data.iter().copied();
        Some(match data.next()? {
            0 => {
                let port = u16::from_be_bytes(data.next_array()?);
                let interval = Duration::from_secs_f64(f64::from_be_bytes(data.next_array()?));

                Self::Open { port, interval }
            }
            1 => {
                let force = data.next()? != 0;
                Self::Close { force }
            }
            2 => Self::Heartbeat,
            3 => {
                let interval = Duration::from_secs_f64(f64::from_be_bytes(data.next_array()?));
                Self::UpdateInterval(interval)
            }
            x => panic!("Received unknown message type: {x}"),
        })
    }
}

/// A message sent from the client to the server
pub enum ClientMessage {
    Frame(Bytes),
    Close,
}

impl ClientMessage {
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Frame(data) => {
                let mut result = vec![0];
                result.extend((data.len() as u32).to_be_bytes());
                result.extend(data);
                result
            }
            Self::Close => vec![1],
        }
    }
}

#[derive(Default)]
pub struct ClientMessageParser {
    data: Vec<u8>,
}

impl ClientMessageParser {
    pub fn read(&mut self, packet: &[u8]) -> Option<ClientMessage> {
        self.data.extend(packet);

        let result = match self.data.first()? {
            0 => {
                let data = self.data.get(1..5)?;
                let size = u32::from_be_bytes(data.try_into().ok()?) as usize;
                if self.data.len() >= 5 + size {
                    let inner = self.data.drain(..5 + size).skip(5).collect();
                    ClientMessage::Frame(inner)
                } else {
                    return None;
                }
            }
            1 => {
                self.data.drain(..1);
                ClientMessage::Close
            }
            x => panic!("Received unknown message type: {x}"),
        };

        Some(result)
    }
}

/// Returns the unspecified IP address for the same IP version.
///
/// Returns `0.0.0.0` for IPv4 and `::` for IPv6.
pub fn unspecified_from(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    }
}

pub fn generate_preview(
    frame: &ImageBuffer<Rgb<u8>, Vec<u8>>,
    max_width: u32,
) -> (u32, u32, Vec<u8>) {
    let width = frame.width();
    let height = frame.height();

    let (new_width, new_height, resized) = if width > max_width {
        let scale = max_width as f32 / width as f32;
        let new_width = max_width;
        let new_height = (height as f32 * scale) as u32;
        (
            new_width,
            new_height,
            image::imageops::resize(
                frame,
                new_width,
                new_height,
                image::imageops::FilterType::Nearest,
            ),
        )
    } else {
        (width, height, frame.clone())
    };

    let mut rgba = Vec::with_capacity((new_width * new_height * 4) as usize);
    for pixel in resized.pixels() {
        rgba.extend_from_slice(&[pixel[0], pixel[1], pixel[2], 255]);
    }

    (new_width, new_height, rgba)
}
