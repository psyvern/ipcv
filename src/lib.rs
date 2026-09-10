use std::{os::fd::RawFd, time::Duration};

use bytes::Bytes;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
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
        pub struct TermMode {
            fd: RawFd,
            original: termios::Termios,
        }
    }
    _ => {
        pub struct TermMode;
    }
}

impl TermMode {
    pub fn new(fd: RawFd) -> std::io::Result<Self> {
        cfg_select! {
            unix => {
                let mut original = termios::Termios::from_fd(0)?;
                termios::tcgetattr(fd, &mut original)?;

                let mut new = original.clone();
                termios::cfmakeraw(&mut new);
                new.c_oflag |= termios::OPOST;

                termios::tcsetattr(fd, termios::TCSAFLUSH, &new)?;

                Ok(Self { fd, original })
            }
            _ => Ok(Self),
        }
    }
}

impl Drop for TermMode {
    fn drop(&mut self) {
        cfg_select! {
            unix => {
                termios::tcsetattr(self.fd, termios::TCSAFLUSH, &self.original)
                    .expect("Can't restore term mode");
            }
            _ => {}
        }
    }
}

/// A message sent from the server to the client
#[derive(Debug)]
pub enum ServerMessage {
    /// Open the TCP connection
    Start {
        /// The port of the TCP socket
        port: u16,
        /// How often to send frame updates
        interval: Duration,
    },
    /// Close the connection
    Quit {
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
            Self::Start { port, interval } => {
                let mut result = vec![0];
                result.extend(port.to_be_bytes());
                result.extend(interval.as_secs_f64().to_be_bytes());

                result
            }
            Self::Quit { force } => vec![1, force as u8],
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

                Self::Start { port, interval }
            }
            1 => {
                let force = data.next()? != 0;
                Self::Quit { force }
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

/// A message sent from the server to the client
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
