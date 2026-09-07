use std::{collections::VecDeque, time::Duration};

#[cfg(unix)]
use std::os::fd::AsRawFd;

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
            fd: std::os::fd::RawFd,
            original: termios::Termios,
        }

        impl TermMode {
            pub fn new() -> std::io::Result<Self> {
                let stdin = std::io::stdin();
                let fd = stdin.as_raw_fd();
                let mut original = termios::Termios::from_fd(fd)?;
                termios::tcgetattr(fd, &mut original)?;

                let mut new = original.clone();
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
            pub fn new() -> std::io::Result<Self> {
                Ok(Self)
            }
        }
    }
}

pub enum ServerMessage {
    Start { port: u16, interval: Duration },
    Quit { force: bool },
    Heartbeat,
}

impl ServerMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Start { port, interval } => {
                format!("start {port}\0{}", interval.as_secs_f64()).into_bytes()
            }
            Self::Quit { force } => format!("quit {force}").into_bytes(),
            Self::Heartbeat => b"heartbeat".to_vec(),
        }
    }
}

pub enum ClientMessage {
    Close,
    Frame(VecDeque<u8>),
}

#[derive(Default)]
pub struct ClientMessageParser {
    data: VecDeque<u8>,
    size: Option<usize>,
}

impl ClientMessageParser {
    pub fn read(&mut self, packet: &[u8]) -> Option<ClientMessage> {
        self.data.extend(packet);

        match self.size {
            None => {
                let first = self.data.pop_front()?;

                if first == 0 {
                    if self.data.len() < 4 {
                        self.data.push_front(0);
                        return None;
                    } else {
                        self.size = Some(u32::from_be_bytes(
                            self.data.drain(..4).next_array().unwrap(),
                        ) as usize);
                    }

                    None
                } else {
                    Some(ClientMessage::Close)
                }
            }
            Some(size) => {
                if self.data.len() >= size {
                    self.size = None;

                    let temp = self.data.split_off(size);
                    let result = std::mem::replace(&mut self.data, temp);

                    Some(ClientMessage::Frame(result))
                } else {
                    None
                }
            }
        }
    }
}

use image::{ImageBuffer, Rgb};
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ServerEvent {
    Started,
    Stopped,
    ClientConnected { address: IpAddr, host: String },
    HeartbeatTick,
    FrameReceived {
        address: IpAddr,
        frame_number: u64,
        frame: Arc<ImageBuffer<Rgb<u8>, Vec<u8>>>,
    },
    Log(String),
}

#[derive(Debug, Clone)]
pub enum GuiCommand {
    DisconnectClient(IpAddr),
    Shutdown { force: bool },
}
