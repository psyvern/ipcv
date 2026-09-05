use std::{collections::VecDeque, os::fd::RawFd, time::Duration};

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

pub fn set_term_mode(fd: RawFd) -> std::io::Result<termios::Termios> {
    let mut old = termios::Termios::from_fd(0)?;
    termios::tcgetattr(fd, &mut old)?;

    let mut new = old.clone();
    termios::cfmakeraw(&mut new);
    new.c_oflag |= termios::OPOST;

    termios::tcsetattr(fd, termios::TCSAFLUSH, &new)?;

    Ok(old)
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
