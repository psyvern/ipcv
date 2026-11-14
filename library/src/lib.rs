pub mod messages;
pub mod threads;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use futures::{Sink, SinkExt};
use itertools::Itertools;
use std::{
    fmt::Display,
    io::Stdin,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};
use strum::EnumIter;
use wincode::{SchemaRead, SchemaWrite};

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

/// Returns the unspecified IP address for the same IP version.
///
/// Returns `0.0.0.0` for IPv4 and `::` for IPv6.
pub fn unspecified_from(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    }
}

pub fn bgra_to_rgba(frame: Vec<u8>) -> Vec<u8> {
    frame
        .into_iter()
        .tuples()
        .flat_map(|(b, g, r, a)| [r, g, b, a])
        .collect()
}

#[derive(Clone)]
pub struct MapSender<S, F> {
    inner: S,
    map: F,
}

impl<S, F> MapSender<S, F> {
    pub fn new(inner: S, map: F) -> Self {
        Self { inner, map }
    }
}

pub trait CustomSender<T>: Sized + Send + 'static {
    type Output<'a>: Future + Send
    where
        Self: 'a;
    fn send(&mut self, message: T) -> Self::Output<'_>;
}

impl<S, F, T, I> CustomSender<I> for MapSender<S, F>
where
    I: Send + 'static,
    S: Sink<T> + Send + Unpin + 'static,
    F: Fn(I) -> T + Clone + Send + 'static,
    T: Send,
{
    type Output<'a> = futures::sink::Send<'a, S, T>;

    fn send(&mut self, message: I) -> Self::Output<'_> {
        SinkExt::send(&mut self.inner, (self.map)(message))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, SchemaRead, SchemaWrite, EnumIter)]
pub enum LogLevel {
    Trace,
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct LogLine {
    pub level: LogLevel,
    pub parts: Vec<(String, u64)>,
    pub message: String,
}

impl Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[\x1b[{};1m{}\x1b[0m] {}{}",
            match self.level {
                LogLevel::Trace => "96",
                LogLevel::Info => "92",
                LogLevel::Warning => "93",
                LogLevel::Error => "91",
            },
            match self.level {
                LogLevel::Trace => "trace",
                LogLevel::Info => " info",
                LogLevel::Warning => " warn",
                LogLevel::Error => "error",
            },
            self.parts
                .iter()
                .map(|(a, b)| format!("[{a} @ 0x{b:X}] "))
                .collect::<String>(),
            self.message
        )
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "info" => Self::Info,
            "warning" => Self::Warning,
            "error" => Self::Error,
            _ => return Err(s.to_owned()),
        })
    }
}
