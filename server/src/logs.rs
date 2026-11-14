use std::{fmt::Display, net::IpAddr};

use itertools::Itertools;
use jiff::{ZonedDateTime, civil::DateTime};
use library::{LogLevel, threads::FFmpegProgress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Remote,
    Local,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub time: DateTime,
    pub level: LogLevel,
    pub address: Option<IpAddr>,
    pub message: LogMessage,
}

impl LogEntry {
    pub fn new(
        level: LogLevel,
        address: impl Into<Option<IpAddr>>,
        message: impl Into<LogMessage>,
    ) -> Self {
        Self {
            time: ZonedDateTime::now().into(),
            level,
            address: address.into(),
            message: message.into(),
        }
    }

    pub fn trace(address: impl Into<Option<IpAddr>>, message: impl Into<LogMessage>) -> Self {
        Self::new(LogLevel::Trace, address, message)
    }

    pub fn info(address: impl Into<Option<IpAddr>>, message: impl Into<LogMessage>) -> Self {
        Self::new(LogLevel::Info, address, message)
    }

    pub fn to_file_line(&self) -> String {
        format!(
            "({}) {}: {}",
            self.time.strftime("%Y-%m-%d %H:%M:%S"),
            match self.level {
                LogLevel::Trace => "TRACE",
                LogLevel::Info => " INFO",
                LogLevel::Warning => " WARN",
                LogLevel::Error => "ERROR",
            },
            self.message,
        )
    }
}

/// Messages written to the log file and shown on the interface
#[derive(Debug, Clone)]
pub enum LogMessage {
    /// The server was stopped
    Stopped,
    Connected {
        name: String,
    },
    Disconnected,
    Accepted,
    Denied,
    FFmpeg {
        parts: Vec<(String, u64)>,
        message: String,
        origin: Origin,
    },
    Frame {
        width: u32,
        height: u32,
        data: Box<[u8]>,
    },
    Progress(FFmpegProgress),
    Other(String),
}

impl LogMessage {
    pub fn should_log(&self) -> bool {
        !matches!(self, Self::Frame { .. } | Self::Progress(_))
    }
}

impl Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => write!(f, "Stopped"),
            Self::Connected { name } => write!(f, "Connected as {name}"),
            Self::Disconnected => write!(f, "Disconnected"),
            Self::Accepted => write!(f, "Accepted"),
            Self::Denied => write!(f, "Denied"),
            Self::FFmpeg {
                parts,
                message,
                origin,
            } => write!(
                f,
                "{{{}}} {}{message}",
                match origin {
                    Origin::Remote => "remote",
                    Origin::Local => "local",
                },
                parts
                    .iter()
                    .map(|x| format!("[{} @ 0x{:X}] ", x.0, x.1))
                    .join("")
            ),
            Self::Frame { .. } => write!(f, "Frame"),
            Self::Progress(_) => write!(f, "Progress"),
            Self::Other(message) => write!(f, "{message}"),
        }
    }
}

impl From<String> for LogMessage {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}
