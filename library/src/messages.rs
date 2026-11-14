use std::{fmt::Display, io::Read, marker::PhantomData, time::Duration};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use wincode::{SchemaRead, SchemaWrite, config::DefaultConfig, io::std_read::ReadAdapter};

use crate::{LogLine, threads::FFmpegProgress};

pub struct BinCodec<R, W>(PhantomData<(R, W)>);

impl<R, W> BinCodec<R, W> {
    /// Provides a bincode based codec
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<R, W> Default for BinCodec<R, W> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> BinCodec<R, ()> {
    pub fn decoder() -> Self {
        Self::default()
    }
}

impl<W> BinCodec<(), W> {
    pub fn encoder() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct ReadError(pub wincode::ReadError);

impl Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<wincode::ReadError> for ReadError {
    fn from(value: wincode::ReadError) -> Self {
        Self(value)
    }
}

impl From<std::io::Error> for ReadError {
    fn from(value: std::io::Error) -> Self {
        Self(wincode::ReadError::Io(value.into()))
    }
}

impl<R, W> Decoder for BinCodec<R, W>
where
    R: for<'de> wincode::SchemaRead<'de, DefaultConfig, Dst = R>,
{
    type Item = R;
    type Error = ReadError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.is_empty() {
            let mut reader = Reader::new(&buf[..]);
            let message = wincode::deserialize_from(ReadAdapter::new(&mut reader))?;
            buf.advance(reader.amount());
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub struct WriteError(pub wincode::WriteError);

impl Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<wincode::WriteError> for WriteError {
    fn from(value: wincode::WriteError) -> Self {
        Self(value)
    }
}

impl From<std::io::Error> for WriteError {
    fn from(value: std::io::Error) -> Self {
        Self(wincode::WriteError::Io(value.into()))
    }
}

impl<R, W> Encoder<W> for BinCodec<R, W>
where
    W: SchemaWrite<DefaultConfig, Src = W>,
{
    type Error = WriteError;

    fn encode(&mut self, item: W, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let size = wincode::serialized_size(&item)?;
        buf.reserve(size as usize);
        let message = wincode::serialize(&item)?;
        buf.put(&message[..]);
        Ok(())
    }
}

#[derive(Debug)]
struct Reader<'buf> {
    buf: &'buf [u8],
    amount: usize,
}

impl<'buf> Reader<'buf> {
    pub fn new(buf: &'buf [u8]) -> Self {
        Reader { buf, amount: 0 }
    }

    pub fn amount(&self) -> usize {
        self.amount
    }
}

impl<'buf, 'a> Read for &'a mut Reader<'buf> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.buf.read(buf)?;
        self.amount += bytes_read;
        Ok(bytes_read)
    }
}

/// A message sent from the server to the client
#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum ServerMessage {
    /// Open the TCP connection
    Open {
        /// The port of the TCP socket
        tcp_port: u16,
        /// The port of the UDP socket
        udp_port: u16,
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
    Allow,
}

/// A message sent from the client to the server
#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum ClientMessage {
    Log(LogLine),
    Progress(FFmpegProgress),
    Close,
}

/// The message a client while waiting for a connection
#[derive(SchemaRead, SchemaWrite)]
pub struct ClientInitialMessage {
    pub port: u16,
    pub name: String,
}
