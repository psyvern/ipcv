use std::{marker::PhantomData, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use nokhwa::utils::{CameraFormat, FrameFormat};
use tokio_util::codec::{Decoder, Encoder};

pub trait Encode: Sized {
    fn encode(self, src: &mut bytes::BytesMut);

    fn encoder() -> CustomCodec<Self, ()> {
        CustomCodec(PhantomData)
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut dst = BytesMut::new();
        self.encode(&mut dst);
        dst.to_vec()
    }
}

pub trait Decode: Sized {
    fn decode(src: &mut bytes::BytesMut) -> Option<Self>;

    fn decoder() -> CustomCodec<(), Self> {
        CustomCodec(PhantomData)
    }

    fn from_bytes(data: &[u8]) -> Option<Self> {
        let mut src = BytesMut::from(data);
        Self::decode(&mut src)
    }
}

pub struct CustomCodec<E, D>(PhantomData<(E, D)>);

impl<E: Encode, D: Decode> CustomCodec<E, D> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: Encode, D> Encoder<E> for CustomCodec<E, D> {
    type Error = std::io::Error;

    fn encode(&mut self, item: E, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

impl<E, D: Decode> Decoder for CustomCodec<E, D> {
    type Error = std::io::Error;

    type Item = D;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(D::decode(src))
    }
}

/// A message sent from the server to the client
#[derive(Debug)]
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

impl Encode for ServerMessage {
    fn encode(self, dst: &mut bytes::BytesMut) {
        match self {
            Self::Open {
                tcp_port,
                udp_port,
                interval,
            } => {
                dst.put_u8(0);
                dst.extend(tcp_port.to_be_bytes());
                dst.extend(udp_port.to_be_bytes());
                dst.extend(interval.as_secs_f64().to_be_bytes());
            }
            Self::Close { force } => dst.extend([1, force as u8]),
            Self::Heartbeat => dst.put_u8(2),
            Self::UpdateInterval(interval) => {
                dst.put_u8(3);
                dst.extend(interval.as_secs_f64().to_be_bytes());
            }
            Self::Allow => {
                dst.put_u8(4);
            }
        }
    }
}

impl Decode for ServerMessage {
    fn decode(src: &mut bytes::BytesMut) -> Option<Self> {
        let src = &mut src.split();
        Some(match src.try_get_u8().ok()? {
            0 => {
                let tcp_port = src.try_get_u16().ok()?;
                let udp_port = src.try_get_u16().ok()?;
                let interval = Duration::try_from_secs_f64(src.try_get_f64().ok()?).ok()?;

                Self::Open {
                    tcp_port,
                    udp_port,
                    interval,
                }
            }
            1 => {
                let force = src.try_get_u8().ok()? != 0;
                Self::Close { force }
            }
            2 => Self::Heartbeat,
            3 => {
                let interval = Duration::try_from_secs_f64(src.try_get_f64().ok()?).ok()?;
                Self::UpdateInterval(interval)
            }
            4 => Self::Allow,
            _ => return None,
        })
    }
}

/// A message sent from the client to the server
pub enum ClientMessage {
    Close,
}

impl Encode for ClientMessage {
    fn encode(self, dst: &mut bytes::BytesMut) {
        match self {
            Self::Close => dst.put_u8(1),
        }
    }
}

impl Decode for ClientMessage {
    fn decode(src: &mut bytes::BytesMut) -> Option<Self> {
        let result = match src.first()? {
            1 => {
                src.advance(1);
                Self::Close
            }
            x => panic!("Received unknown message type: {x}"),
        };

        Some(result)
    }
}

/// The message a client while waiting for a connection
pub struct ClientInitialMessage {
    pub port: u16,
    pub format: CameraFormat,
    pub host: String,
}

impl Encode for ClientInitialMessage {
    fn encode(self, dst: &mut bytes::BytesMut) {
        dst.extend(b"open ");
        dst.put_u16(self.port);
        dst.put_u32(self.format.width());
        dst.put_u32(self.format.height());
        dst.put_u32(self.format.frame_rate());
        dst.put_u8(match self.format.format() {
            FrameFormat::MJPEG => 0,
            FrameFormat::YUYV => 1,
            FrameFormat::NV12 => 2,
            FrameFormat::GRAY => 3,
            FrameFormat::RAWRGB => 4,
            FrameFormat::RAWBGR => 5,
        });
        dst.extend(self.host.bytes());
    }
}

impl Decode for ClientInitialMessage {
    fn decode(src: &mut bytes::BytesMut) -> Option<Self> {
        let src = &mut src.split();
        let data = src.strip_prefix(b"open ")?;

        let mut data = data.iter().copied();
        let port = u16::from_be_bytes(data.next_array()?);
        let width = u32::from_be_bytes(data.next_array()?);
        let height = u32::from_be_bytes(data.next_array()?);
        let frame_rate = u32::from_be_bytes(data.next_array()?);
        let format = match data.next()? {
            0 => FrameFormat::MJPEG,
            1 => FrameFormat::YUYV,
            2 => FrameFormat::NV12,
            3 => FrameFormat::GRAY,
            4 => FrameFormat::RAWRGB,
            5 => FrameFormat::RAWBGR,
            _ => return None,
        };
        let host = String::from_utf8(data.collect()).ok()?;

        Some(Self {
            port,
            format: CameraFormat::new_from(width, height, format, frame_rate),
            host,
        })
    }
}
