pub mod messages;

use std::{
    io::Stdin,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use futures::{Sink, SinkExt};
use image::{ImageBuffer, Rgb};

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
