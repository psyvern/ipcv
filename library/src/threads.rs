use futures::Stream;
use std::{collections::HashMap, marker::Unpin, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    time::timeout,
};

use crate::{LogLevel, LogLine};

#[derive(Clone, Debug)]
pub struct FFmpegProgress {
    pub frame: usize,
    pub fps: f64,
    pub q: f64,
    pub bitrate: String,
    pub total_size: usize,
    pub time: Duration,
    pub dup_frames: usize,
    pub drop_frames: usize,
    pub speed: f64,
}

impl FFmpegProgress {
    pub fn from_map(mut values: HashMap<String, String>) -> Option<Self> {
        Some(Self {
            frame: values.remove("frame")?.parse().ok()?,
            fps: values.remove("fps")?.parse().ok()?,
            q: values.remove("stream_0_0_q")?.parse().ok()?,
            bitrate: values.remove("bitrate")?.parse().ok()?,
            total_size: values.remove("total_size")?.parse().ok()?,
            time: Duration::from_micros(values.remove("out_time_us")?.parse().ok()?),
            dup_frames: values.remove("dup_frames")?.parse().ok()?,
            drop_frames: values.remove("drop_frames")?.parse().ok()?,
            speed: values.remove("speed")?.trim_end_matches('x').parse().ok()?,
        })
    }
}

pub fn stdout_stream(
    stdout: impl AsyncRead + Unpin,
) -> impl Stream<Item = std::io::Result<(u32, u32, Vec<u8>)>> {
    async_stream::try_stream! {
        let mut stdout = BufReader::new(stdout);

        let mut header = [0u8; 6];
        loop {
            stdout.read_exact(&mut header).await?;

            if !(header[0] == b'B' && header[1] == b'M') {
                eprintln!("unexpected header: {header:?}");
                break;
            }

            let size = u32::from_le_bytes([header[2], header[3], header[4], header[5]]) as usize;

            let mut buf = vec![0; size];
            stdout.read_exact(&mut buf[6..]).await?;

            let offset = u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]) as usize;

            let width = u32::from_le_bytes([buf[18], buf[19], buf[20], buf[21]]);
            let height = u32::from_le_bytes([buf[22], buf[23], buf[24], buf[25]]);
            let data = buf.split_off(offset);
            let data = crate::bgra_to_rgba(data);

            yield (width, height, data);
        }
    }
}

pub enum StderrMessage {
    Log(LogLine),
    Progress(FFmpegProgress),
}

pub fn stderr_stream(
    stderr: impl AsyncRead + Unpin,
) -> impl Stream<Item = std::io::Result<StderrMessage>> {
    async_stream::try_stream! {
        let mut stderr = BufReader::new(stderr).lines();
        let mut map = HashMap::new();

        let mut last = None::<(LogLine, bool)>;

        loop {
            let line = match timeout(Duration::from_secs(1), stderr.next_line()).await {
                Ok(x) => match x? {
                    None => break,
                    Some(x) => x,
                },
                Err(_) => {
                    if let Some((line, _)) = last.take() {
                        yield StderrMessage::Log(line);
                    }

                    continue;
                },
            };

            if let Some((key, value)) = line.split_once('=')
                && key
                    .chars()
                    .all(|x| matches!(x, '0'..='9' | '_' | 'a'..='z'))
            {
                if key == "progress" {
                    let progress = FFmpegProgress::from_map(map);
                    map = HashMap::new();

                    if let Some(progress) = progress {
                        yield StderrMessage::Progress(progress);
                    }
                } else {
                    map.insert(key.to_owned(), value.trim().to_owned());
                }
            } else {
                let mut parts = Vec::new();
                let mut line = line.as_str();
                while let Some(new) = line.strip_prefix('[') {
                    if let Some((part, new)) = new.split_once("] ") {
                        parts.push(part);
                        line = new;
                    } else {
                        break;
                    }
                }
                let level: Option<LogLevel> = parts.pop().and_then(|x| x.parse().ok());
                match level {
                    Some(level) => {
                        if let Some((last_line, true)) = &mut last && level == LogLevel::Info && line.starts_with(' ') {
                            last_line.line.push('\n');
                            last_line.line.push_str(line);
                        } else {
                            if let Some((line, _)) = last {
                                yield StderrMessage::Log(line);
                            }

                            last = Some((
                                LogLine {
                                    level,
                                    parts: parts.into_iter().map(str::to_owned).collect(),
                                    line: line.to_owned(),
                                },
                                line.starts_with("Input #")
                                    || line.starts_with("Output #")
                                    || line.starts_with("Stream mapping:")
                            ));
                        }
                    }
                    None => {
                        if let Some((last_line, _)) = &mut last {
                            last_line.line.push('\n');
                            last_line.line.push_str(line);
                        }
                    }
                }
            }
        }

        if let Some((line, _)) = last {
            yield StderrMessage::Log(line);
        }
    }
}
