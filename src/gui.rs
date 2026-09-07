use futures::SinkExt;
use iced::widget::{column, container, text};
use iced::{Element, Subscription, Task, Theme};
use ipcv::{GuiCommand, ServerEvent};

pub fn run() -> iced::Result {
    iced::application("IPCV Server GUI", update, view)
        .subscription(subscription)
        .theme(|_| Theme::Dark)
        .run()
}

#[derive(Default)]
struct State {
    server_status: String,
    last_frame: Option<iced::widget::image::Handle>,
}

#[derive(Debug, Clone)]
pub enum Message {
    ServerEvent(ServerEvent),
}

fn update(state: &mut State, message: Message) -> Task<Message> {
    match message {
        Message::ServerEvent(event) => {
            match event {
                ServerEvent::Started => {
                    state.server_status = "Server is running...".to_string();
                }
                ServerEvent::Stopped => {
                    return iced::exit();
                }
                ServerEvent::HeartbeatTick => {
                    state.server_status = "Server is running (Heartbeat Ok)".to_string();
                }
                ServerEvent::ClientConnected { address, host } => {
                    state.server_status = format!("Connected to {} ({})", host, address);
                }
                ServerEvent::Log(msg) => {
                    state.server_status = format!("Log: {}", msg);
                }
                ServerEvent::FrameReceived {
                    address,
                    frame_number,
                    frame,
                } => {
                    state.server_status = format!("Frame {} from {}", frame_number, address);
                    
                    let width = frame.width();
                    let height = frame.height();
                    let mut rgba = Vec::with_capacity((width * height * 4) as usize);
                    for pixel in frame.pixels() {
                        rgba.extend_from_slice(&[pixel[0], pixel[1], pixel[2], 255]);
                    }
                    state.last_frame = Some(iced::widget::image::Handle::from_rgba(width, height, rgba));
                }
            }
            Task::none()
        }
    }
}

fn view(state: &State) -> Element<'_, Message> {
    let mut content = column![
        text("IPCV Server GUI").size(40),
        text("Status:").size(20),
        text(&state.server_status).size(16),
    ]
    .spacing(20);

    if let Some(handle) = &state.last_frame {
        content = content.push(iced::widget::image(handle.clone()));
    }

    container(content)
        .center_x(iced::Length::Fill)
        .center_y(iced::Length::Fill)
        .into()
}

fn subscription(_state: &State) -> Subscription<Message> {
    struct ServerSubscription;

    Subscription::run_with_id(
        std::any::TypeId::of::<ServerSubscription>(),
        iced::stream::channel(100, |mut output| async move {
            let _ = output
                .send(Message::ServerEvent(ServerEvent::Started))
                .await;

            let args = crate::ARGS.get().unwrap().clone();

            let _ = crate::server_loop(args, output).await;
        }),
    )
}
