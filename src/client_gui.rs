use futures::SinkExt;
use iced::widget::{button, column, container, text};
use iced::{Element, Subscription, Task, Theme};
use ipcv::GuiCommand;

#[derive(Debug, Clone)]
pub enum ClientEvent {
    Started,
    Stopped,
    Connected { server: String },
    Disconnected,
    FrameSent { count: u64 },
    Log(String),
    PreviewFrame(u32, u32, Vec<u8>),
}

pub fn run() -> iced::Result {
    iced::application("IPCV Client GUI", update, view)
        .subscription(subscription)
        .theme(|_| Theme::Dark)
        .run()
}

#[derive(Default)]
struct State {
    status: String,
    gui_tx: Option<tokio::sync::mpsc::UnboundedSender<GuiCommand>>,
    preview_frame: Option<iced::widget::image::Handle>,
}

#[derive(Debug, Clone)]
pub enum Message {
    ClientEvent(ClientEvent),
    GuiTxReady(tokio::sync::mpsc::UnboundedSender<GuiCommand>),
    SendCommand(GuiCommand),
}

fn update(state: &mut State, message: Message) -> Task<Message> {
    match message {
        Message::ClientEvent(event) => {
            match event {
                ClientEvent::Started => {
                    state.status = "Client is running...".to_string();
                }
                ClientEvent::Stopped => {
                    return iced::exit();
                }
                ClientEvent::Connected { server } => {
                    state.status = format!("Connected to {}", server);
                }
                ClientEvent::Disconnected => {
                    state.status = "Disconnected".to_string();
                }
                ClientEvent::FrameSent { count } => {
                    state.status = format!("Sent frame {}", count);
                }
                ClientEvent::Log(msg) => {
                    state.status = format!("Log: {}", msg);
                }
                ClientEvent::PreviewFrame(width, height, rgba) => {
                    state.preview_frame = Some(iced::widget::image::Handle::from_rgba(width, height, rgba));
                }
            }
            Task::none()
        }
        Message::GuiTxReady(tx) => {
            state.gui_tx = Some(tx);
            Task::none()
        }
        Message::SendCommand(cmd) => {
            if let Some(tx) = &state.gui_tx {
                let _ = tx.send(cmd);
            }
            Task::none()
        }
    }
}

fn view(state: &State) -> Element<'_, Message> {
    let mut content = column![
        text("IPCV Client GUI").size(40),
        text("Status:").size(20),
        text(&state.status).size(16),
    ]
    .spacing(20);

    if let Some(handle) = &state.preview_frame {
        content = content.push(iced::widget::image(handle.clone()).width(iced::Length::Fixed(340.0)));
    } else {
        content = content.push(
            container(text("No Preview").size(16))
                .center_x(340.0)
                .center_y(255.0),
        );
    }

    content = content.push(button("Disconnect").on_press(Message::SendCommand(GuiCommand::Disconnect)));

    container(content)
        .center_x(iced::Length::Fill)
        .center_y(iced::Length::Fill)
        .into()
}

fn subscription(_state: &State) -> Subscription<Message> {
    struct ClientSubscription;

    Subscription::run_with_id(
        std::any::TypeId::of::<ClientSubscription>(),
        iced::stream::channel(100, |mut output| async move {
            let (gui_tx, gui_rx) = tokio::sync::mpsc::unbounded_channel();
            let _ = output.send(Message::GuiTxReady(gui_tx)).await;

            let _ = output
                .send(Message::ClientEvent(ClientEvent::Started))
                .await;

            let args = crate::ARGS.get().unwrap().clone();

            let _ = crate::client_loop(args, output, gui_rx).await;
        }),
    )
}
