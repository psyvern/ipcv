use futures::SinkExt;
use iced::widget::{button, column, container, row, scrollable, text};
use iced::{Element, Subscription, Task, Theme};
use ipcv::{GuiCommand, ServerEvent};
use std::collections::HashMap;
use std::net::IpAddr;

pub fn run() -> iced::Result {
    iced::application("IPCV Server GUI", update, view)
        .subscription(subscription)
        .theme(|_| Theme::Dark)
        .run()
}

struct ClientState {
    host: String,
    last_frame: Option<iced::widget::image::Handle>,
    frame_number: u64,
}

#[derive(Default)]
struct State {
    server_status: String,
    clients: HashMap<IpAddr, ClientState>,
    waiting_clients: HashMap<IpAddr, String>,
    gui_tx: Option<tokio::sync::mpsc::UnboundedSender<GuiCommand>>,
}

#[derive(Debug, Clone)]
pub enum Message {
    ServerEvent(ServerEvent),
    GuiTxReady(tokio::sync::mpsc::UnboundedSender<GuiCommand>),
    SendCommand(GuiCommand),
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
                    state.clients.insert(
                        address,
                        ClientState {
                            host,
                            last_frame: None,
                            frame_number: 0,
                        },
                    );
                }
                ServerEvent::Log(msg) => {
                    state.server_status = format!("Log: {}", msg);
                }
                ServerEvent::FrameReceived {
                    address,
                    frame_number,
                    preview_width,
                    preview_height,
                    preview_rgba,
                } => {
                    state.server_status = format!("Frame {} from {}", frame_number, address);

                    let handle = iced::widget::image::Handle::from_rgba(preview_width, preview_height, preview_rgba);

                    let client = state.clients.entry(address).or_insert_with(|| ClientState {
                        host: "Unknown".to_string(),
                        last_frame: None,
                        frame_number: 0,
                    });
                    client.last_frame = Some(handle);
                    client.frame_number = frame_number;
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
                let _ = tx.send(cmd.clone());
            }
            match cmd {
                GuiCommand::DisconnectClient(address) => {
                    if let Some(client) = state.clients.remove(&address) {
                        state.waiting_clients.insert(address, client.host);
                    }
                }
                GuiCommand::AcceptClient(address) => {
                    state.waiting_clients.remove(&address);
                }
                _ => {}
            }
            Task::none()
        }
    }
}

fn view(state: &State) -> Element<'_, Message> {
    let mut header = column![
        text("IPCV Server GUI").size(40),
        text("Status:").size(20),
        text(&state.server_status).size(16),
    ]
    .spacing(20);

    if !state.waiting_clients.is_empty() {
        let mut waiting_col = column![text("Waiting Clients:").size(20)].spacing(10);
        for (address, host) in &state.waiting_clients {
            waiting_col = waiting_col.push(
                row![
                    text(format!("{} ({})", host, address)).size(16),
                    button("Accept").on_press(Message::SendCommand(GuiCommand::AcceptClient(*address)))
                ].spacing(10)
            );
        }
        header = header.push(waiting_col);
    }

    let mut clients_row = row![].spacing(20);

    for (address, client) in &state.clients {
        let mut client_col = column![].spacing(10);

        if let Some(handle) = &client.last_frame {
            client_col = client_col
                .push(iced::widget::image(handle.clone()).width(iced::Length::Fixed(340.0)));
        } else {
            client_col = client_col.push(
                container(text("No Preview").size(16))
                    .center_x(340.0)
                    .center_y(255.0),
            );
        }

        let client_data = column![
            text(format!("Host: {}", client.host)).size(16),
            text(format!("Address: {}", address)).size(14),
            text(format!("Frame Number: {}", client.frame_number)).size(14),
            button("Disconnect").on_press(Message::SendCommand(GuiCommand::DisconnectClient(
                *address,
            ))),
        ]
        .spacing(5);

        client_col = client_col.push(client_data);

        clients_row = clients_row.push(container(client_col).padding(10));
    }

    let scrollable_clients = iced::widget::Scrollable::with_direction(
        clients_row,
        scrollable::Direction::Horizontal(scrollable::Scrollbar::new()),
    );

    let content = column![header, scrollable_clients].spacing(40);

    container(content)
        .padding(20)
        .center_x(iced::Length::Fill)
        .center_y(iced::Length::Fill)
        .into()
}

fn subscription(_state: &State) -> Subscription<Message> {
    struct ServerSubscription;

    Subscription::run_with_id(
        std::any::TypeId::of::<ServerSubscription>(),
        iced::stream::channel(100, |mut output| async move {
            // gui_tx is sent back to the GUI state so UI interactions can send commands.
            // gui_rx is passed into the background server loop so it can receive and process these commands.
            let (gui_tx, gui_rx) = tokio::sync::mpsc::unbounded_channel();
            let _ = output.send(Message::GuiTxReady(gui_tx)).await;

            let _ = output
                .send(Message::ServerEvent(ServerEvent::Started))
                .await;

            let args = crate::ARGS.get().unwrap().clone();

            let _ = crate::server_loop(args, output, gui_rx).await;
        }),
    )
}
