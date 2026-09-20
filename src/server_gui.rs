use futures::SinkExt;
use iced::border::rounded;
use iced::font::Weight;
use iced::theme::Custom;
use iced::theme::palette::Seed;
use iced::widget::{button, column, container, rich_text, row, scrollable, span, text};
use iced::{Alignment, Element, Font, Length, Subscription, Task, Theme, color, never};
use indexmap::IndexMap;
use ipcv::{GuiCommand, ServerEvent};
use lucide_icons::Icon;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

pub fn run() -> iced::Result {
    let seed = Seed {
        background: color!(0x201d2e), // Base
        text: color!(0xcdd6f4),       // Text
        primary: color!(0xb0a0fd),    // Blue
        success: color!(0xa6e3a1),    // Green
        warning: color!(0xf9e2af),    // Yellow
        danger: color!(0xf38ba8),     // Red
    };

    iced::application(State::default, update, view)
        .font(Font::with_family("Outfit").weight(Weight::Medium))
        .fonts([lucide_icons::LUCIDE_FONT_BYTES])
        .title(|_: &State| "IPCV Server GUI".to_owned())
        .subscription(subscription)
        .theme(move |_: &State| Theme::Custom(Custom::new("Custom", seed).into()))
        .run()
}

struct ClientState {
    host: String,
    last_frame: Option<iced::widget::image::Handle>,
    frame_number: u64,
}

struct State {
    server_status: String,
    clients: IndexMap<IpAddr, ClientState>,
    waiting_clients: HashMap<IpAddr, String>,
    gui_tx: Option<tokio::sync::mpsc::UnboundedSender<GuiCommand>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            server_status: Default::default(),
            clients: [
                (
                    IpAddr::V4(Ipv4Addr::new(255, 0, 1, 2)),
                    ClientState {
                        host: "ciaoooo".to_owned(),
                        last_frame: None,
                        frame_number: 8,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        frame_number: 69,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        frame_number: 81,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 2, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        frame_number: 69,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 4, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        frame_number: 81,
                    },
                ),
            ]
            .into(),
            waiting_clients: Default::default(),
            gui_tx: Default::default(),
        }
    }
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

                    let handle = iced::widget::image::Handle::from_rgba(
                        preview_width,
                        preview_height,
                        preview_rgba,
                    );

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
                    if let Some(client) = state.clients.shift_remove(&address) {
                        state.waiting_clients.insert(address, client.host);
                    }
                }
                GuiCommand::AcceptClient(address) => {
                    state.waiting_clients.remove(&address);
                }
                GuiCommand::Move(from, to) => {
                    state.clients.move_index(from, to);
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
                    button("Accept")
                        .on_press(Message::SendCommand(GuiCommand::AcceptClient(*address)))
                ]
                .spacing(10),
            );
        }
        header = header.push(waiting_col);
    }

    let mut clients_grid = row![].spacing(16);

    for (index, (&address, client)) in state.clients.iter().enumerate() {
        let mut client_col = column![].spacing(8).align_x(Alignment::Center);

        client_col = client_col.push(
            container(if let Some(handle) = &client.last_frame {
                Element::new(iced::widget::image(handle.clone()).border_radius(8))
            } else {
                Element::new(
                    column![
                        // text()
                        Icon::ImageOff.widget().size(96),
                        text("No Preview").size(16)
                    ]
                    .align_x(Alignment::Center),
                )
            })
            .center_x(360)
            .center_y(270)
            .clip(true)
            .style(|theme: &Theme| {
                container::Style::default()
                    .background(theme.palette().background.neutral.color)
                    .border(rounded(8))
            }),
        );

        client_col = client_col.push(
            container(
                column![
                    rich_text![
                        span("Host: "),
                        span(&client.host).font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(16)
                    .on_link_click(never),
                    rich_text![
                        span("Address: "),
                        span(address.to_string())
                            .font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                    rich_text![
                        span("Frame Number: "),
                        span(client.frame_number.to_string())
                            .font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                ]
                .spacing(4),
            )
            .align_left(360),
        );
        client_col = client_col.push(
            row![
                button(row![Icon::ExternalLink.widget(), "Open folder"].spacing(4))
                    .on_press(Message::SendCommand(GuiCommand::OpenFolder(address))),
                button(row![Icon::Unplug.widget(), "Disconnect"].spacing(4))
                    .style(iced::widget::button::danger)
                    .on_press(Message::SendCommand(GuiCommand::DisconnectClient(address))),
                button(Icon::ArrowLeft.widget())
                    .style(iced::widget::button::background)
                    .on_press_maybe(
                        (index > 0)
                            .then(|| Message::SendCommand(GuiCommand::Move(index, index - 1)))
                    ),
                button(Icon::ArrowRight.widget())
                    .style(iced::widget::button::background)
                    .on_press_maybe(
                        (index + 1 < state.clients.len())
                            .then(|| Message::SendCommand(GuiCommand::Move(index, index + 1)))
                    ),
            ]
            .spacing(8),
        );

        clients_grid = clients_grid.push(
            container(client_col)
                .style(|theme| {
                    container::Style::default()
                        .background(theme.palette().background.weakest.color)
                        .border(rounded(16))
                })
                .padding(8),
        );
    }

    for (&address, client) in &state.waiting_clients {
        let mut client_col = column![].spacing(8).align_x(Alignment::Center);

        client_col = client_col.push(
            container(Element::new(
                column![
                    // text()
                    Icon::ImageOff.widget().size(96),
                    text("No Preview").size(16)
                ]
                .align_x(Alignment::Center),
            ))
            .center_x(360)
            .center_y(270)
            .clip(true)
            .style(|theme: &Theme| {
                container::Style::default()
                    .background(theme.palette().background.neutral.color)
                    .border(rounded(8))
            }),
        );

        client_col = client_col.push(
            container(
                column![
                    rich_text![
                        span("Host: "),
                        span(client).font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(16)
                    .on_link_click(never),
                    rich_text![
                        span("Address: "),
                        span(address.to_string())
                            .font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                    rich_text![
                        span("Frame Number: "),
                        span("").font(Font::with_family("Outfit").weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                ]
                .spacing(4),
            )
            .align_left(360),
        );
        client_col = client_col.push(
            row![
                button("Accept").on_press(Message::SendCommand(GuiCommand::AcceptClient(address))),
                button(row![Icon::Unplug.widget(), "Disconnect"].spacing(4))
                    .style(iced::widget::button::danger)
                    .on_press(Message::SendCommand(GuiCommand::DisconnectClient(address))),
            ]
            .spacing(8),
        );

        clients_grid = clients_grid.push(
            container(client_col)
                .style(|theme| {
                    container::Style::default()
                        .background(theme.palette().background.weakest.color)
                        .border(rounded(16))
                })
                .padding(8),
        );
    }

    let scrollable_clients = iced::widget::Scrollable::with_direction(
        clients_grid
            .wrap()
            .vertical_spacing(16)
            .align_x(Alignment::Center),
        scrollable::Direction::Vertical(scrollable::Scrollbar::new().spacing(8)),
    );

    let content = column![header, scrollable_clients].spacing(40);

    container(content)
        .padding(20)
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into()
}

fn subscription(_state: &State) -> Subscription<Message> {
    struct ServerSubscription;

    Subscription::run_with(std::any::TypeId::of::<ServerSubscription>(), |_| {
        iced::stream::channel(
            100,
            |mut output: futures::channel::mpsc::Sender<Message>| async move {
                // gui_tx is sent back to the GUI state so UI interactions can send commands.
                // gui_rx is passed into the background server loop so it can receive and process these commands.
                let (gui_tx, gui_rx) = tokio::sync::mpsc::unbounded_channel();
                let _ = output.send(Message::GuiTxReady(gui_tx)).await;

                let _ = output
                    .send(Message::ServerEvent(ServerEvent::Started))
                    .await;

                let args = crate::ARGS.get().unwrap().clone();

                let _ = crate::server_loop(args, output, gui_rx).await;
            },
        )
    })
}

trait IconToWidget {
    fn widget<'a, Theme>(self) -> iced::widget::Text<'a, Theme>
    where
        Theme: iced::widget::text::Catalog + 'a;
}

impl IconToWidget for Icon {
    fn widget<'a, Theme>(self) -> iced::widget::Text<'a, Theme>
    where
        Theme: iced::widget::text::Catalog + 'a,
    {
        iced::widget::text(char::from(self).to_string()).font(iced::Font::with_family("lucide"))
    }
}
