use crossterm::event::EventStream;
use futures::StreamExt;
use iced::border::rounded;
use iced::font::Weight;
use iced::widget::{button, column, container, rich_text, row, scrollable, span, text};
use iced::{Alignment, Element, Font, Length, Subscription, Task, Theme, never};
use indexmap::IndexMap;
use ipcv::SenderExt;
use lucide_icons::Icon;
use std::net::{IpAddr, Ipv4Addr};
use tokio::sync::mpsc::UnboundedSender;

use crate::{InterfaceMessage, ServerEvent, map_key};

pub fn run() -> iced::Result {
    iced::application(State::default, update, view_2)
        .fonts([lucide_icons::LUCIDE_FONT_BYTES])
        .title("IPCV Server GUI")
        .subscription(subscription)
        .run()
}

struct ClientState {
    host: String,
    last_frame: Option<iced::widget::image::Handle>,
    frame_number: u64,
    waiting: bool,
}

struct State {
    server_status: String,
    clients: IndexMap<IpAddr, ClientState>,
    gui_tx: Option<UnboundedSender<InterfaceMessage>>,
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
                        waiting: true,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        frame_number: 69,
                        waiting: true,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        frame_number: 81,
                        waiting: true,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 2, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        frame_number: 69,
                        waiting: true,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 4, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        frame_number: 81,
                        waiting: true,
                    },
                ),
            ]
            .into(),
            gui_tx: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    Ready(UnboundedSender<InterfaceMessage>),
    ServerEvent(ServerEvent),
    Interface(InterfaceMessage),
}

fn update(state: &mut State, message: Message) -> Task<Message> {
    match message {
        Message::ServerEvent(event) => match event {
            ServerEvent::Stopped => {
                return iced::exit();
            }
            ServerEvent::ClientConnected { address, host } => {
                state.server_status = format!("Connected to {} ({})", host, address);
                state.clients.insert(
                    address,
                    ClientState {
                        host,
                        last_frame: None,
                        frame_number: 0,
                        waiting: true,
                    },
                );
            }
            ServerEvent::ClientAccepted { address } => {
                if let Some(client) = state.clients.get_mut(&address) {
                    client.waiting = false;
                }
            }
            ServerEvent::FrameReceived {
                address,
                frame_number,
                width,
                height,
                data,
            } => {
                state.server_status = format!("Frame {} from {}", frame_number, address);

                let handle = iced::widget::image::Handle::from_rgba(width, height, data);

                if let Some(client) = state.clients.get_mut(&address) {
                    client.last_frame = Some(handle);
                    client.frame_number = frame_number;
                }
            }
        },
        Message::Ready(tx) => {
            state.server_status = "Server is running...".to_string();
            state.gui_tx = Some(tx);
        }
        Message::Interface(cmd) => {
            match cmd {
                InterfaceMessage::DisconnectClient(address) => {
                    state.clients.shift_remove(&address);
                }
                InterfaceMessage::Move(from, to) => {
                    state.clients.move_index(from, to);
                }
                _ => {}
            }

            if let Some(tx) = &state.gui_tx {
                let _ = tx.send(cmd);
            }
        }
    }

    Task::none()
}

fn view_2(state: &State) -> Element<'_, Message> {
    view(state).map(Message::Interface)
}

fn view(state: &State) -> Element<'_, InterfaceMessage> {
    let header = column![
        text("IPCV Server GUI").size(40),
        text("Status:").size(20),
        text(&state.server_status).size(16),
    ]
    .spacing(20);

    let mut clients_grid = row![].spacing(16);

    for (index, (&address, client)) in state.clients.iter().enumerate() {
        let mut client_col = column![].spacing(8).align_x(Alignment::Center);

        client_col = client_col.push(
            container(if client.waiting {
                Element::new(
                    column![
                        Icon::RotateCwFadingClock.widget().size(96),
                        text("Waiting...").size(16)
                    ]
                    .align_x(Alignment::Center),
                )
            } else {
                if let Some(handle) = &client.last_frame {
                    Element::new(iced::widget::image(handle.clone()).border_radius(8))
                } else {
                    Element::new(
                        column![
                            Icon::ImageOff.widget().size(96),
                            text("No Preview").size(16)
                        ]
                        .align_x(Alignment::Center),
                    )
                }
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
                        span(&client.host).font(Font::DEFAULT.weight(Weight::Bold))
                    ]
                    .size(16)
                    .on_link_click(never),
                    rich_text![
                        span("Address: "),
                        span(address.to_string()).font(Font::DEFAULT.weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                    rich_text![
                        span("Frame Number: "),
                        span(client.frame_number.to_string())
                            .font(Font::DEFAULT.weight(Weight::Bold))
                    ]
                    .size(14)
                    .on_link_click(never),
                ]
                .spacing(4),
            )
            .align_left(360),
        );

        if client.waiting {
            client_col = client_col.push(
                row![
                    button(row![Icon::CircleDashedCheck.widget(), "Accept"].spacing(4))
                        .style(iced::widget::button::success)
                        .on_press(InterfaceMessage::AcceptClient(address)),
                    button(row![Icon::Unplug.widget(), "Deny"].spacing(4))
                        .style(iced::widget::button::danger)
                        .on_press(InterfaceMessage::DisconnectClient(address)),
                ]
                .spacing(8),
            );
        } else {
            client_col = client_col.push(
                row![
                    button(row![Icon::ExternalLink.widget(), "Open folder"].spacing(4))
                        .on_press(InterfaceMessage::OpenFolder(address)),
                    button(row![Icon::Unplug.widget(), "Disconnect"].spacing(4))
                        .style(iced::widget::button::danger)
                        .on_press(InterfaceMessage::DisconnectClient(address)),
                    button(Icon::ArrowLeft.widget())
                        .style(iced::widget::button::background)
                        .on_press_maybe(
                            (index > 0).then(|| InterfaceMessage::Move(index, index - 1))
                        ),
                    button(Icon::ArrowRight.widget())
                        .style(iced::widget::button::background)
                        .on_press_maybe(
                            (index + 1 < state.clients.len())
                                .then(|| InterfaceMessage::Move(index, index + 1))
                        ),
                ]
                .spacing(8),
            );
        }

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
                let _ = output.send(Message::Ready(gui_tx.clone())).await;

                let args = crate::ARGS.get().unwrap().clone();

                if args.tui {
                    tokio::spawn(async move {
                        let mut events = EventStream::new();
                        while let Some(Ok(event)) = events.next().await {
                            if let crossterm::event::Event::Key(x) = event
                                && let Some(x) = map_key(x)
                                && gui_tx.send(x).is_err()
                            {
                                break;
                            }
                        }
                    });
                }

                let mut output = output.map(Message::ServerEvent);
                let _ = crate::server_loop(args, &mut output, gui_rx).await;
                let _ = output.send(ServerEvent::Stopped).await;
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
