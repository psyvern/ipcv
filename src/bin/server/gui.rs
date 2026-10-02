use futures::StreamExt;
use futures::channel::mpsc::UnboundedSender;
use iced::border::rounded;
use iced::font::Weight;
use iced::widget::{button, column, container, rich_text, row, scrollable, span, text};
use iced::{Alignment, Element, Font, Length, Subscription, Task, Theme, never};
use indexmap::IndexMap;
use lucide_icons::Icon;
use std::hash::Hash;
use std::net::{IpAddr, Ipv4Addr};

use crate::executor::CustomExecutor;
use crate::{InterfaceMessage, ServerEvent};

pub fn run(
    gui_tx: UnboundedSender<InterfaceMessage>,
    server_rx: flume::Receiver<ServerEvent>,
) -> iced::Result {
    iced::application(move || State::new(gui_tx.clone()), update, view_2)
        .fonts([lucide_icons::LUCIDE_FONT_BYTES])
        .title("IPCV Server GUI")
        .subscription(move |state| subscription(state, server_rx.clone()))
        .executor::<CustomExecutor>()
        .exit_on_close_request(false)
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
    gui_tx: UnboundedSender<InterfaceMessage>,
}

impl State {
    fn new(gui_tx: UnboundedSender<InterfaceMessage>) -> Self {
        Self {
            server_status: Default::default(),
            // clients: [
            //     (
            //         IpAddr::V4(Ipv4Addr::new(255, 0, 1, 2)),
            //         ClientState {
            //             host: "ciaoooo".to_owned(),
            //             last_frame: None,
            //             frame_number: 8,
            //             waiting: true,
            //         },
            //     ),
            //     (
            //         IpAddr::V4(Ipv4Addr::new(25, 0, 1, 2)),
            //         ClientState {
            //             host: "ciaooodddo".to_owned(),
            //             last_frame: None,
            //             frame_number: 69,
            //             waiting: true,
            //         },
            //     ),
            //     (
            //         IpAddr::V4(Ipv4Addr::new(5, 0, 1, 2)),
            //         ClientState {
            //             host: "ciaooodsdso".to_owned(),
            //             last_frame: None,
            //             frame_number: 81,
            //             waiting: true,
            //         },
            //     ),
            //     (
            //         IpAddr::V4(Ipv4Addr::new(25, 2, 1, 2)),
            //         ClientState {
            //             host: "ciaooodddo".to_owned(),
            //             last_frame: None,
            //             frame_number: 69,
            //             waiting: true,
            //         },
            //     ),
            //     (
            //         IpAddr::V4(Ipv4Addr::new(5, 4, 1, 2)),
            //         ClientState {
            //             host: "ciaooodsdso".to_owned(),
            //             last_frame: None,
            //             frame_number: 81,
            //             waiting: true,
            //         },
            //     ),
            // ]
            // .into(),
            clients: Default::default(),
            gui_tx,
        }
    }
}

#[derive(Debug, Clone)]
enum Message {
    Server(ServerEvent),
    Interface(InterfaceMessage),
}

fn update(state: &mut State, message: Message) -> Task<Message> {
    match message {
        Message::Server(event) => match event {
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
            ServerEvent::ClientDisconnected { address } => {
                state.clients.shift_remove(&address);
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
        Message::Interface(cmd) => {
            if let InterfaceMessage::Move(from, to) = cmd {
                state.clients.move_index(from, to);
            }

            let _ = state.gui_tx.unbounded_send(cmd);
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

        {
            let row = if client.waiting {
                row![
                    button(row![Icon::CircleDashedCheck.widget(), "Accept"].spacing(4))
                        .style(iced::widget::button::success)
                        .on_press(InterfaceMessage::AcceptClient(address)),
                    button(row![Icon::Unplug.widget(), "Deny"].spacing(4))
                        .style(iced::widget::button::danger)
                        .on_press(InterfaceMessage::DisconnectClient(address)),
                ]
            } else {
                row![
                    button(row![Icon::ExternalLink.widget(), "Open folder"].spacing(4))
                        .on_press(InterfaceMessage::OpenFolder(address)),
                    button(row![Icon::Unplug.widget(), "Disconnect"].spacing(4))
                        .style(iced::widget::button::danger)
                        .on_press(InterfaceMessage::DisconnectClient(address)),
                ]
            }
            .push(
                button(Icon::ArrowLeft.widget())
                    .style(iced::widget::button::background)
                    .on_press_maybe((index > 0).then(|| InterfaceMessage::Move(index, index - 1))),
            )
            .push(
                button(Icon::ArrowRight.widget())
                    .style(iced::widget::button::background)
                    .on_press_maybe(
                        (index + 1 < state.clients.len())
                            .then(|| InterfaceMessage::Move(index, index + 1)),
                    ),
            )
            .spacing(8);

            client_col = client_col.push(row);
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

fn subscription(_state: &State, server_rx: flume::Receiver<ServerEvent>) -> Subscription<Message> {
    struct Tmp(flume::Receiver<ServerEvent>);

    impl Hash for Tmp {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            0.hash(state);
        }
    }

    Subscription::batch([
        iced::window::close_requests()
            .map(|_| Message::Interface(InterfaceMessage::Shutdown { force: false })),
        Subscription::run_with(Tmp(server_rx), |server_rx| {
            server_rx.0.clone().into_stream().map(Message::Server)
        }),
    ])
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
