use futures::StreamExt;
use futures::channel::mpsc::UnboundedSender;
use iced::border::rounded;
use iced::font::Weight;
use iced::widget::image::Allocation;
use iced::widget::{button, column, container, rich_text, row, scrollable, span, text, responsive};
use iced::{Alignment, Element, Font, Length, Subscription, Task, Theme, never};
use indexmap::IndexMap;
use lucide_icons::Icon;
use std::hash::Hash;
use std::net::IpAddr;

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
    last_frame: Option<Allocation>,
    frame_number: u64,
    waiting: bool,
}

struct State {
    server_status: String,
    clients: IndexMap<IpAddr, ClientState>,
    denied: IndexMap<IpAddr, String>,
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
            denied: IndexMap::default(),
            gui_tx,
        }
    }
}

#[derive(Debug, Clone)]
enum Message {
    Server(ServerEvent),
    Interface(InterfaceMessage),
    ThumbnailAllocated(IpAddr, Result<Allocation, iced::widget::image::Error>),
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
                if let Some(client) = state.clients.get_mut(&address) {
                    state.server_status = format!("Client {} disconnected", address);
                    client.waiting = true;
                }
            }
            ServerEvent::ClientDenied { address } => {
                if let Some(_client) = state.clients.shift_remove(&address) {
                    state.server_status = format!("Client {} removed", address);
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
                    client.frame_number = frame_number;
                }

                return iced::widget::image::allocate(handle)
                    .map(move |x| Message::ThumbnailAllocated(address, x));
            }
        },
        Message::Interface(cmd) => {
            if let InterfaceMessage::Move(from, to) = cmd {
                state.clients.move_index(from, to);
            }

            if let InterfaceMessage::DenyClient(address) = cmd
                && let Some(client) = state.clients.get(&address)
            {
                state.denied.insert(address, client.host.clone());
            }

            if let InterfaceMessage::AllowClient(address) = cmd {
                state.denied.shift_remove(&address);
            }

            let _ = state.gui_tx.unbounded_send(cmd);
        }
        Message::ThumbnailAllocated(address, allocation) => {
            if let Some(client) = state.clients.get_mut(&address) {
                client.last_frame = allocation.ok();
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
                if let Some(frame) = &client.last_frame {
                    Element::new(iced::widget::image(frame.handle()).border_radius(8))
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
                        .on_press(InterfaceMessage::DenyClient(address)),
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

    let mut content = column![header, scrollable_clients.height(Length::Fill)].spacing(40);

    if !state.denied.is_empty() {
        const DENIED_ROW_HEIGHT: f32 = 32.0;
        const DENIED_SPACING: f32 = 8.0;
        const DENIED_VISIBLE: usize = 2;
        const DENIED_ITEM_WIDTH: f32 = 460.0;
        const DENIED_COLUMN_SPACING: f32 = 24.0;
        const DENIED_MAX_COLUMNS: usize = 3;

        let denied_grid = responsive(move |size| {
            let columns = (((size.width + DENIED_COLUMN_SPACING)
                / (DENIED_ITEM_WIDTH + DENIED_COLUMN_SPACING)) as usize)
                .clamp(1, DENIED_MAX_COLUMNS)
                .min(state.denied.len());

            let entries: Vec<_> = state.denied.iter().collect();
            let mut lines = column![].spacing(DENIED_SPACING);

            for chunk in entries.chunks(columns) {
                let mut line = row![].spacing(DENIED_COLUMN_SPACING);

                for &(&address, host) in chunk {
                    line = line.push(
                        row![
                            text(host).width(200),
                            text(address.to_string()).width(150),
                            button("Allow")
                                .style(iced::widget::button::success)
                                .on_press(InterfaceMessage::AllowClient(address)),
                        ]
                        .spacing(16)
                        .width(DENIED_ITEM_WIDTH)
                        .height(DENIED_ROW_HEIGHT)
                        .align_y(Alignment::Center),
                    );
                }

                lines = lines.push(line);
            }

            let visible = entries.len().div_ceil(columns).min(DENIED_VISIBLE) as f32;
            let height = visible * DENIED_ROW_HEIGHT + (visible - 1.0) * DENIED_SPACING;

            container(
                iced::widget::Scrollable::with_direction(
                    lines,
                    scrollable::Direction::Vertical(scrollable::Scrollbar::new().spacing(8)),
                )
                .height(height),
            )
            .center_x(Length::Fill)
        })
        .height(Length::Shrink);

        let denied_list = column![text("Denied clients").size(20), denied_grid].spacing(8);

        content = content.push(denied_list);    
    }

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
