use futures::StreamExt;
use futures::channel::mpsc::UnboundedSender;
use iced::border::rounded;
use iced::font::Weight;
use iced::keyboard::{Key, Modifiers};
use iced::theme::palette::Seed;
use iced::widget::image::Allocation;
use iced::widget::scrollable::Scrollbar;
use iced::widget::{
    Scrollable, button, column, container, responsive, rich_text, row, rule, scrollable, space,
    span, stack, svg, text, tooltip,
};
use iced::{
    Alignment, Border, Font, Length, Shadow, Subscription, Task, Theme, Widget, color, never,
    padding,
};
use indexmap::IndexMap;
use jiff::Timestamp;
use lucide_icons::Icon;
use rust_fontconfig::GenericFamily;
use std::hash::Hash;
use std::net::{IpAddr, Ipv4Addr};

use crate::executor::CustomExecutor;
use crate::{InterfaceMessage, ServerEvent};

const HYDRA_SVG: &[u8] = include_bytes!("../../../hydra.svg");

pub fn run(
    gui_tx: UnboundedSender<InterfaceMessage>,
    server_rx: flume::Receiver<ServerEvent>,
) -> iced::Result {
    // let mut config = fontconfig::FontConfig::default();
    // let font = config.find("sans".to_owned(), None).unwrap();
    // let monospace = config.find("monospace".to_owned(), None).unwrap();
    // println!("{} - {}", font.name, monospace.name);
    // let mut pattern = fontconfig::OwnedPattern::new();
    // // pattern.add(
    // //     &fontconfig::pattern::properties::FC_FAMILY,
    // //     "sans-serif".to_owned(),
    // // );
    // // pattern.add(&fontconfig::pattern::properties::FC_VARIABLE, false);
    // let pattern = pattern.font_match(&mut config);
    // println!("{:?}", pattern.name());

    use rust_fontconfig::{FcFontCache, FcPattern};

    let cache = FcFontCache::build();
    // println!("{:#?}", cache.list());

    // Query a font by name
    let mut trace = Vec::new();
    let results = cache.query(
        &FcPattern {
            family: Some(GenericFamily::SansSerif.as_css().to_owned()),
            ..Default::default()
        },
        &mut trace,
    );

    for trace in trace {
        if trace.path.contains("Outfit") {
            println!("{trace:#?}");
        }
    }

    println!("{results:#?}");

    if let Some(results) = results {
        println!("{:#?}", cache.get_metadata_by_id(&results.id));
        println!("{:#?}", cache.get_font_by_id(&results.id));
    }

    iced::application(
        move || (State::new(gui_tx.clone()), Task::none()),
        State::update,
        State::view,
    )
    .font(Font::with_family("sans-serif"))
    .fonts([lucide_icons::LUCIDE_FONT_BYTES])
    .title("hydra")
    .window(iced::window::Settings {
        exit_on_close_request: false,
        platform_specific: iced::window::settings::PlatformSpecific {
            application_id: "hydra".to_owned(),
            ..Default::default()
        },
        ..Default::default()
    })
    .subscription(move |state| state.subscription(server_rx.clone()))
    .theme(State::theme)
    .executor::<CustomExecutor>()
    .run()
}

struct ClientState {
    host: String,
    last_frame: Option<Allocation>,
    waiting: bool,
}

struct State {
    log: Vec<(Timestamp, String)>,
    clients: IndexMap<IpAddr, ClientState>,
    denied: IndexMap<IpAddr, String>,
    sidebar: bool,
    gui_tx: UnboundedSender<InterfaceMessage>,
    theme: Theme,
    font: Font,
}

fn keybind_desc<'a, M: 'a>(desc: &'a str, key: &'a str) -> impl Widget<M> + 'a {
    row![desc, keybind(key)]
        .spacing(8)
        .align_y(Alignment::Center)
}

fn keybind<M>(text: &str) -> impl Widget<M> {
    container(
        iced::widget::text(text).font(Font::with_family("Fira Code").weight(Weight::Semibold)),
    )
    .padding([2, 6])
    .style(|theme: &Theme| {
        let palette = theme.palette();

        container::Style {
            background: Some(palette.background.weak.color.into()),
            text_color: Some(palette.background.weak.text),
            border: Border {
                width: 1.0,
                radius: 6.into(),
                color: palette.background.neutral.color,
            },
            ..Default::default()
        }
    })
}

impl State {
    fn log(&mut self, text: String) {
        self.log.push((Timestamp::now(), text))
    }

    fn new(gui_tx: UnboundedSender<InterfaceMessage>) -> Self {
        Self {
            log: Default::default(),
            clients: [
                // (
                //     IpAddr::V4(Ipv4Addr::new(255, 0, 1, 2)),
                //     ClientState {
                //         host: "ciaoooo".to_owned(),
                //         last_frame: None,
                //         waiting: true,
                //     },
                // ),
                // (
                //     IpAddr::V4(Ipv4Addr::new(25, 0, 1, 2)),
                //     ClientState {
                //         host: "ciaooodddo".to_owned(),
                //         last_frame: None,
                //         waiting: true,
                //     },
                // ),
                // (
                //     IpAddr::V4(Ipv4Addr::new(5, 0, 1, 2)),
                //     ClientState {
                //         host: "ciaooodsdso".to_owned(),
                //         last_frame: None,
                //         waiting: true,
                //     },
                // ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 2, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        waiting: true,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 4, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        waiting: true,
                    },
                ),
            ]
            .into(),
            // clients: Default::default(),
            denied: IndexMap::default(),
            sidebar: false,
            gui_tx,
            theme: Theme::custom(
                "Darker",
                Seed {
                    background: color!(0x141318),
                    text: color!(0xF0F0F0),
                    primary: color!(0x5865F2),
                    // success: color!(0x12664f),
                    success: color!(0x7FFF7F),
                    warning: color!(0xffc14e),
                    danger: color!(0xc3423f),
                },
            ),
            font: Font::with_family("Outfit"),
        }
    }

    fn theme(&self) -> Theme {
        self.theme.clone()
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Server(event) => match event {
                ServerEvent::Stopped => {
                    return iced::exit();
                }
                ServerEvent::ClientConnected { address, host } => {
                    self.log(format!("Connected to {} ({})", host, address));
                    self.clients.insert(
                        address,
                        ClientState {
                            host,
                            last_frame: None,
                            waiting: true,
                        },
                    );
                }
                ServerEvent::ClientAccepted { address } => {
                    if let Some(client) = self.clients.get_mut(&address) {
                        client.waiting = false;
                    }
                }
                ServerEvent::ClientDisconnected { address } => {
                    if let Some(client) = self.clients.get_mut(&address) {
                        client.waiting = true;
                    }

                    self.log(format!("Client {} disconnected", address));
                }
                ServerEvent::ClientDenied { address } => {
                    if let Some(_client) = self.clients.shift_remove(&address) {
                        self.log(format!("Client {} removed", address));
                    }
                }
                ServerEvent::FrameReceived {
                    address,
                    width,
                    height,
                    data,
                } => {
                    let handle = iced::widget::image::Handle::from_rgba(width, height, data);

                    return iced::widget::image::allocate(handle)
                        .map(move |x| Message::ThumbnailAllocated(address, x));
                }
            },
            Message::Interface(cmd) => {
                if let InterfaceMessage::Move(from, to) = cmd {
                    self.clients.move_index(from, to);
                }

                if let InterfaceMessage::DenyClient(address) = cmd
                    && let Some(client) = self.clients.get(&address)
                {
                    self.denied.insert(address, client.host.clone());
                }

                if let InterfaceMessage::AllowClient(address) = cmd {
                    self.denied.shift_remove(&address);
                }

                let _ = self.gui_tx.unbounded_send(cmd);
            }
            Message::ThumbnailAllocated(address, allocation) => {
                if let Some(client) = self.clients.get_mut(&address) {
                    client.last_frame = allocation.ok();
                }
            }
            Message::ToggleSidebar => self.sidebar = !self.sidebar,
        }

        Task::none()
    }

    fn view(&self) -> impl Widget<Message> {
        let clients = row(self
            .clients
            .iter()
            .enumerate()
            .map(|(index, (&address, client))| {
                view_client(
                    index,
                    self.clients.len(),
                    address,
                    client,
                    &self.theme,
                    &self.font,
                )
            }))
        .spacing(16)
        .padding(8)
        .wrap()
        .vertical_spacing(16)
        .align_x(Alignment::Center);

        let clients = Scrollable::with_direction(
            container(clients)
                .width(Length::Fill)
                .align_x(Alignment::Center),
            scrollable::Direction::Vertical(Scrollbar::new().spacing(0).padding(8)),
        )
        .height(Length::Fill);

        let mut content = column![container(clients).boxed()];

        if !self.denied.is_empty() {
            const DENIED_ROW_HEIGHT: f32 = 32.0;
            const DENIED_SPACING: f32 = 8.0;
            const DENIED_VISIBLE: usize = 2;
            const DENIED_ITEM_WIDTH: f32 = 460.0;
            const DENIED_COLUMN_SPACING: f32 = 24.0;
            const DENIED_MAX_COLUMNS: usize = 3;

            let denied_grid = responsive(move |size| {
                let columns = (((size.width + DENIED_COLUMN_SPACING)
                    / (DENIED_ITEM_WIDTH + DENIED_COLUMN_SPACING))
                    as usize)
                    .clamp(1, DENIED_MAX_COLUMNS)
                    .min(self.denied.len());

                let entries: Vec<_> = self.denied.iter().collect();
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
                                    .on_press(InterfaceMessage::AllowClient(address).into()),
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
                    Scrollable::with_direction(
                        lines,
                        scrollable::Direction::Vertical(Scrollbar::new().spacing(8)),
                    )
                    .height(height),
                )
                .center_x(Length::Fill)
            })
            .height(Length::Shrink);

            let denied_list = column![text("Denied clients").size(20), denied_grid].spacing(8);

            content = content.push(denied_list.boxed());
        }

        let header = row![
            svg(iced::widget::svg::Handle::from_memory(HYDRA_SVG))
                .width(24)
                .height(24),
            text("hydra")
                .font(self.font.weight(Weight::Semibold))
                .size(20),
            space().width(Length::Fill),
            tooltip(
                button(
                    if self.sidebar {
                        Icon::PanelRightClose
                    } else {
                        Icon::PanelRight
                    }
                    .widget()
                    .size(20)
                )
                .padding(padding::all(0).right(4))
                .style(iced::widget::button::text)
                .on_press(Message::ToggleSidebar),
                container(keybind_desc(
                    if self.sidebar {
                        "Close sidebar"
                    } else {
                        "Open sidebar"
                    },
                    "CTRL+SHIFT+S"
                ))
                .padding(8)
                .style(container::bordered_box),
                tooltip::Position::Bottom,
            ),
        ]
        .align_y(Alignment::Center)
        .spacing(4)
        .padding(8);

        let mut content = row![column![
            header,
            rule::horizontal(1),
            container(content)
                .center_x(Length::Fill)
                .center_y(Length::Fill)
        ]];

        if self.sidebar {
            content = content.push(rule::vertical(2).boxed()).push(
                container(
                    column(self.log.iter().map(|(time, x)| {
                        column![
                            text(time.strftime("%H:%M:%S").to_string())
                                .size(12)
                                .font(Font::with_family("Fira Code").weight(Weight::Medium))
                                .color(color!(0xFFFFFF, 0.65)),
                            text(x)
                        ]
                    }))
                    .spacing(8)
                    .width(320),
                )
                .padding(8)
                .boxed(),
            );
        }

        content
    }

    fn subscription(&self, server_rx: flume::Receiver<ServerEvent>) -> Subscription<Message> {
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
            iced::keyboard::listen().filter_map(|event| match event {
                iced::keyboard::Event::KeyPressed { key, modifiers, .. }
                    if key == Key::Character("s".into())
                        && modifiers == Modifiers::SHIFT | Modifiers::CTRL =>
                {
                    Some(Message::ToggleSidebar)
                }
                _ => None,
            }),
        ])
    }
}

#[derive(Debug, Clone)]
enum Message {
    Server(ServerEvent),
    Interface(InterfaceMessage),
    ThumbnailAllocated(IpAddr, Result<Allocation, iced::widget::image::Error>),
    ToggleSidebar,
}

impl From<InterfaceMessage> for Message {
    fn from(value: InterfaceMessage) -> Self {
        Self::Interface(value)
    }
}

fn view_client<'a>(
    index: usize,
    max: usize,
    address: IpAddr,
    client: &'a ClientState,
    theme: &Theme,
    font: &Font,
) -> impl Widget<Message> + 'a {
    let mut client_col = column![
        stack![
            container(if client.waiting {
                column![
                    Icon::RotateCwFadingClock.widget().size(96),
                    text("Waiting...").size(16)
                ]
                .align_x(Alignment::Center)
                .boxed()
            } else {
                if let Some(frame) = &client.last_frame {
                    iced::widget::image(frame.handle()).border_radius(8).boxed()
                } else {
                    column![
                        Icon::ImageOff.widget().size(96),
                        text("No Preview").size(16)
                    ]
                    .align_x(Alignment::Center)
                    .boxed()
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
            if client.waiting {
                space().boxed()
            } else {
                container(
                    button(Icon::RotateCwSquare.widget())
                        .style(|a, b| {
                            let mut b = iced::widget::button::background(a, b);
                            b.border.radius = 8.0.into();
                            b
                        })
                        .on_press(InterfaceMessage::Rotate(address).into()),
                )
                .padding(8)
                .boxed()
            },
        ]
        .boxed()
    ]
    .spacing(8)
    .align_x(Alignment::Center);

    client_col = client_col.push(
        container(
            column![
                rich_text![
                    span("Host: "),
                    span(&client.host).font(font.weight(Weight::Bold))
                ]
                .size(16)
                .on_link_click(never),
                rich_text![
                    span("Address: "),
                    span(address.to_string()).font(font.weight(Weight::Bold)),
                    span(":5009")
                        .font(font.weight(Weight::Bold))
                        .color(theme.palette().primary.strong.color)
                ]
                .size(14)
                .on_link_click(never),
            ]
            .spacing(4),
        )
        .align_left(360)
        .boxed(),
    );

    {
        let left = button(Icon::ArrowLeft.widget())
            .style(iced::widget::button::background)
            .on_press_maybe((index > 0).then(|| InterfaceMessage::Move(index, index - 1).into()));
        let right = button(Icon::ArrowRight.widget())
            .style(iced::widget::button::background)
            .on_press_maybe(
                (index + 1 < max).then(|| InterfaceMessage::Move(index, index + 1).into()),
            );

        let row = if client.waiting {
            row![
                button(row![Icon::CircleDashedCheck.widget(), "Accept"].spacing(4))
                    .style(iced::widget::button::success)
                    .on_press(InterfaceMessage::AcceptClient(address).into()),
                button(row![Icon::Unplug.widget(), "Deny"].spacing(4))
                    .style(iced::widget::button::danger)
                    .on_press(InterfaceMessage::DenyClient(address).into()),
                left,
                right
            ]
        } else {
            row![
                button(row![Icon::ExternalLink.widget(), "Open folder"].spacing(4))
                    .on_press(InterfaceMessage::OpenFolder(address).into()),
                button(row![Icon::Unplug.widget(), "Disconnect"].spacing(4))
                    .style(iced::widget::button::danger)
                    .on_press(InterfaceMessage::DisconnectClient(address).into()),
                left,
                right
            ]
        }
        .spacing(8);

        client_col = client_col.push(row.boxed());
    }

    container(client_col)
        .style(|theme: &Theme| {
            container::Style::default()
                .background(theme.palette().background.weakest.color)
                .border(
                    rounded(16), // .color(theme.palette().background.neutral.color)
                                 // .width(2.0),
                )
                .shadow(Shadow {
                    color: color!(0, 0.25),
                    blur_radius: 8.0,
                    ..Default::default()
                })
        })
        .padding(8)
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
