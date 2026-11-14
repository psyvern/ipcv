use futures::StreamExt;
use futures::channel::mpsc::UnboundedSender;
use iced::border::rounded;
use iced::font::Weight;
use iced::keyboard::key::Named;
use iced::keyboard::{Key, Modifiers};
use iced::theme::palette::Seed;
use iced::widget::image::Allocation;
use iced::widget::scrollable::Scrollbar;
use iced::widget::text::Ellipsis;
use iced::widget::{
    Scrollable, button, center, column, container, pane_grid, responsive, rich_text, row, rule,
    scrollable, space, span, stack, svg, text, tooltip,
};
use iced::{
    Alignment, Border, Color, Degrees, Font, Length, Rotation, Shadow, Subscription, Task, Theme,
    Vector, Widget, color, never, padding,
};
use indexmap::IndexMap;
use jiff::Timestamp;
use library::LogLevel;
use library::threads::FFmpegProgress;
use lucide_icons::Icon;
use std::ffi::CString;
use std::hash::Hash;
use std::net::{IpAddr, Ipv4Addr};

use crate::executor::CustomExecutor;
use crate::{InterfaceMessage, ServerClientEvent, ServerEvent};

enum Pane {
    Main,
    Sidebar,
}

fn get_fonts() -> Option<(String, String)> {
    cfg_select! {
        unix => {
            use fontconfig::{Fontconfig, Pattern};
            let fc = Fontconfig::new()?;

            let mut pattern = Pattern::new(&fc).ok()?;
            pattern
                .add_string(fontconfig::FC_FAMILY, &CString::new("sans").ok()?)
                .ok()?;
            let pattern = pattern.font_match().ok()?;
            let sans = pattern.get_string(fontconfig::FC_FAMILY).ok()?.to_owned();

            let mut pattern = Pattern::new(&fc).ok()?;
            pattern
                .add_string(fontconfig::FC_FAMILY, &CString::new("monospace").ok()?)
                .ok()?;
            let pattern = pattern.font_match().ok()?;
            let monospace = pattern.get_string(fontconfig::FC_FAMILY).ok()?.to_owned();

            Some((sans, monospace))
        }
        _ => None
    }
}

pub fn run(
    gui_tx: UnboundedSender<InterfaceMessage>,
    server_rx: flume::Receiver<ServerEvent>,
) -> iced::Result {
    let (sans, monospace) = get_fonts().unzip();

    let font = match &sans {
        Some(x) => Font::with_family(x.as_str()),
        None => Font::DEFAULT,
    };

    iced::application(
        move || {
            (
                State::new(gui_tx.clone(), sans.clone(), monospace.clone()),
                Task::none(),
            )
        },
        State::update,
        State::view,
    )
    .font(font)
    .fonts([lucide_icons::LUCIDE_FONT_BYTES])
    .title("hydra")
    .window(iced::window::Settings {
        exit_on_close_request: false,
        icon: iced::window::icon::from_file_data(include_bytes!("../../assets/logo.png"), None)
            .ok(),
        platform_specific: iced::window::settings::PlatformSpecific {
            #[cfg(target_family = "unix")]
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
    progress: Option<FFmpegProgress>,
    rotation: u8,
}

struct Style {
    theme: Theme,
    sans: Font,
    monospace: Font,
}

#[derive(Debug)]
enum ServerLog {
    Connected(String),
    Other(String),
}

struct State {
    log: Vec<(Timestamp, LogLevel, Option<IpAddr>, ServerLog)>,
    clients: IndexMap<IpAddr, ClientState>,
    selected_client: Option<IpAddr>,
    selected_level: Option<LogLevel>,
    denied: IndexMap<IpAddr, String>,
    sidebar: bool,
    automatic: bool,
    gui_tx: UnboundedSender<InterfaceMessage>,
    style: Style,
    panes: pane_grid::State<Pane>,
}

fn keybind<M>(key: &str, modifiers: Modifiers, font: Font) -> impl Widget<M> {
    container(
        row![
            modifiers.control().then(|| Icon::ChevronUp.widget()),
            modifiers.shift().then(|| Icon::ArrowBigUpDash.widget()),
            modifiers.alt().then(|| Icon::Option.widget()),
            modifiers.logo().then(|| Icon::Command.widget()),
            iced::widget::text(key)
                .font(font.weight(Weight::Semibold))
                .line_height(1.0),
        ]
        .spacing(4),
    )
    .padding(6)
    .style(|theme: &Theme| {
        let palette = theme.palette();

        container::Style {
            background: Some(palette.background.weak.color.into()),
            text_color: Some(palette.background.weak.text),
            border: Border {
                width: 1.0,
                radius: 6.into(),
                color: palette.background.strong.color,
            },
            ..Default::default()
        }
    })
}

fn keybind_tooltip<'a, M: 'a>(
    content: impl Widget<M> + 'a,
    description: &'a str,
    key: &'a str,
    modifiers: Modifiers,
    font: Font,
) -> impl Widget<M> + 'a {
    tooltip(
        content,
        row![description, keybind(key, modifiers, font)]
            .spacing(8)
            .align_y(Alignment::Center)
            .padding(8),
        tooltip::Position::Bottom,
    )
    .gap(4)
    .style(|t: &Theme| container::Style {
        border: Border {
            width: 1.0,
            radius: 8.0.into(),
            color: t.palette().background.neutral.color,
        },
        shadow: Shadow {
            color: color!(0, 0.5),
            blur_radius: 8.0,
            ..Default::default()
        },
        ..container::bordered_box(t)
    })
}

impl State {
    fn log(&mut self, level: LogLevel, client: Option<IpAddr>, text: String) {
        self.log
            .push((Timestamp::now(), level, client, ServerLog::Other(text)))
    }

    fn new(
        gui_tx: UnboundedSender<InterfaceMessage>,
        font: Option<String>,
        monospace: Option<String>,
    ) -> Self {
        Self {
            automatic: false,
            log: Default::default(),
            selected_client: None,
            selected_level: None,
            clients: [
                (
                    IpAddr::V4(Ipv4Addr::new(255, 0, 1, 2)),
                    ClientState {
                        host: "ciaoooo".to_owned(),
                        last_frame: None,
                        waiting: true,
                        progress: None,
                        rotation: 0,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        waiting: true,
                        progress: None,
                        rotation: 0,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 0, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        waiting: true,
                        progress: None,
                        rotation: 0,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(25, 2, 1, 2)),
                    ClientState {
                        host: "ciaooodddo".to_owned(),
                        last_frame: None,
                        waiting: true,
                        progress: None,
                        rotation: 0,
                    },
                ),
                (
                    IpAddr::V4(Ipv4Addr::new(5, 4, 1, 2)),
                    ClientState {
                        host: "ciaooodsdso".to_owned(),
                        last_frame: None,
                        waiting: true,
                        progress: None,
                        rotation: 0,
                    },
                ),
            ]
            .into(),
            // clients: Default::default(),
            denied: IndexMap::default(),
            sidebar: false,
            gui_tx,
            style: Style {
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
                sans: match font {
                    Some(x) => Font::with_family(x.as_str()),
                    None => Font::DEFAULT,
                },
                monospace: match monospace {
                    Some(x) => Font::with_family(x.as_str()),
                    None => Font::MONOSPACE,
                },
            },
            panes: {
                let (mut state, pane) = pane_grid::State::new(Pane::Main);
                state.split(pane_grid::Axis::Vertical, pane, Pane::Sidebar);
                state
            },
        }
    }

    fn theme(&self) -> Theme {
        self.style.theme.clone()
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::PaneResized(event) => {
                self.panes.resize(event.split, event.ratio);
            }
            Message::Server(event) => match event {
                ServerEvent::Stopped => {
                    return iced::exit();
                }
                ServerEvent::Client(address, event) => match event {
                    ServerClientEvent::Connected { host } => {
                        self.log.push((
                            Timestamp::now(),
                            LogLevel::Info,
                            Some(address),
                            ServerLog::Connected(host.clone()),
                        ));
                        self.clients.insert(
                            address,
                            ClientState {
                                host,
                                last_frame: None,
                                waiting: true,
                                progress: None,
                                rotation: 0,
                            },
                        );
                    }
                    ServerClientEvent::Accepted => {
                        if let Some(client) = self.clients.get_mut(&address) {
                            client.waiting = false;
                        }
                    }
                    ServerClientEvent::Disconnected => {
                        if let Some(client) = self.clients.get_mut(&address) {
                            client.waiting = true;
                        }

                        self.log(LogLevel::Info, Some(address), "Disconnected".to_owned());
                    }
                    ServerClientEvent::Denied => {
                        if let Some(_client) = self.clients.shift_remove(&address) {
                            self.log(LogLevel::Info, Some(address), "Removed".to_owned());
                        }
                    }
                    ServerClientEvent::Frame {
                        width,
                        height,
                        data,
                    } => {
                        let handle = iced::widget::image::Handle::from_rgba(width, height, data);

                        return iced::widget::image::allocate(handle)
                            .map(move |x| Message::ThumbnailAllocated(address, x));
                    }
                    ServerClientEvent::Progress(progress) => {
                        if let Some(client) = self.clients.get_mut(&address) {
                            client.progress = Some(progress);
                        }
                    }
                    ServerClientEvent::Log(log) => {
                        self.log(log.level, Some(address), log.line);
                    }
                },
            },
            Message::Interface(cmd) => {
                match cmd {
                    InterfaceMessage::DenyClient(address)
                        if let Some(client) = self.clients.get(&address) =>
                    {
                        self.denied.insert(address, client.host.clone());
                    }
                    InterfaceMessage::AllowClient(address) => {
                        self.denied.shift_remove(&address);
                    }
                    _ => (),
                }

                let _ = self.gui_tx.unbounded_send(cmd);
            }
            Message::ThumbnailAllocated(address, allocation) => {
                if let Some(client) = self.clients.get_mut(&address) {
                    client.last_frame = allocation.ok();
                }
            }
            Message::ToggleSidebar => self.sidebar = !self.sidebar,
            Message::ToggleAutoAccept => self.automatic = !self.automatic,
            Message::SelectClient(address) => {
                self.selected_client = Some(address);
                self.sidebar = true;
            }
            Message::SelectLevel(level) => {
                self.selected_level = if self.selected_level == Some(level) {
                    None
                } else {
                    Some(level)
                };
            }
            Message::SelectNext => {
                self.selected_client = match self
                    .selected_client
                    .and_then(|x| self.clients.get_index_of(&x))
                {
                    None => self.clients.keys().next(),
                    Some(index) => {
                        if index + 1 == self.clients.len() {
                            self.clients.keys().next()
                        } else {
                            self.clients.keys().nth(index + 1)
                        }
                    }
                }
                .cloned()
            }
            Message::SelectPrevious => {
                self.selected_client = match self
                    .selected_client
                    .and_then(|x| self.clients.get_index_of(&x))
                {
                    None => self.clients.keys().last(),
                    Some(index) => {
                        if index == 0 {
                            self.clients.keys().last()
                        } else {
                            self.clients.keys().nth(index - 1)
                        }
                    }
                }
                .cloned()
            }
            Message::SwapNext => {
                if let Some(index) = self
                    .selected_client
                    .and_then(|x| self.clients.get_index_of(&x))
                {
                    let new = (index + 1) % self.clients.len();
                    self.clients.swap_indices(index, new);
                }
            }
            Message::SwapPrevious => {
                if let Some(index) = self
                    .selected_client
                    .and_then(|x| self.clients.get_index_of(&x))
                {
                    let new = if index == 0 {
                        self.clients.len() - 1
                    } else {
                        index - 1
                    };

                    self.clients.swap_indices(index, new);
                }
            }
            Message::Move(from, to) => {
                self.clients.move_index(from, to);
            }
            Message::Rotate(address, clockwise) => {
                if let Some(client) = self.clients.get_mut(&address) {
                    client.rotation = if clockwise {
                        if client.rotation == 0 {
                            3
                        } else {
                            client.rotation - 1
                        }
                    } else {
                        (client.rotation + 1) % 4
                    };
                }
            }
            Message::RotateCurrent(clockwise) => {
                if let Some(selected) = self.selected_client {
                    return self.update(Message::Rotate(selected, clockwise));
                }
            }
            Message::Deselect => self.selected_client = None,
            Message::CopyLog(index) => {
                if let Some(log) = self.log.get(index) {
                    println!("{log:#?}");
                }
            }
        }

        Task::none()
    }

    fn view(&self) -> impl Widget<Message> {
        let content = || {
            let clients =
                row(self
                    .clients
                    .iter()
                    .enumerate()
                    .map(|(index, (&address, client))| {
                        view_client(
                            index,
                            Some(address) == self.selected_client,
                            self.clients.len(),
                            address,
                            client,
                            &self.style,
                        )
                    }))
                .spacing(8)
                .padding([8, 0])
                .wrap()
                .vertical_spacing(8);

            let clients = container(
                scrollable(center(clients))
                    .spacing(8)
                    .padding(8)
                    .style(custom_scrollbar)
                    .height(Length::Fill),
            )
            .padding([0, 8]);

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
                svg(iced::widget::svg::Handle::from_memory(include_bytes!(
                    "../../assets/logo.svg"
                )))
                .width(24)
                .height(24),
                space().width(4),
                text("hydra")
                    .font(self.style.sans.weight(Weight::Semibold))
                    .size(20)
                    .line_height(1.2),
                space().width(Length::Fill),
                keybind_tooltip(
                    button(Icon::FolderOpen.widget().size(24))
                        .padding(8)
                        .style(iced::widget::button::text)
                        .on_press(Message::ToggleAutoAccept),
                    "Open directory",
                    "O",
                    Modifiers::CTRL | Modifiers::SHIFT,
                    self.style.monospace,
                ),
                keybind_tooltip(
                    button(
                        if self.automatic {
                            Icon::Zap
                        } else {
                            Icon::ZapOff
                        }
                        .widget()
                        .size(24)
                    )
                    .padding(8)
                    .style(iced::widget::button::text)
                    .on_press(Message::ToggleAutoAccept),
                    "Auto-accept clients",
                    "A",
                    Modifiers::CTRL | Modifiers::SHIFT,
                    self.style.monospace,
                ),
                keybind_tooltip(
                    button(
                        if self.sidebar {
                            Icon::PanelRightClose
                        } else {
                            Icon::PanelRight
                        }
                        .widget()
                        .size(24)
                    )
                    .padding(8)
                    .style(iced::widget::button::text)
                    .on_press(Message::ToggleSidebar),
                    if self.sidebar {
                        "Close sidebar"
                    } else {
                        "Open sidebar"
                    },
                    "S",
                    Modifiers::CTRL | Modifiers::SHIFT,
                    self.style.monospace,
                ),
            ]
            // .height(48)
            .align_y(Alignment::Center)
            .padding(padding::left(8));

            let mut content = row![column![
                header,
                rule::horizontal(2),
                container(content)
                    .center_x(Length::Fill)
                    .center_y(Length::Fill)
            ]];

            content
        };

        let sidebar = || {
            column![
                self.selected_client
                    .and_then(|x| self.clients.get(&x).map(|y| (x, y)))
                    .map(|(address, client)| {
                        column![
                            column![
                                text(address.to_string())
                                    .size(14)
                                    .line_height(1.0)
                                    .font(self.style.monospace.weight(Weight::Semibold))
                                    .color(
                                        self.style
                                            .theme
                                            .palette()
                                            .background
                                            .base
                                            .text
                                            .scale_alpha(0.75)
                                    ),
                                client
                                    .progress
                                    .as_ref()
                                    .map(|x| view_progress(x, &self.style).boxed())
                                    .unwrap_or("<none>".boxed())
                            ]
                            .padding(8),
                            rule::horizontal(2),
                        ]
                    }),
                // row([
                //     (Icon::BadgeQuestionMark, 0x66ccff),
                //     (Icon::Info, 0x99ffcc),
                //     (Icon::TriangleExclamationPoint, 0xffff99),
                //     (Icon::OctagonX, 0xff9999),
                // ]
                // .into_iter()
                // .map(|(icon, color)| {
                //     icon.widget().size(24).color(Color::from_packed_rgb8(color))
                // }))
                // .spacing(8),
                container(
                    scrollable(
                        column(
                            self.log
                                .iter()
                                .enumerate()
                                .filter(|(_, (_, _, x, _))| self
                                    .selected_client
                                    .is_none_or(|y| x == &Some(y)))
                                .filter(|(_, (_, x, _, _))| self
                                    .selected_level
                                    .is_none_or(|y| x == &y))
                                .map(|(index, (time, level, address, text))| {
                                    view_log(
                                        &self.style,
                                        index,
                                        *time,
                                        *level,
                                        address.filter(|_| self.selected_client.is_none()),
                                        text,
                                    )
                                })
                        )
                        .spacing(12)
                        .padding([8, 0])
                        .width(Length::Fill)
                    )
                    .spacing(8)
                    .padding(8)
                    .style(custom_scrollbar)
                )
                .padding([0, 8])
                .height(Length::Fill),
            ]
        };

        // content = content.push(rule::vertical(2).boxed());
        // content = content.push(sidebar);
        // }

        pane_grid(&self.panes, move |_, state, is_maximized| {
            pane_grid::Content::new(match state {
                Pane::Main => row![content(), rule::vertical(1)].boxed(),
                Pane::Sidebar => row![rule::vertical(1), sidebar()].boxed(),
            })
        })
        // .on_drag(Message::PaneDragged)
        .on_resize(10, Message::PaneResized)
        .style(|theme| pane_grid::Style {
            ..pane_grid::default(theme)
        })
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
                iced::keyboard::Event::KeyPressed { key, modifiers, .. } => {
                    Some(match (key.as_ref(), modifiers) {
                        (Key::Character("s"), modifiers)
                            if modifiers == Modifiers::SHIFT | Modifiers::CTRL =>
                        {
                            Message::ToggleSidebar
                        }
                        (Key::Character("r"), Modifiers::NONE) => Message::RotateCurrent(false),
                        (Key::Character("r"), Modifiers::SHIFT) => Message::RotateCurrent(true),
                        (Key::Named(Named::ArrowLeft), Modifiers::NONE) => Message::SelectPrevious,
                        (Key::Named(Named::ArrowRight), Modifiers::NONE) => Message::SelectNext,
                        (Key::Named(Named::ArrowLeft), Modifiers::SHIFT) => Message::SwapPrevious,
                        (Key::Named(Named::ArrowRight), Modifiers::SHIFT) => Message::SwapNext,
                        (Key::Named(Named::Escape), Modifiers::NONE) => Message::Deselect,
                        _ => return None,
                    })
                }
                _ => None,
            }),
        ])
    }
}

pub fn custom_scrollbar(theme: &Theme, status: scrollable::Status) -> scrollable::Style {
    let palette = theme.palette();

    let scrollbar = scrollable::Rail {
        background: Some(palette.background.weak.color.into()),
        border: rounded(100),
        scroller: scrollable::Scroller {
            background: palette.background.strongest.color.into(),
            border: rounded(100).width(2).color(palette.background.weak.color),
        },
    };

    let auto_scroll = scrollable::AutoScroll {
        background: palette.background.base.color.scale_alpha(0.9).into(),
        border: rounded(u32::MAX)
            .width(1)
            .color(palette.background.base.text.scale_alpha(0.8)),
        shadow: Shadow {
            color: Color::BLACK.scale_alpha(0.7),
            offset: Vector::ZERO,
            blur_radius: 2.0,
        },
        icon: palette.background.base.text.scale_alpha(0.8),
    };

    match status {
        scrollable::Status::Active { .. } => scrollable::Style {
            container: container::Style::default(),
            vertical_rail: scrollbar,
            horizontal_rail: scrollbar,
            gap: None,
            auto_scroll,
        },
        scrollable::Status::Hovered {
            is_horizontal_scrollbar_hovered,
            is_vertical_scrollbar_hovered,
            ..
        } => {
            let hovered_scrollbar = scrollable::Rail {
                scroller: scrollable::Scroller {
                    background: palette.primary.strong.color.into(),
                    ..scrollbar.scroller
                },
                ..scrollbar
            };

            scrollable::Style {
                container: container::Style::default(),
                vertical_rail: if is_vertical_scrollbar_hovered {
                    hovered_scrollbar
                } else {
                    scrollbar
                },
                horizontal_rail: if is_horizontal_scrollbar_hovered {
                    hovered_scrollbar
                } else {
                    scrollbar
                },
                gap: None,
                auto_scroll,
            }
        }
        scrollable::Status::Dragged {
            is_horizontal_scrollbar_dragged,
            is_vertical_scrollbar_dragged,
            ..
        } => {
            let dragged_scrollbar = scrollable::Rail {
                scroller: scrollable::Scroller {
                    background: palette.primary.base.color.into(),
                    ..scrollbar.scroller
                },
                ..scrollbar
            };

            scrollable::Style {
                container: container::Style::default(),
                vertical_rail: if is_vertical_scrollbar_dragged {
                    dragged_scrollbar
                } else {
                    scrollbar
                },
                horizontal_rail: if is_horizontal_scrollbar_dragged {
                    dragged_scrollbar
                } else {
                    scrollbar
                },
                gap: None,
                auto_scroll,
            }
        }
    }
}
#[derive(Debug, Clone)]
enum Message {
    Server(ServerEvent),
    Interface(InterfaceMessage),
    ThumbnailAllocated(IpAddr, Result<Allocation, iced::widget::image::Error>),
    Move(usize, usize),
    Rotate(IpAddr, bool),
    ToggleSidebar,
    ToggleAutoAccept,
    SelectClient(IpAddr),
    SelectLevel(LogLevel),
    SelectNext,
    SelectPrevious,
    SwapNext,
    SwapPrevious,
    RotateCurrent(bool),
    Deselect,
    PaneResized(pane_grid::ResizeEvent),
    CopyLog(usize),
}

impl From<InterfaceMessage> for Message {
    fn from(value: InterfaceMessage) -> Self {
        Self::Interface(value)
    }
}

fn view_client<'a>(
    index: usize,
    selected: bool,
    max: usize,
    address: IpAddr,
    client: &'a ClientState,
    style: &'a Style,
) -> impl Widget<Message> + 'a {
    fn floating_button(theme: &Theme, status: button::Status) -> button::Style {
        button::Style {
            border: rounded(4),
            ..iced::widget::button::background(theme, status)
        }
    }

    let mut client_col = column![
        stack![
            container(if client.waiting {
                column![
                    Icon::RotateCwFadingClock.widget().size(96),
                    text("Waiting...").size(16),
                ]
                .align_x(Alignment::Center)
                .boxed()
            } else {
                if let Some(frame) = &client.last_frame {
                    iced::widget::image(frame.handle())
                        .border_radius(8)
                        .rotation(Rotation::Solid(
                            Degrees(client.rotation as f32 * 90.0).into(),
                        ))
                        .boxed()
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
            container(
                column![
                    row![
                        (!client.waiting).then(|| keybind_tooltip(
                            button(Icon::RotateCwSquare.widget().size(22))
                                .style(floating_button)
                                .padding(4)
                                .on_press(Message::Rotate(address, false)),
                            "Rotate",
                            "R",
                            Modifiers::NONE,
                            style.monospace,
                        )),
                        space().width(Length::Fill),
                        keybind_tooltip(
                            button(Icon::ArrowLeft.widget().size(22))
                                .style(floating_button)
                                .padding(4)
                                .on_press_maybe(
                                    (index > 0).then(|| Message::Move(index, index - 1))
                                ),
                            "Move left",
                            "←",
                            Modifiers::SHIFT,
                            style.monospace,
                        ),
                        keybind_tooltip(
                            button(Icon::ArrowRight.widget().size(22))
                                .style(floating_button)
                                .padding(4)
                                .on_press_maybe(
                                    (index + 1 < max).then(|| Message::Move(index, index + 1)),
                                ),
                            "Move right",
                            "→",
                            Modifiers::SHIFT,
                            style.monospace
                        )
                    ]
                    .spacing(8),
                    space().height(Length::Fill),
                    if client.waiting {
                        row![
                            button(
                                row![Icon::CircleDashedCheck.widget(), "Accept"]
                                    .align_y(Alignment::Center)
                                    .spacing(4)
                            )
                            .style(iced::widget::button::success)
                            .on_press(InterfaceMessage::AcceptClient(address).into()),
                            button(
                                row![Icon::Unplug.widget(), "Deny"]
                                    .align_y(Alignment::Center)
                                    .spacing(4)
                            )
                            .style(iced::widget::button::danger)
                            .on_press(InterfaceMessage::DenyClient(address).into()),
                        ]
                    } else {
                        row![
                            space().width(Length::Fill),
                            keybind_tooltip(
                                button(Icon::ExternalLink.widget().size(22))
                                    .style(|t, s| {
                                        button::Style {
                                            border: rounded(4),
                                            shadow: Shadow {
                                                color: color!(0, 0.5),
                                                blur_radius: 8.0,
                                                ..Default::default()
                                            },
                                            ..button::primary(t, s)
                                        }
                                    })
                                    .padding(4)
                                    .on_press(InterfaceMessage::OpenFolder(address).into()),
                                "Open directory",
                                "E",
                                Modifiers::NONE,
                                style.monospace,
                            ),
                            keybind_tooltip(
                                button(Icon::Unplug.widget().size(22))
                                    .style(|t, s| {
                                        button::Style {
                                            border: rounded(4),
                                            shadow: Shadow {
                                                color: color!(0, 0.5),
                                                blur_radius: 8.0,
                                                ..Default::default()
                                            },
                                            ..button::danger(t, s)
                                        }
                                    })
                                    .padding(4)
                                    .on_press(InterfaceMessage::DisconnectClient(address).into()),
                                "Disconnect",
                                "X",
                                Modifiers::NONE,
                                style.monospace,
                            ),
                        ]
                    }
                    .spacing(8)
                ]
                .align_x(Alignment::Center)
            )
            .padding(8)
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
                    span(&client.host).font(style.sans.weight(Weight::Bold))
                ]
                .ellipsis(Ellipsis::End)
                .size(16)
                .on_link_click(never),
                rich_text![
                    span("Address: "),
                    span(address.to_string()).font(style.sans.weight(Weight::Bold)),
                    span(":5009")
                        .font(style.sans.weight(Weight::Bold))
                        .color(style.theme.palette().primary.strong.color)
                ]
                .size(14)
                .on_link_click(never),
            ]
            .spacing(4),
        )
        .align_left(360)
        .boxed(),
    );

    pub fn custom_style(selected: bool, theme: &Theme, status: button::Status) -> button::Style {
        let palette = theme.palette();

        let base = button::Style {
            background: Some(theme.palette().background.weakest.color.into()),
            text_color: palette.background.base.text,
            border: rounded(16)
                .color(theme.palette().primary.base.color)
                .width(if selected { 2 } else { 0 }),
            shadow: Shadow {
                color: color!(0, 0.25),
                blur_radius: 8.0,
                ..Default::default()
            },
            ..Default::default()
        };

        match status {
            button::Status::Hovered => button::Style { ..base },
            _ => base,
        }
    }

    button(client_col)
        .style(move |a, b| custom_style(selected, a, b))
        .padding(8)
        .on_press(Message::SelectClient(address))
}

fn view_progress<'a>(progress: &'a FFmpegProgress, style: &Style) -> impl Widget<Message> + 'a {
    let bold = style.monospace.weight(Weight::Bold);

    row![
        column!["Frame", "FPS", "Quality", "Bitrate", "Time", "Speed"].align_x(Alignment::End),
        column![
            text(progress.frame).font(bold),
            text(progress.fps).font(bold),
            text(progress.q).font(bold),
            text(&progress.bitrate).font(bold),
            text(format!("{:?}", progress.time)).font(bold),
            text(progress.speed).font(bold),
        ]
    ]
    .spacing(4)
}

fn view_log<'a>(
    style: &Style,
    index: usize,
    time: Timestamp,
    level: LogLevel,
    address: Option<IpAddr>,
    text: &'a ServerLog,
) -> impl Widget<Message> + 'a {
    let palette = style.theme.palette();

    let color = match level {
        LogLevel::Info => palette.success,
        LogLevel::Warning => palette.warning,
        LogLevel::Error => palette.danger,
    }
    .base
    .color;

    button(column![
        rich_text![
            span(time.strftime("%H:%M:%S").to_string())
                .color(style.theme.palette().background.base.text.scale_alpha(0.75)),
            span(address.map(|x| format!(" {x}")).unwrap_or_default())
                .color(style.theme.palette().primary.strong.color),
            " ",
            span(format!("{level:?}")).color(color).link(level)
        ]
        .size(14)
        .line_height(1.0)
        .font(style.monospace.weight(Weight::Semibold))
        .on_link_click(Message::SelectLevel),
        match text {
            ServerLog::Connected(host) => rich_text![
                "Connected as ",
                span(host).font(style.sans.weight(Weight::Bold))
            ]
            .on_link_click(never)
            .boxed(),
            ServerLog::Other(text) => text.boxed(),
        } // iced::widget::text(text).line_height(1.125),
    ])
    .padding(0)
    .style(button::text)
    .on_press(Message::CopyLog(index))
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
        iced::widget::text(char::from(self).to_string())
            .font(iced::Font::with_family("lucide"))
            .line_height(1.0)
    }
}
