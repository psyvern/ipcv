import termios
import tty
from dataclasses import dataclass
from typing import Any


@dataclass
class FrameInfo: ...


@dataclass
class Start:
    delay: float


@dataclass
class Quit:
    force: bool


@dataclass
class Heartbeat: ...


ServerMessage = Start | Quit | Heartbeat


def to_bytes(message: ServerMessage) -> bytes:
    match message:
        case Start(delay):
            return f"start {delay}".encode()
        case Quit(force):
            return f"quit {force}".encode()
        case Heartbeat():
            return b"heartbeat"


def set_term_mode(fd) -> list[Any]:
    old = termios.tcgetattr(fd)

    new = list(old)
    tty.cfmakeraw(new)
    # new[tty.IFLAG] |= termios.INLCR | termios.IGNCR | termios.ICRNL
    new[tty.OFLAG] |= termios.OPOST

    termios.tcsetattr(fd, termios.TCSAFLUSH, new)

    return old


def is_closing_character(character: str) -> bool:
    match character:
        case "\x03" | "\x04" | "\x1a" | "\x1b" | "\x1c":
            return True
        case _:
            return False
