import termios
import tty
from typing import TYPE_CHECKING, Any

# if TYPE_CHECKING:
#     from _typeshed import FileDescriptorLike


def find_key[K, V](dictionary: dict[K, V], target: V) -> K | None:
    for key, value in dictionary.items():
        if value == target:
            return key

    return None


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
