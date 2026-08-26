import select
import socket
import struct
import sys
import termios
from typing import Any

from colored import Fore, Style, stylize

import util

MCAST_GRP = "224.1.1.1"
PORT = 5007
IS_ALL_GROUPS = True


def print_hosts(hosts: dict[str, Any]):
    length = max(len(x) for x in list(hosts) + ["host"])
    separator = f"+-----------------+-{'-' * length}-+"

    print()
    print(separator)
    print(f"| address         | {'host'.ljust(length)} |")
    print(separator)
    for host, address in hosts.items():
        print(
            f"| {stylize(address.ljust(15), Fore.light_green + Style.bold)} | {stylize(host.ljust(length), Fore.light_blue + Style.bold)} |"
        )
    print(separator)
    print()


def loop_iteration(hosts: dict[str, Any], data_socket: socket.socket) -> bool | None:
    inputs, _, _ = select.select([sys.stdin, data_socket.fileno()], [], [])
    for input in inputs:
        if input == sys.stdin:
            match sys.stdin.read(1):
                case "h":
                    print_hosts(hosts)
                case "q":
                    return False
                case "Q":
                    return True
                case x if util.is_closing_character(x):
                    return False
        elif input == data_socket.fileno():
            bytes, (address, _port) = data_socket.recvfrom(1024)

            # print(bytes)
            # print(f"{address}:{port}")

            data = bytes.decode()

            if data.startswith("open "):
                host = data[5:]
                previous = hosts.get(host)

                if previous is None:
                    hosts[host] = address
                    print(
                        f"Added address {stylize(address, Fore.light_green + Style.bold)} as host {stylize(host, Fore.light_blue + Style.bold)}"
                    )

                    data_socket.sendto(f"start {2}".encode(), (address, PORT))
                else:
                    print(f"Host `{host}` already bound to address `{previous}`")
            elif data == "close":
                host = util.find_key(hosts, address)

                if host not in hosts:
                    print(f"Address `{address}` not bound to a host")
                else:
                    hosts.pop(host)
                    print(
                        f"Removed host `{host}` from host table (was address `{address}`)"
                    )
            else:
                host = util.find_key(hosts, address)
                if host is not None:
                    print(f"data from `{host}`: {data}")
                else:
                    print(f"data from unknown host: {data}")


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if IS_ALL_GROUPS:
        # on this port, receives ALL multicast groups
        sock.bind(("", PORT))
    else:
        # on this port, listen ONLY to MCAST_GRP
        sock.bind((MCAST_GRP, PORT))

    mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    hosts: dict[str, Any] = {}

    stdin = sys.stdin.fileno()

    term_mode = util.set_term_mode(sys.stdin)

    while True:
        force = loop_iteration(hosts, sock)
        if force is not None:
            msg = f"quit {force}".encode()
            for host, address in hosts.items():
                print(f"Closing connection to `{host}`")
                sock.sendto(msg, (address, PORT))

            termios.tcsetattr(stdin, termios.TCSAFLUSH, term_mode)
            break


main()
