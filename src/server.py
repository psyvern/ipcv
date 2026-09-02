import argparse
import datetime
import select
import shutil
import socket
import struct
import sys
import termios
import threading
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any

import cv2
import numpy
from colored import Fore, Style, stylize

import shared

MCAST_GRP = "224.1.1.1"
IS_ALL_GROUPS = True


@dataclass
class ClientInfo:
    host: str
    address: Any
    port: int
    width: int
    height: int
    thread: threading.Thread


@dataclass
class Settings:
    port: int
    "Communication port (must be the same in clients)"
    update_delay: float
    "Delay between updates from clients (in seconds)"
    debug: bool
    "Log additional data"
    output: str
    "Output directory"


def parse_args() -> Settings:
    parser = argparse.ArgumentParser("ipcv-server")
    parser.add_argument(
        "--port",
        "-p",
        default=5007,
        help="Communication port (must be the same in clients). Defaults to `5007`",
        type=int,
    )
    parser.add_argument(
        "--update-delay",
        "-u",
        default=5,
        help="Delay between updates from clients (in seconds). Defaults to `5`",
        type=float,
    )
    parser.add_argument(
        "--debug", "-d", help="Log additional data", action="store_true"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="output",
        help="Output directory. Defaults to `./output`",
        type=str,
    )

    args = parser.parse_args()
    return Settings(
        port=args.port,
        update_delay=args.update_delay,
        debug=args.debug,
        output=args.output,
    )


def print_hosts(clients: list[ClientInfo]):
    length = max([len(x.host) for x in clients] + [len("host")])
    separator = f"+-----------------+-{'-' * length}-+"

    print()
    print(separator)
    print(f"| address         | {'host'.ljust(length)} |")
    print(separator)
    for info in clients:
        print(
            f"| {stylize(info.address.ljust(15), Fore.light_green + Style.bold)} | {stylize(info.host.ljust(length), Fore.light_blue + Style.bold)} |"
        )
    print(separator)
    print()


def read_stream(info: ClientInfo, settings: Settings):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    for _ in range(5):
        try:
            sock.connect((info.address, info.port))
            break
        except ConnectionRefusedError:
            sleep(3)

    counter = 0

    data = b""
    payload_size = 0
    size = info.width * info.height * 3

    directory = Path(settings.output).joinpath(info.address)
    shutil.rmtree(directory)
    directory.mkdir(parents=True)

    log = directory.joinpath("output.log").open("w")

    while True:
        while len(data) < payload_size:
            packet = sock.recv(4096)
            if not packet:
                break
            data += packet
        packed_msg_size = data[:payload_size]
        data = data[payload_size:]

        while len(data) < size:
            data += sock.recv(4096)
        frame_data = data[:size]
        data = data[size:]
        frame = numpy.frombuffer(frame_data, numpy.uint8).reshape(
            info.height, info.width, 3
        )

        cv2.imwrite(directory.joinpath(f"{counter:05}.png"), frame)
        counter += 1

        log.write(f"[{datetime.datetime.now()}] ciaooo {counter:05}\n")

        log.flush()


def loop_iteration(
    settings: Settings, clients: list[ClientInfo], data_socket: socket.socket
) -> bool | None:
    inputs, _, _ = select.select([sys.stdin, data_socket.fileno()], [], [])
    for input in inputs:
        if input == sys.stdin:
            match sys.stdin.read(1):
                case "h":
                    print_hosts(clients)
                case "q":
                    return False
                case "Q":
                    return True
                case x if shared.is_closing_character(x):
                    return False
        elif input == data_socket.fileno():
            data, (address, _) = data_socket.recvfrom(4096)
            info = next((x for x in clients if x.address == address), None)

            # print(bytes)
            # print(f"{address}:{port}")

            if data.startswith(b"open "):
                data = data[5:].decode()
                width, height, host = data.split(" ", 2)

                if info is None:
                    info = ClientInfo(
                        host, address, settings.port, int(width), int(height), None
                    )
                    clients.append(info)
                    print(
                        f"Added address {stylize(address, Fore.light_green + Style.bold)} as host {stylize(host, Fore.light_blue + Style.bold)}"
                    )

                    data_socket.sendto(
                        shared.to_bytes(shared.Start(settings.update_delay)),
                        (address, 5007),
                    )
                    thread = threading.Thread(
                        target=read_stream, args=(info, settings), daemon=True
                    )
                    info.thread = thread
                    thread.start()
                else:
                    print(
                        f"Address `{info.address}` already bound to host `{info.host}`"
                    )
            elif data == b"close":
                if info is None:
                    print(f"Address `{address}` not bound to a host")
                else:
                    clients.remove(info)
                    print(
                        f"Removed address `{info.address}` from host table (was host `{info.host}`)"
                    )
            elif data.startswith(b"frame "):
                if info is not None:
                    frame = data[6:]
                    total = info.width * info.height * 3

                    while len(frame) < total - 16384:
                        frame += data_socket.recv(16384)
                    while len(frame) < total:
                        frame += data_socket.recv(total - len(frame))

                    frame = numpy.frombuffer(frame, numpy.uint8).reshape(
                        info.height, info.width, 3
                    )

                    cv2.imwrite("test.png", frame)
                    # cv2.imshow(
                    #     "Frame",
                    #     ,
                    # )
                    # cv2.waitKey(1)
                    print(f"frame data from `{info.host}`")
                else:
                    print("frame data from unknown host")
            else:
                if info is not None:
                    print(f"data from `{info.host}`: {data}")
                else:
                    print(f"data from unknown host: {data}")


def main():
    settings = parse_args()
    print(settings)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if IS_ALL_GROUPS:
        # on this port, receives ALL multicast groups
        sock.bind(("", settings.port))
    else:
        # on this port, listen ONLY to MCAST_GRP
        sock.bind((MCAST_GRP, settings.port))

    mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    clients: list[ClientInfo] = []

    stdin = sys.stdin.fileno()

    term_mode = shared.set_term_mode(sys.stdin)

    while True:
        force = loop_iteration(settings, clients, sock)
        if force is not None:
            msg = f"quit {force}".encode()
            for info in clients:
                print(f"Closing connection to `{info.host}`")
                sock.sendto(msg, (info.address, info.port))

            termios.tcsetattr(stdin, termios.TCSAFLUSH, term_mode)
            break


main()
