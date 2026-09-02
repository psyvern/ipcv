import argparse
import select
import socket
import sys
import termios
import threading
import time
from dataclasses import dataclass
from typing import Any

import cv2
from colored import Fore, Style, stylize

import shared

MCAST_GRP = "224.1.1.1"
PORT = 5007


@dataclass
class Settings:
    port: int
    "Communication port (must be the same in the server)"
    update_delay: int
    "Delay between sending (in seconds)"
    debug: bool
    "Log additional data"


def parse_args() -> Settings:
    parser = argparse.ArgumentParser("ipcv-server")
    parser.add_argument(
        "--port",
        "-p",
        default=5007,
        help="Communication port (must be the same in the server). Defaults to `5007`",
        type=int,
    )
    parser.add_argument(
        "--update-delay",
        "-u",
        default=5,
        help="Delay between updates from clients (in seconds). Defaults to `5`",
        type=int,
    )
    parser.add_argument(
        "--debug", "-d", help="Log additional data", action="store_true"
    )

    args = parser.parse_args()
    return Settings(
        port=args.port,
        update_delay=args.update_delay,
        debug=args.debug,
    )


def wait_for_server(width: int, height: int) -> tuple[Any, float] | None:
    output_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # regarding socket.IP_MULTICAST_TTL
    # ---------------------------------
    # for all packets sent, after two hops on the network the packet will not
    # be re-sent/broadcast (see https://www.tldp.org/HOWTO/Multicast-HOWTO-6.html)
    output_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    output_socket.connect((MCAST_GRP, PORT))

    input_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    input_socket.bind(("", PORT))
    input_socket.settimeout(5)

    should_loop = True
    initial_message = f"open {width} {height} {socket.gethostname()}".encode()

    while should_loop:
        output_socket.send(initial_message)

        for input in select.select([sys.stdin, input_socket.fileno()], [], [], 5)[0]:
            match input:
                case _ if input == sys.stdin:
                    match sys.stdin.read(1):
                        case x if shared.is_closing_character(x):
                            should_loop = False
                        case "q" | "Q":
                            should_loop = False
                case _ if input == input_socket.fileno():
                    data, (address, _port) = input_socket.recvfrom(1024)

                    string_data = data.decode()
                    if string_data.startswith("start "):
                        delay = float(string_data[6:])

                        return address, delay

    return None


def loop_iteration(
    input_socket: socket.socket, output_socket: socket.socket
) -> bool | None:
    inputs, _, _ = select.select([sys.stdin, input_socket.fileno()], [], [], 5)
    if not inputs:
        # print("Heartbeat not received, detaching...")
        # output_socket.send(b"close")
        # return True
        ...

    for input in inputs:
        if input == sys.stdin:
            match sys.stdin.read(1):
                case x if shared.is_closing_character(x):
                    output_socket.send(b"close")
                    return False
                case "q" | "Q":
                    output_socket.send(b"close")
                    return False
        elif input == input_socket.fileno():
            data = input_socket.recv(1024)
            string_data = data.decode()

            if string_data.startswith("quit "):
                force = string_data[5:] != "False"

                if force:
                    print("Server closed, closing...")
                    return False
                else:
                    print("Server closed, detaching...")
                    return True

            if string_data == "ping":
                print("Ping!!")

            # bytes, (address, port) = sock.recvfrom(10240)
            # print(bytes)
            # print(f"{address}:{port}")

            # host = bytes.decode()
            # if host not in hosts_map:
            #     hosts_map[host] = address
            #     sock.sendto(b"ciaooo", address)

            # print(hosts_map)


def loop(address: Any, time: float, capture: cv2.VideoCapture) -> bool:
    input_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    input_socket.bind(("", PORT))

    # output_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # output_socket.connect((address, PORT))

    output_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def get_ip_address():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        print(s.getsockname())
        return s.getsockname()[0]

    output_socket.bind((get_ip_address(), PORT))
    output_socket.listen(1)

    exit_event = threading.Event()

    streaming_thread = threading.Thread(
        target=stream_data,
        args=(output_socket, time, capture, exit_event),
        kwargs={},
    )
    streaming_thread.start()

    while True:
        retry = loop_iteration(input_socket, output_socket)
        if retry is not None:
            exit_event.set()
            streaming_thread.join()
            return retry

        if not streaming_thread.is_alive():
            return True


def main():
    term_mode = shared.set_term_mode(sys.stdin)

    capture = cv2.VideoCapture(0)
    capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

    try:
        while True:
            result = wait_for_server(width, height)
            if result is None:
                break

            address, delay = result

            print(f"Connected to {stylize(address, Fore.light_green + Style.bold)}")
            retry = loop(address, delay, capture)

            if not retry:
                break
    except Exception as e:
        print(e)

    termios.tcsetattr(sys.stdin, termios.TCSAFLUSH, term_mode)


def stream_data(
    socket: socket.socket,
    delay: float,
    capture: cv2.VideoCapture,
    exit: threading.Event,
):
    counter = 0
    start = time.time()

    socket, _ = socket.accept()

    try:
        while not exit.is_set():
            while time.time() - start < delay:
                ok, frame = capture.read()

            start = time.time()
            ok, frame = capture.read()
            if ok:
                socket.sendall(frame.tobytes())

                # socket.send(f"{counter}".encode())
                print(f"sending data: {counter}")
                counter += 1

            # exit.wait(delay)
    except:
        ...

    capture.release()


main()
