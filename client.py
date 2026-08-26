import select
import socket
import sys
import termios
import threading
from typing import Any

from colored import Fore, Style, stylize

import util

MCAST_GRP = "224.1.1.1"
PORT = 5007


def wait_for_server() -> tuple[Any, int] | None:
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
    while should_loop:
        output_socket.send(f"open {socket.gethostname()}".encode())

        inputs, _, _ = select.select([sys.stdin, input_socket.fileno()], [], [], 5)
        for input in inputs:
            match input:
                case _ if input == sys.stdin:
                    match sys.stdin.read(1):
                        case x if util.is_closing_character(x):
                            should_loop = False
                        case "q" | "Q":
                            should_loop = False
                case _ if input == input_socket.fileno():
                    data, (address, _port) = input_socket.recvfrom(1024)

                    string_data = data.decode()
                    if string_data.startswith("start "):
                        time = int(string_data[6:])

                        return address, time

    return None


def loop_iteration(
    input_socket: socket.socket, output_socket: socket.socket
) -> bool | None:
    inputs, _, _ = select.select([sys.stdin, input_socket.fileno()], [], [], 10)
    if not inputs:
        print("Heartbeat not received, detaching...")
        output_socket.send(b"close")
        return True

    for input in inputs:
        if input == sys.stdin:
            match sys.stdin.read(1):
                case x if util.is_closing_character(x):
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


def loop(address: Any, time: int) -> bool:
    input_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    input_socket.bind(("", PORT))

    output_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    output_socket.connect((address, PORT))

    exit_event = threading.Event()

    streaming_thread = threading.Thread(
        target=stream_data,
        args=(output_socket, time, exit_event),
        kwargs={},
    )
    streaming_thread.start()

    while True:
        retry = loop_iteration(input_socket, output_socket)
        if retry is not None:
            exit_event.set()
            streaming_thread.join()
            return retry


def main():
    term_mode = util.set_term_mode(sys.stdin)

    while True:
        result = wait_for_server()
        if result is None:
            break

        address, time = result

        print(f"Connected to {stylize(address, Fore.light_green + Style.bold)}")
        retry = loop(address, time)

        if not retry:
            break

    termios.tcsetattr(sys.stdin, termios.TCSAFLUSH, term_mode)


def stream_data(socket: socket.socket, delay: int, exit: threading.Event):
    counter = 0

    while not exit.is_set():
        socket.send(f"{counter}".encode())
        print(f"sending data: {counter}")
        counter += 1

        exit.wait(delay)


main()
