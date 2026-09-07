"""Minimal raw-socket HTTP test server for deterministic timeout/slow-response tests.

Gives full control over exactly when the status line, headers, and body bytes are
written to the socket - something pytest-httpserver (built on Werkzeug) does not
expose. Not used by production code, only by tests in this repository.
"""

import contextlib
import socket
import threading
from typing import Callable

Handler = Callable[[socket.socket], None]


@contextlib.contextmanager
def raw_http_server(handler: Handler):
    """Start a background TCP server on 127.0.0.1 that runs `handler(conn)` once per
    accepted connection. Yields the server's base URL (e.g. "http://127.0.0.1:54321")."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(5)
    server_sock.settimeout(0.5)
    port = server_sock.getsockname()[1]
    stop = threading.Event()

    def serve():
        while not stop.is_set():
            try:
                conn, _ = server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                handler(conn)
            except OSError:
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/resource"
    finally:
        stop.set()
        thread.join(timeout=2)
        server_sock.close()


def drain_request(conn: socket.socket) -> None:
    """Read (and discard) the incoming request line/headers so the client's send
    completes normally before we start writing the response."""
    conn.settimeout(5)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk


def respond_status_then_stall(status_line: bytes, headers: bytes, delay_seconds: float) -> Handler:
    """Send a full status line + headers immediately (ending the header block), then
    stall before ever writing a body - simulating a service that answers fast but
    whose body never (usefully) arrives."""

    def _handler(conn: socket.socket) -> None:
        drain_request(conn)
        conn.sendall(status_line + headers + b"\r\n")
        conn.settimeout(delay_seconds + 5)
        import time

        time.sleep(delay_seconds)
        with contextlib.suppress(OSError):
            conn.sendall(b"x" * 10)

    return _handler


def respond_after_delay(status_line: bytes, headers: bytes, body: bytes, delay_seconds: float) -> Handler:
    """Wait `delay_seconds`, then send a complete response (status/headers/body) in
    one shot - simulating a backend that takes a while to answer but then responds
    normally with no separate header/body delay."""

    def _handler(conn: socket.socket) -> None:
        drain_request(conn)
        import time

        time.sleep(delay_seconds)
        with contextlib.suppress(OSError):
            conn.sendall(status_line + headers + b"\r\n" + body)

    return _handler


def accept_and_hang(_hang_seconds: float) -> Handler:
    """Accept the TCP connection, read the request, and then never write anything
    back - simulating a fully unresponsive origin."""

    def _handler(conn: socket.socket) -> None:
        drain_request(conn)
        import time

        time.sleep(_hang_seconds)

    return _handler
