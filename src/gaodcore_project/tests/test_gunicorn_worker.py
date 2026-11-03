"""DrainingThreadWorker: no accepted connection is lost when the worker stops, and the heartbeat goes on meanwhile."""

import os
import socket
import threading
import time
from concurrent import futures
from importlib.metadata import version
from unittest import mock

import pytest
from gunicorn.config import Config
from gunicorn.glogging import Logger
from gunicorn.workers.gthread import ThreadWorker

from gaodcore_project import gunicorn_worker
from gaodcore_project.gunicorn_worker import DrainingThreadWorker

# --- Guard: this worker overrides internals of gunicorn 21.2 -------------------------------------------------------

_REVIEWED_GUNICORN = (21, 2)
_USED_ATTRIBUTES = ("poller", "_keep", "_lock", "futures", "nr_conns", "tpool", "sockets", "worker_connections")
_USED_METHODS = ("accept", "on_client_socket_readable", "murder_keepalived", "is_parent_alive", "finish_request",
                 "notify", "run")


def test_gunicorn_version_was_reviewed_for_the_draining_worker():
    installed = tuple(int(part) for part in version("gunicorn").split(".")[:2])
    assert installed == _REVIEWED_GUNICORN, (
        f"gunicorn {version('gunicorn')} is installed but gaodcore_project/gunicorn_worker.py was written for "
        f"{'.'.join(map(str, _REVIEWED_GUNICORN))}.x: review DrainingThreadWorker.run against "
        "gunicorn/workers/gthread.py (ThreadWorker.run) and update _REVIEWED_GUNICORN in this test.")


@pytest.mark.parametrize("name", _USED_METHODS)
def test_gunicorn_method_used_by_the_draining_worker_still_exists(name):
    assert callable(getattr(ThreadWorker, name, None)), (
        f"gunicorn's ThreadWorker no longer has {name}(), which DrainingThreadWorker uses: review gunicorn_worker.py")


def test_gunicorn_attributes_used_by_the_draining_worker_still_exist():
    worker = _make_worker()
    missing = [name for name in _USED_ATTRIBUTES if not hasattr(worker, name)]
    assert not missing, f"ThreadWorker no longer sets {missing}, which DrainingThreadWorker uses: review gunicorn_worker.py"


# --- Behavior with a real local server -----------------------------------------------------------------------------


def _app(environ, start_response):
    if environ["PATH_INFO"] == "/slow":
        time.sleep(1.0)
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [environ["PATH_INFO"].encode()]


def _make_worker(threads=2, listener=None, graceful_timeout=30):
    cfg = Config()
    cfg.set("threads", threads)
    cfg.set("worker_connections", threads + 2)
    cfg.set("graceful_timeout", graceful_timeout)
    cfg.set("keepalive", 2)
    worker = DrainingThreadWorker(1, os.getppid(), [listener] if listener else [], _app, 30, cfg, Logger(cfg))
    worker.wsgi = _app
    worker.tpool = futures.ThreadPoolExecutor(max_workers=threads)
    worker.poller = __import__("selectors").DefaultSelector()
    worker._lock = threading.RLock()  # pylint: disable=protected-access
    return worker


@pytest.fixture(name="running")
def fixture_running():
    """A worker serving a local port in a thread. Yields (worker, port, run_thread)."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(64)
    worker = _make_worker(threads=2, listener=listener)
    beats = []
    original_notify = worker.notify
    worker.notify = lambda: (beats.append(time.monotonic()), original_notify())[1]
    worker.beats = beats
    thread = threading.Thread(target=worker.run, name="worker-run", daemon=True)
    thread.start()
    yield worker, listener.getsockname()[1], thread
    worker.alive = False
    thread.join(10)


def _get(port, path, keepalive=False, results=None, key=None):
    sock = socket.create_connection(("127.0.0.1", port), timeout=15)
    sock.sendall(f"GET {path} HTTP/1.1\r\nHost: x\r\n"
                 f"Connection: {'keep-alive' if keepalive else 'close'}\r\n\r\n".encode())
    data = b""
    try:
        while not data.endswith(b"0\r\n\r\n"):  # chunked response
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    except OSError as err:
        data = f"ERROR {err!r}".encode()
    finally:
        if not keepalive:
            sock.close()
    if results is not None:
        results[key] = data
    return (data, sock) if keepalive else data


def _wait_until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def test_connections_accepted_but_without_thread_are_served_when_the_worker_stops(running):
    worker, port, run_thread = running
    results = {}
    clients = [threading.Thread(target=_get, args=(port, "/slow", False, results, i)) for i in range(2)]  # both threads busy
    for client in clients:
        client.start()
    assert _wait_until(lambda: len(worker.futures) == 2)
    queued = [threading.Thread(target=_get, args=(port, "/queued", False, results, f"q{i}")) for i in range(2)]
    for client in queued:  # accepted by the worker (worker_connections = threads + 2) but no thread is free
        client.start()
    assert _wait_until(lambda: worker.nr_conns == 4)
    worker.alive = False
    for client in clients + queued:
        client.join(15)
    run_thread.join(15)
    assert not run_thread.is_alive()
    assert all(b"/slow" in results[key] for key in (0, 1)), results
    assert all(results[f"q{i}"].startswith(b"HTTP/1.1 200") and b"/queued" in results[f"q{i}"] for i in range(2)), results


def test_heartbeat_continues_while_draining(running):
    worker, port, run_thread = running
    results = {}
    client = threading.Thread(target=_get, args=(port, "/slow", False, results, 0))
    client.start()
    assert _wait_until(lambda: len(worker.futures) == 1)
    worker.alive = False
    assert _wait_until(lambda: len(worker.beats) > 0)
    start = len(worker.beats)
    client.join(15)
    run_thread.join(15)
    assert b"/slow" in results[0]
    assert len(worker.beats) >= start
    assert not run_thread.is_alive()


def test_idle_keepalive_connection_is_closed_when_the_worker_stops(running):
    worker, port, run_thread = running
    data, sock = _get(port, "/fast", keepalive=True)
    assert b"/fast" in data
    assert _wait_until(lambda: len(worker._keep) == 1)  # pylint: disable=protected-access
    worker.alive = False
    sock.settimeout(10)
    assert sock.recv(10) == b""  # closed by the worker, no request was pending
    sock.close()
    run_thread.join(10)
    assert not run_thread.is_alive()


def test_worker_exits_promptly_when_nothing_is_pending(running):
    worker, _port, run_thread = running
    worker.alive = False
    run_thread.join(5)
    assert not run_thread.is_alive()


def test_new_connections_are_not_accepted_after_stopping(running):
    worker, port, run_thread = running
    worker.alive = False
    run_thread.join(5)
    assert not run_thread.is_alive()
    with pytest.raises(OSError):
        sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        sock.sendall(b"GET /fast HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        if sock.recv(10) == b"":
            raise ConnectionResetError("closed without response")


def test_fresh_connection_without_request_is_dropped_after_the_grace_period(running):
    worker, port, run_thread = running
    idle = socket.create_connection(("127.0.0.1", port), timeout=15)  # accepted, never sends anything
    assert _wait_until(lambda: worker.nr_conns == 1)
    with mock.patch.object(gunicorn_worker, "_FRESH_CONNECTION_GRACE", 0.3):
        worker.alive = False
        run_thread.join(10)
    assert not run_thread.is_alive()
    assert idle.recv(10) == b""
    idle.close()


def test_stopping_after_graceful_timeout_gives_up_on_stuck_requests():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    worker = _make_worker(threads=1, listener=listener, graceful_timeout=1)
    release = threading.Event()

    def stuck_app(environ, start_response):  # pylint: disable=unused-argument
        release.wait(20)
        start_response("200 OK", [])
        return [b""]

    worker.wsgi = stuck_app
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    port = listener.getsockname()[1]
    client = threading.Thread(target=_get, args=(port, "/slow"), daemon=True)
    client.start()
    assert _wait_until(lambda: len(worker.futures) == 1)
    started = time.monotonic()
    worker.alive = False
    thread.join(10)
    release.set()
    assert not thread.is_alive()
    assert time.monotonic() - started < 6
