"""Deterministic tests for utils.download()/download_async() timeout and error
handling, exercised against a local raw-socket server instead of the real (flaky)
consorciozaragoza.es API.
"""

import asyncio
import socket
import time

import aiohttp
import pytest

from exceptions import BadGateway
from gaodcore.tests.slow_http_server import (
    accept_and_hang,
    drain_request,
    raw_http_server,
    respond_after_delay,
    respond_status_then_stall,
)
from utils import download, download_async

# --- sync client (requests) -------------------------------------------------


def test_sync_immediate_error_status_propagates_without_extra_delay():
    handler = respond_after_delay(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\n",
        b"{}",
        delay_seconds=0.3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            download(url)
        elapsed = time.monotonic() - start
    assert elapsed < 2, f"took {elapsed}s, expected close to the 0.3s server delay"


def test_sync_immediate_headers_slow_body_rejected_without_waiting_for_body():
    handler = respond_status_then_stall(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            download(url)
        elapsed = time.monotonic() - start
    assert elapsed < 1, f"took {elapsed}s, should reject on headers alone, not wait on body"


def test_sync_200_with_stalled_body_raises_bad_gateway(monkeypatch):
    import utils

    monkeypatch.setattr(utils, "EXTERNAL_SERVICE_TIMEOUT_SECONDS", 1)
    handler = respond_status_then_stall(
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            download(url)
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the patched 1s timeout"


def test_sync_connection_refused_raises_bad_gateway():
    # Nothing listening on this port.
    with pytest.raises(BadGateway):
        download("http://127.0.0.1:1/resource")


def test_sync_successful_response_still_works():
    body = b'{"ok": true}'
    handler = respond_after_delay(
        b"HTTP/1.1 200 OK\r\n",
        f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n".encode(),
        body,
        delay_seconds=0,
    )
    with raw_http_server(handler) as url:
        assert download(url) == {"ok": True}


# --- async client (aiohttp) -------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


async def _download_async_once(url):
    async with aiohttp.ClientSession() as session:
        return await download_async(session, url)


def test_async_immediate_error_status_propagates_without_extra_delay():
    handler = respond_after_delay(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\n",
        b"{}",
        delay_seconds=0.3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            _run(_download_async_once(url))
        elapsed = time.monotonic() - start
    assert elapsed < 2, f"took {elapsed}s, expected close to the 0.3s server delay"


def test_async_immediate_headers_slow_body_rejected_without_waiting_for_body():
    handler = respond_status_then_stall(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            _run(_download_async_once(url))
        elapsed = time.monotonic() - start
    assert elapsed < 1, f"took {elapsed}s, should reject on headers alone, not wait on body"


def test_async_200_with_stalled_body_raises_bad_gateway_within_total_budget(monkeypatch):
    """This is the case that previously escaped as an uncaught asyncio.TimeoutError:
    a 200 response whose body never finishes must still surface as BadGateway,
    bounded by the total aiohttp timeout budget."""
    import utils

    monkeypatch.setattr(utils, "EXTERNAL_SERVICE_TIMEOUT_SECONDS", 1)
    handler = respond_status_then_stall(
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            _run(_download_async_once(url))
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the patched 1s total timeout"


def test_async_connection_accepted_then_fully_silent_times_out(monkeypatch):
    import utils

    monkeypatch.setattr(utils, "EXTERNAL_SERVICE_TIMEOUT_SECONDS", 1)
    handler = accept_and_hang(_hang_seconds=3)
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(BadGateway):
            _run(_download_async_once(url))
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the patched 1s total timeout"


def test_async_successful_response_still_works():
    body = b'{"ok": true}'
    handler = respond_after_delay(
        b"HTTP/1.1 200 OK\r\n",
        f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n".encode(),
        body,
        delay_seconds=0,
    )
    with raw_http_server(handler) as url:
        assert _run(_download_async_once(url)) == {"ok": True}


def test_async_connection_refused_preserves_cause_as_bad_gateway():
    # Reserve a port without listening, so another service cannot take it.
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{reserved.getsockname()[1]}/resource"
        with pytest.raises(BadGateway) as exc:
            _run(_download_async_once(url))
    assert exc.value.status_code == 502
    assert isinstance(exc.value.__cause__, aiohttp.ClientConnectorError)


def test_async_server_disconnect_preserves_cause_as_bad_gateway():
    # Close after receiving the request, before sending any response headers.
    with raw_http_server(drain_request) as url:
        with pytest.raises(BadGateway) as exc:
            _run(_download_async_once(url))
    assert exc.value.status_code == 502
    assert isinstance(exc.value.__cause__, aiohttp.ServerDisconnectedError)


def test_async_incomplete_body_preserves_cause_as_bad_gateway():
    handler = respond_after_delay(
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 1000\r\nContent-Type: application/json\r\n",
        b'{"ok":',
        delay_seconds=0,
    )
    with raw_http_server(handler) as url:
        with pytest.raises(BadGateway) as exc:
            _run(_download_async_once(url))
    assert exc.value.status_code == 502
    assert isinstance(exc.value.__cause__, aiohttp.ClientPayloadError)


def test_async_cancel_during_body_read_propagates_and_closes_connection():
    async def scenario():
        headers_received = asyncio.Event()
        peer_disconnected = asyncio.Event()

        async def serve(reader, writer):
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n"
                    b"Content-Type: application/json\r\n\r\n"
                )
                await writer.drain()
                await reader.read()  # Wait until cancellation closes the socket.
            finally:
                writer.close()
                await writer.wait_closed()
                peer_disconnected.set()

        async def on_headers(*args):
            headers_received.set()

        trace = aiohttp.TraceConfig()
        trace.on_request_end.append(on_headers)
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        async with server, aiohttp.ClientSession(trace_configs=[trace]) as session:
            port = server.sockets[0].getsockname()[1]
            task = asyncio.create_task(download_async(session, f"http://127.0.0.1:{port}/resource"))
            try:
                await asyncio.wait_for(headers_received.wait(), timeout=2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                # Assert closure while the session is still open.
                await asyncio.wait_for(peer_disconnected.wait(), timeout=2)
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    _run(scenario())
