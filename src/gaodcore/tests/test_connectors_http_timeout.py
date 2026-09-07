"""Deterministic tests for the HTTP connector timeout/error handling in connectors.py.

Uses a raw-socket test server (slow_http_server.py) instead of the real, flaky
consorciozaragoza.es API so these tests are fast and reproducible.
"""

import time
from unittest.mock import Mock

import pytest

from connectors import (
    DriverConnectionError,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    _get_engine,
    _get_engine_from_api,
)
from gaodcore.tests.slow_http_server import (
    accept_and_hang,
    raw_http_server,
    respond_after_delay,
    respond_status_then_stall,
)


def test_default_timeout_constant_is_finite():
    assert isinstance(DEFAULT_HTTP_TIMEOUT_SECONDS, (int, float))
    assert DEFAULT_HTTP_TIMEOUT_SECONDS > 0


def test_immediate_error_status_propagates_without_extra_delay():
    """503 with immediate headers and a short delay: the call should fail promptly,
    close to the delay, not block for the (much larger) default timeout."""
    handler = respond_after_delay(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\n",
        b"{}",
        delay_seconds=0.3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(DriverConnectionError):
            _get_engine_from_api(url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)
        elapsed = time.monotonic() - start
    assert elapsed < 2, f"took {elapsed}s, expected close to the 0.3s server delay"


def test_immediate_headers_slow_body_rejected_without_waiting_for_body():
    """503 whose headers arrive immediately but whose body stalls for longer than the
    configured timeout: urllib raises HTTPError right after the status line, without
    ever waiting on the slow body."""
    handler = respond_status_then_stall(
        b"HTTP/1.1 503 Service Unavailable\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(DriverConnectionError):
            _get_engine_from_api(url, timeout=1)
        elapsed = time.monotonic() - start
    assert elapsed < 1, f"took {elapsed}s, should reject on headers alone, not wait on body"


def test_200_with_stalled_body_times_out_with_driver_connection_error():
    """Status 200 with headers sent, but body never (usefully) arrives: this used to
    raise a raw TimeoutError that escaped the except (HTTPError, URLError) clause.
    It must now be converted into DriverConnectionError, bounded by `timeout`."""
    handler = respond_status_then_stall(
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 100000\r\nContent-Type: application/json\r\n",
        delay_seconds=3,
    )
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(DriverConnectionError):
            _get_engine_from_api(url, timeout=1)
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the 1s timeout"


def test_connection_accepted_then_fully_silent_times_out():
    """Origin accepts the TCP connection and then never sends anything at all
    (no status line, no headers): must not hang forever."""
    handler = accept_and_hang(_hang_seconds=3)
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(DriverConnectionError):
            _get_engine_from_api(url, timeout=1)
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the 1s timeout"


def test_no_explicit_timeout_still_bounded_by_default(monkeypatch):
    """The ordinary public path must never end up calling urlopen(timeout=None):
    when the caller passes timeout=None, DEFAULT_HTTP_TIMEOUT_SECONDS is used
    instead. Verified here with a small monkeypatched default so the test stays
    fast without changing the *behaviour* under test (defaulting-when-None)."""
    import connectors

    monkeypatch.setattr(connectors, "DEFAULT_HTTP_TIMEOUT_SECONDS", 1)
    handler = accept_and_hang(_hang_seconds=3)
    with raw_http_server(handler) as url:
        start = time.monotonic()
        with pytest.raises(DriverConnectionError):
            _get_engine(url)
        elapsed = time.monotonic() - start
    assert elapsed < 2.5, f"took {elapsed}s, expected bounded by the patched 1s default"


def test_successful_json_response_still_works():
    """Baseline: a normal, fast 200 JSON response must still work end-to-end."""
    body = b'[{"id": 1, "name": "a"}]'
    handler = respond_after_delay(
        b"HTTP/1.1 200 OK\r\n",
        f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n".encode(),
        body,
        delay_seconds=0,
    )
    with raw_http_server(handler) as url:
        engine = _get_engine_from_api(url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)
    with engine.connect() as conn:
        from sqlalchemy import text

        rows = conn.execute(text("SELECT id, name FROM temporal_table")).fetchall()
    assert [tuple(row) for row in rows] == [(1, "a")]


@pytest.mark.parametrize(
    "uri, timeout_key",
    [
        ("postgresql://example.invalid/test", "connect_timeout"),
        ("mysql://example.invalid/test", "connect_timeout"),
        ("mssql+pyodbc://example.invalid/test", "timeout"),
    ],
)
@pytest.mark.parametrize("timeout", [None, 45])
def test_http_default_does_not_override_database_timeouts(monkeypatch, uri, timeout_key, timeout):
    """SQL keeps its default or an explicitly requested timeout, never the HTTP default."""
    create_engine = Mock()
    monkeypatch.setattr("connectors.create_engine", create_engine)

    _get_engine(uri, timeout=timeout)

    expected = {} if timeout is None else {timeout_key: timeout}
    assert create_engine.call_args.kwargs["connect_args"] == expected
