"""Zaragoza transport resources served in process, never through the public URL of GA_OD_Core.

``internal://transports/zaragoza/stops`` is the stops catalogue, served from a copy shared by every worker in the
``transports`` cache (Redis in production, memory of the process without it). Nothing runs outside the request: the one
that finds the copy old and gets the lock downloads it from the API of the Consorcio.

Exclusion between workers uses ``cache.add`` (``SET NX EX`` in Redis) and is never waited for: whoever does not get a
lock serves the previous copy or answers 503 with ``Retry-After``. A lock expires after ``stops_lock_seconds``, longer
than the call it protects, so a worker that dies frees it by itself.

The rows are loaded into the same in-memory SQLite table as an HTTP connector, with the same columns and types, so the
generic code applies filters, ``like``, fields, sort, pagination and formats as before. ``arrival_time`` is not served
yet: ``source_of`` knows its URI and ``open_for_request`` refuses it with a clear error.
"""

import http.client
import json
import logging
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from time import monotonic
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from uuid import uuid4

from django.conf import settings
from django.core.cache import caches
from django.utils import timezone
from redis.exceptions import RedisError

from connectors import (
    INTERNAL_SCHEME,
    DriverConnectionError,
    NotImplementedSchemaError,
    OpenResource,
    _get_model,
    engine_from_rows,
)
from exceptions import DataNotReady
from gaodcore_project.cache_backends import TRANSPORTS_CACHE_ALIAS
from log_sanitizer import mask_uri
from phase_timing import phase

logger = logging.getLogger(__name__)

SOURCE_STOPS = "zaragoza/stops"
SOURCE_ARRIVALS = "zaragoza/arrival_time"
_SOURCES = {
    f"{INTERNAL_SCHEME}://transports/zaragoza/stops": SOURCE_STOPS,
    f"{INTERNAL_SCHEME}://transports/zaragoza/arrival_time": SOURCE_ARRIVALS,
}

# Keys of the ``transports`` cache. The copy has no expiry (it must not be evicted by time: its age is checked here);
# the lock and the backoff expire by themselves.
STOPS_KEY = "zaragoza:stops"
STOPS_LOCK_KEY = "lock:stops"
STOPS_BACKOFF_KEY = "zaragoza:stops:backoff"
STOPS_FAILURES_KEY = "zaragoza:stops:failures"
_FAILURES_TTL = 3600
_STOPS_MAX_BYTES = 50_000_000


def internal_config():
    """``projects.transport.zaragoza.internal`` of the configuration (tests replace it)."""
    return settings.CONFIG.projects.transport.zaragoza.internal


def zaragoza_config():
    """``projects.transport.zaragoza`` of the configuration: base URL and endpoints of the API."""
    return settings.CONFIG.projects.transport.zaragoza


def transports_cache():
    """Cache shared by the workers (tests replace it)."""
    return caches[TRANSPORTS_CACHE_ALIAS]


def source_of(uri: str) -> str:
    """Name of the internal source of ``uri``; only the known URIs are accepted."""
    try:
        return _SOURCES[uri.strip()]
    except KeyError as err:
        raise NotImplementedSchemaError(f'Internal source "{mask_uri(uri)}" does not exist.') from err


def is_internal(uri: str) -> bool:
    return urlparse(uri or "").scheme == INTERNAL_SCHEME


def _not_supported(source: str) -> NotImplementedSchemaError:
    return NotImplementedSchemaError(f'Internal source "{source}" is not supported yet.')


# --------------------------------------------------------------------------------------------------------------------
# Calls to the API
# --------------------------------------------------------------------------------------------------------------------


class FetchError(Exception):
    """A call to the API failed. ``kind`` is ``error`` (network, timeout, HTTP status) or ``invalid`` (answer)."""

    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind
        self.reason = reason


def fetch_json(url: str, timeout: float, max_bytes: int) -> Any:
    """GET ``url`` and parse its JSON body, within ``timeout`` seconds for the whole call.

    Every socket operation is bounded by the time left and the body is read in pieces, checking the deadline between
    them, so a server that trickles bytes cannot hold the caller much longer than ``timeout``. A body over
    ``max_bytes`` is an invalid answer.
    """
    deadline = monotonic() + timeout
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - URL from the configuration
            if response.getcode() != HTTPStatus.OK:
                raise FetchError("error", f"HTTP status {response.getcode()}")
            chunks = []
            size = 0
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise FetchError("error", f"timed out after {timeout}s while reading")
                _set_socket_timeout(response, remaining)
                chunk = response.read1(65536)
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise FetchError("invalid", f"answer larger than {max_bytes} bytes")
                chunks.append(chunk)
            if getattr(response, "length", None):
                # Content-Length announced more than arrived: the connection closed mid-answer.
                raise FetchError("error", f"connection closed with {response.length} bytes missing")
    except FetchError:
        raise
    except http.client.HTTPException as err:
        # IncompleteRead of a chunked answer, bad status line...
        raise FetchError("error", f"{type(err).__name__}: {err}") from err
    except HTTPError as err:
        status = err.code
        err.close()
        raise FetchError("error", f"HTTP status {status}") from err
    except URLError as err:
        raise FetchError("error", f"URL error: {err.reason}") from err
    except (TimeoutError, OSError) as err:
        raise FetchError("error", f"{type(err).__name__}: {err}") from err
    try:
        return json.loads(b"".join(chunks))
    except ValueError as err:
        raise FetchError("invalid", f"invalid JSON: {err}") from err


def _set_socket_timeout(response, seconds: float) -> None:
    """Bound the next socket read of ``response`` (best effort: the socket is not public API of http.client)."""
    sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
    if sock is not None:
        sock.settimeout(max(seconds, 0.001))


# --------------------------------------------------------------------------------------------------------------------
# Stops catalogue (copy shared through the cache)
# --------------------------------------------------------------------------------------------------------------------


class SnapshotUnavailable(DriverConnectionError):
    """There is no usable copy of a catalogue (none yet, older than the stale margin, or the cache is down)."""


@dataclass
class SnapshotState:
    """What a request did with the stops copy, for the ``download_timing`` line."""

    age: Optional[float]
    state: str  # fresh | stale | refreshed | downloaded | missing | expired | unavailable

    def describe(self) -> str:
        age = "-" if self.age is None else f"{self.age:.0f}"
        return f"stops:{self.state}:age={age}"


def stops_resource(timer=None) -> Tuple[OpenResource, SnapshotState]:
    """Open the stops copy as a resource, refreshing it on demand (see ``load_stops``).

    @raise SnapshotUnavailable: no usable copy and this request could not get one.
    """
    with phase(timer, "connect"):
        rows, state = load_stops()
        engine = engine_from_rows(rows)
        try:
            model = _get_model(engine=engine, object_location=None, object_location_schema=None)
        except BaseException:
            engine.dispose()
            raise
    return OpenResource(engine=engine, model=model), state


def load_stops() -> Tuple[List[Dict[str, Any]], SnapshotState]:
    """Rows of the stops catalogue and what was done to get them.

    Fresh for ``stops_max_age_seconds``. Older (up to ``stops_max_stale_seconds`` more) it is served, and the request
    that gets the lock refreshes it first (~1 s) while the rest serve the previous copy. Older than that, or absent, it
    counts as missing: whoever gets the lock downloads it and answers, the rest fail with ``SnapshotUnavailable``.
    During the backoff after a failed download nothing is downloaded. Any Redis error is ``SnapshotUnavailable``.
    """
    cfg = internal_config()
    cache = transports_cache()
    try:
        entry = cache.get(STOPS_KEY)
        age = _age(entry)
        if age is not None and age > cfg.stops_max_age_seconds + cfg.stops_max_stale_seconds:
            entry, state_if_none = None, "expired"
        else:
            state_if_none = "missing" if age is None else ""
        if entry is not None and age <= cfg.stops_max_age_seconds:
            return entry["rows"], SnapshotState(age, "fresh")
        usable = entry["rows"] if entry is not None else None
        stale = SnapshotState(age, "stale")
        if cache.get(STOPS_BACKOFF_KEY) is not None:
            return _stale_or_raise(usable, stale, SnapshotState(age, state_if_none))
        token = uuid4().hex
        if not cache.add(STOPS_LOCK_KEY, token, cfg.stops_lock_seconds):
            return _stale_or_raise(usable, stale, SnapshotState(age, state_if_none))
        try:
            return _refresh(cache, cfg, usable, age, state_if_none)
        finally:
            _release(cache, STOPS_LOCK_KEY, token)
    except RedisError as err:
        logger.warning("The transports cache is not available for the stops copy: %s: %s", type(err).__name__, err)
        raise SnapshotUnavailable(
            "The stops copy is not available.", reason=SnapshotState(None, "unavailable").describe()
        ) from err


def _age(entry: Optional[Dict[str, Any]]) -> Optional[float]:
    if entry is None:
        return None
    return (timezone.now() - entry["fetched_at"]).total_seconds()


def _stale_or_raise(rows, stale: SnapshotState, missing: SnapshotState):
    """Serve the previous copy if there is one, else fail without waiting for anybody."""
    if rows is not None:
        return rows, stale
    raise SnapshotUnavailable("The stops copy is not available.", reason=missing.describe())


def _release(cache, key: str, token: str) -> None:
    """Free the lock ``key`` if it is still ours (it may have expired and been taken by another worker)."""
    try:
        if cache.get(key) == token:
            cache.delete(key)
    except RedisError:
        logger.warning("Could not release the lock %s; it expires by itself", key, exc_info=True)


def _refresh(cache, cfg, usable, age, state_if_none):
    """Download the stops under the lock and publish them; on failure keep the previous copy and back off."""
    # Another worker may have published a copy while this one was getting the lock.
    entry = cache.get(STOPS_KEY)
    fresh_age = _age(entry)
    if entry is not None and fresh_age <= cfg.stops_max_age_seconds:
        return entry["rows"], SnapshotState(fresh_age, "fresh")
    started = monotonic()
    try:
        data = fetch_json(zaragoza_config().get_url("stops"), cfg.stops_timeout_seconds, _STOPS_MAX_BYTES)
        stops = _validate_stops(data, len(usable) if usable is not None else 0, cfg.stops_min_ratio)
    except FetchError as err:
        failures = (cache.get(STOPS_FAILURES_KEY) or 0) + 1
        wait = min(cfg.refresh_backoff_max_seconds, cfg.refresh_backoff_min_seconds * 2 ** (failures - 1))
        cache.set(STOPS_FAILURES_KEY, failures, _FAILURES_TTL)
        cache.set(STOPS_BACKOFF_KEY, failures, wait)
        logger.warning(
            "transport_refresh source=%s result=failed kind=%s reason=%s failures=%s backoff=%ss copy_age=%s",
            SOURCE_STOPS, err.kind, err.reason, failures, wait, "-" if age is None else f"{age:.0f}",
        )
        return _stale_or_raise(usable, SnapshotState(age, "stale"), SnapshotState(age, state_if_none or "missing"))
    try:
        cache.set(STOPS_KEY, {"fetched_at": timezone.now(), "rows": stops}, None)
        cache.delete(STOPS_FAILURES_KEY)
    except RedisError:
        # The rows are good: answer with them even though the others cannot share them yet.
        logger.warning("Could not publish the stops copy", exc_info=True)
    logger.info("transport_refresh source=%s result=refreshed rows=%s duration=%.3f", SOURCE_STOPS, len(stops),
                monotonic() - started)
    return stops, SnapshotState(0.0, "refreshed" if usable is not None else "downloaded")


_SCALARS = (str, int, float, bool, type(None))


def _validate_stops(data: Any, previous_rows: int, min_ratio: float) -> List[Dict[str, Any]]:
    """The ``stops`` list of an answer of the API, or ``FetchError('invalid')`` if it does not look like the catalogue.

    Every stop must be a flat object with ``stop_id`` (nested values would not fit the table), and a catalogue much
    smaller than the published one (``min_ratio``) is taken for a truncated answer.
    """
    stops = data.get("stops") if isinstance(data, dict) else None
    if not isinstance(stops, list) or not stops:
        raise FetchError("invalid", "no stops in the answer")
    for stop in stops:
        if not isinstance(stop, dict) or "stop_id" not in stop:
            raise FetchError("invalid", "a stop is not an object with stop_id")
        if not all(isinstance(value, _SCALARS) for value in stop.values()):
            raise FetchError("invalid", f"stop {stop.get('stop_id')!r} has nested values")
    if previous_rows and len(stops) < previous_rows * min_ratio:
        raise FetchError("invalid", f"{len(stops)} stops, the published copy has {previous_rows}")
    return stops


# --------------------------------------------------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------------------------------------------------


def engine_for_uri(uri: str):
    """Engine of an internal source for the generic code (``connectors._get_engine``): health checks, the manager
    validator and ``show_columns``.

    @raise SnapshotUnavailable: stops without a usable copy.
    """
    source = source_of(uri)
    if source == SOURCE_STOPS:
        resource, _state = stops_resource()
        return resource.engine
    raise _not_supported(source)


@dataclass
class OpenedInternal:
    resource: OpenResource
    description: str


def open_for_request(uri: str, filters, like, fields, sort, timer=None) -> OpenedInternal:
    """Open an internal source for ``preview``/``download`` (the caller disposes the engine).

    ``filters``, ``like``, ``fields`` and ``sort`` are for the sources that decide what to read from the query.

    @raise DataNotReady: stops without a usable copy (503 with Retry-After).
    @raise NotImplementedSchemaError: the source exists but is not served yet.
    """
    source = source_of(uri)
    if source == SOURCE_STOPS:
        try:
            resource, state = stops_resource(timer)
        except SnapshotUnavailable as err:
            exc = DataNotReady(internal_config().stops_retry_after_seconds)
            exc.transport = err.reason
            raise exc from err
        return OpenedInternal(resource, state.describe())
    raise _not_supported(source)
