"""Zaragoza transport resources served in process, never through the public URL of GA_OD_Core.

``internal://transports/zaragoza/stops`` is the stops catalogue, served from a copy shared by every worker in the
``transports`` cache (Redis in production, memory of the process without it). Nothing runs outside the request: the one
that finds the copy old and gets the lock downloads it from the API of the Consorcio.

Exclusion between workers uses ``cache.add`` (``SET NX EX`` in Redis) and is never waited for: whoever does not get a
lock serves the previous copy or answers 503 with ``Retry-After``. A lock expires after ``stops_lock_seconds``, longer
than the call it protects, so a worker that dies frees it by itself.

The rows are loaded into the same in-memory SQLite table as an HTTP connector, with the same columns and types, so the
generic code applies filters, ``like``, fields, sort, pagination and formats as before.
``internal://transports/zaragoza/arrival_time`` serves the arrivals of ONE stop, read from ``arrival_time/{code}``. The
stop comes from an equality on ``stop_id`` or ``cod_parada`` in ``filters``. Each stop is cached for every worker
(``zaragoza:arrival:<code>``, expiring by itself), at most one worker calls the API for a stop at a time (a lock per
stop) and at most ``arrivals_max_concurrency`` calls run at once among all the workers (slots). Nobody waits: a request
that cannot call serves the previous value while it is young enough, or gets a quick 503. A query that designates no
stop is the aggregate of every stop of the stops copy (``zaragoza:arrival:all``): rebuilt in the request that gets
``lock:all`` (one call per stop, ``max_concurrency`` at a time, a failed stop is skipped and counted), served from the
cache otherwise, or the previous aggregate / 503 while another worker rebuilds.
"""

import csv
import http.client
import io
import json
import logging
import math
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http import HTTPStatus
from time import monotonic
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from uuid import uuid4

from django.conf import settings
from django.core.cache import caches
from django.utils import timezone
from redis.exceptions import RedisError
from rest_framework_csv.renderers import CSVRenderer

from connectors import (
    INTERNAL_SCHEME,
    DriverConnectionError,
    FieldNoExistsError,
    NotImplementedSchemaError,
    OpenResource,
    SortFieldNoExistsError,
    _get_model,
    engine_from_rows,
)
from exceptions import BadGateway, DataNotReady, ErrorCodes, ServiceUnavailable, UpstreamBusy
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

# Keys of the arrivals of a stop (``<code>`` is the canonical stop code). The value expires after its TTL plus the stale
# margin; the lock and the slots expire by themselves; the error key makes the stop wait ``arrivals_error_ttl_seconds``.
ARRIVAL_KEY = "zaragoza:arrival:{code}"
ARRIVAL_ERROR_KEY = "zaragoza:arrival:{code}:error"
ARRIVAL_LOCK_KEY = "lock:arrival:{code}"
ARRIVAL_SLOT_KEY = "lock:arrival:slot:{index}"

# Columns of the arrivals resource as the HTTP connector read them from arrival_time.csv: the fields of the API plus
# the stop code twice, sorted as the CSV renderer sorts its header. Used to reject unknown columns before any call and
# as the columns of an answer without rows.
ARRIVAL_API_FIELDS = (
    "id", "id_linea", "name", "route", "direction", "bus", "departure_time", "remaining_time", "TR", "inc", "url",
)
STOP_FIELDS = ("stop_id", "cod_parada")
ARRIVAL_COLUMNS = tuple(sorted(ARRIVAL_API_FIELDS + STOP_FIELDS))

# What the API answers in ``nombre`` for a code it does not know.
UNKNOWN_STOP_NAME = "parada desconocida"
_STOP_CODE_RE = re.compile(r"[1-9][0-9]{0,5}")
_MAX_STOP_CODE = 999999

# Keys of the aggregate of every stop. The value expires after its TTL plus the stale margin; the lock and the error
# key (a rebuild where every stop failed) expire by themselves.
ALL_KEY = "zaragoza:arrival:all"
ALL_ERROR_KEY = "zaragoza:arrival:all:error"
ALL_LOCK_KEY = "lock:all"


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
# Arrivals of one stop
# --------------------------------------------------------------------------------------------------------------------


@dataclass
class ArrivalPlan:
    """What a query asks of the arrivals: one stop (``code``, canonical), no row at all (``impossible``: an equality no
    row can satisfy) or every stop (``aggregate``)."""

    code: Optional[str] = None
    impossible: bool = False
    aggregate: bool = False


def canonical_stop_code(value: Any) -> Optional[str]:
    """The stop code an equality value designates, as the API and the ``stop_id`` column write it, or None.

    The columns are text, so ``40`` and ``"40"`` match the row of stop 40 but ``"040"``, ``40.0`` or ``" 40"`` match
    nothing; booleans and codes outside 1..999999 never designate a stop.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if 1 <= value <= _MAX_STOP_CODE else None
    if isinstance(value, str) and _STOP_CODE_RE.fullmatch(value):
        return value
    return None


def plan_arrivals(filters: Dict[str, Any], like: Any, fields: Iterable[str], sort: Iterable[Any]) -> ArrivalPlan:
    """Check the columns of the query against the known schema and find the stop it designates. No call is made.

    @raise FieldNoExistsError / SortFieldNoExistsError: unknown column, or a filter on ``id`` without a stop (400
        ``INVALID_FIELD`` / ``INVALID_SORT``, as the generic code would answer).
    """
    known = frozenset(ARRIVAL_COLUMNS)
    unknown_filters = [name for name in filters if not name.startswith("$") and name not in known]
    if isinstance(like, dict):
        unknown_filters += [name for name in like if name not in known]
    if unknown_filters:
        raise FieldNoExistsError(f"Filter field: {', '.join(dict.fromkeys(unknown_filters))} not exists.")
    for name in fields:
        if name not in known:
            raise FieldNoExistsError(f"Field: {name} not exists.")
    for item in sort:
        if item.field not in known:
            raise SortFieldNoExistsError(message=f"Sort field: {item.field} not exists.")

    values = [filters[name] for name in STOP_FIELDS if name in filters and not isinstance(filters[name], (dict, list))]
    if not values:
        if "id" in filters:
            # ``id`` is the id of the arrival in the API, not the code of the stop (the old fan-out never made it the
            # stop either): without a stop it would need every row, so the client is told how to ask.
            raise FieldNoExistsError(
                "Filter field: id does not designate a stop of this resource; filter by stop_id or cod_parada "
                "(equality), as with parada."
            )
        return ArrivalPlan(aggregate=True)
    codes = {canonical_stop_code(value) for value in values}
    if None in codes or len(codes) > 1:
        # An equality no row can satisfy (not a code, or two different stops): empty answer, no call.
        return ArrivalPlan(impossible=True)
    return ArrivalPlan(code=codes.pop())


@dataclass
class ArrivalAnswer:
    """Rows of one stop, already as the HTTP connector read them (text), and how they were obtained."""

    code: str
    status: str  # ok | empty | unknown
    fields: List[str]
    rows: List[List[str]]
    cache: str  # hit | miss | stale
    age: float = 0.0

    def describe(self) -> str:
        return f"arrivals:{self.cache}:{self.status}:code={self.code}:age={self.age:.0f}"


def open_all_arrivals(timer=None) -> Tuple[OpenResource, str]:
    """Resource with the arrivals of every stop: the single place where the aggregate (a query without a stop) hooks in.

    @raise DataNotReady: no usable stops copy, or the shared cache is down.
    @raise UpstreamBusy, ServiceUnavailable, BadGateway: no aggregate to serve and none could be built.
    """
    with phase(timer, "connect"):
        aggregate, description = lookup_all_arrivals()
    return _resource_from_rows(aggregate["fields"], aggregate["rows"], timer), description


def _resource_from_rows(fields: List[str], rows: List[List[str]], timer=None) -> OpenResource:
    with phase(timer, "connect"):
        engine = engine_from_rows([dict(zip(fields, row)) for row in rows], fields)
        try:
            model = _get_model(engine=engine, object_location=None, object_location_schema=None)
        except BaseException:
            engine.dispose()
            raise
    return OpenResource(engine=engine, model=model)


def arrivals_resource(plan: ArrivalPlan, timer=None) -> Tuple[OpenResource, str]:
    """Open the rows the query designates as a resource (no call when ``plan.impossible``)."""
    if plan.aggregate:
        return open_all_arrivals(timer)
    if plan.impossible:
        fields, rows, description = list(ARRIVAL_COLUMNS), [], "arrivals:none:impossible"
    else:
        with phase(timer, "connect"):
            answer = lookup_arrivals(plan.code)
        fields, rows, description = answer.fields, answer.rows, answer.describe()
    return _resource_from_rows(fields, rows, timer), description


def _all_description(entry: Dict[str, Any], state: str, age: float) -> str:
    return f"arrivals:all:{state}:age={age:.0f}:failures={entry['failures']}"


def lookup_all_arrivals() -> Tuple[Dict[str, Any], str]:
    """The aggregate of the arrivals of every stop of the stops copy and what was done to get it.

    Fresh for ``arrivals_aggregate_ttl_seconds``. Older (up to ``arrivals_aggregate_max_stale_seconds`` more) it is
    served while one request, the one that gets ``lock:all``, rebuilds it: one call per stop, ``max_concurrency`` at a
    time, within the lock. Nobody waits for a lock: the rest serve the previous aggregate or get a quick 503
    (``UpstreamBusy``). When every stop failed nothing is published and no new attempt is made for
    ``arrivals_aggregate_error_ttl_seconds``. Any Redis error is ``DataNotReady``.
    """
    cfg = internal_config()
    cache = transports_cache()
    try:
        entry = cache.get(ALL_KEY)
        age = _arrival_age(entry)
        if age is not None and age < cfg.arrivals_aggregate_ttl_seconds:
            return entry, _all_description(entry, "hit", age)
        failed = cache.get(ALL_ERROR_KEY)
        if failed is not None:
            remaining = cfg.arrivals_aggregate_error_ttl_seconds - (timezone.now() - failed["at"]).total_seconds()
            return _stale_all_or_raise(entry, age, cfg, _arrival_failure(failed["kind"], remaining), "failed")
        token = uuid4().hex
        if not cache.add(ALL_LOCK_KEY, token, cfg.arrivals_aggregate_lock_seconds):
            busy = UpstreamBusy(cfg.arrivals_aggregate_retry_after_seconds)
            return _stale_all_or_raise(entry, age, cfg, busy, "busy")
        try:
            return _rebuild_all(cache, cfg, entry, age)
        finally:
            _release(cache, ALL_LOCK_KEY, token)
    except RedisError as err:
        logger.warning("The transports cache is not available for the arrivals aggregate: %s: %s",
                       type(err).__name__, err)
        exc = DataNotReady(cfg.arrivals_aggregate_retry_after_seconds)
        exc.transport = "arrivals:all:unavailable"
        raise exc from err


def _stale_all_or_raise(entry, age, cfg, exc, reason: str):
    """Serve the previous aggregate if still within the stale margin, else raise ``exc``."""
    limit = cfg.arrivals_aggregate_ttl_seconds + cfg.arrivals_aggregate_max_stale_seconds
    if age is not None and age < limit:
        return entry, _all_description(entry, f"stale-{reason}", age)
    exc.transport = f"arrivals:all:{reason}"
    raise exc


def _rebuild_all(cache, cfg, entry, age):
    """Call every stop under the lock and publish the aggregate; if all fail keep the previous one and back off."""
    # Another worker may have published while this one was getting the lock.
    current = cache.get(ALL_KEY)
    current_age = _arrival_age(current)
    if current_age is not None and current_age < cfg.arrivals_aggregate_ttl_seconds:
        return current, _all_description(current, "hit", current_age)
    started = monotonic()
    try:
        stops, _state = load_stops()
    except SnapshotUnavailable as err:
        # Without the catalogue there is nothing to ask for: serve the previous aggregate or say it is not ready.
        exc = DataNotReady(cfg.stops_retry_after_seconds)
        exc.transport = f"arrivals:all:{err.reason}"
        return _stale_all_or_raise(entry, age, cfg, exc, "nostops")
    # The lock expires ``arrivals_aggregate_lock_seconds`` after it was taken: stop calling before that.
    deadline = started + cfg.arrivals_aggregate_lock_seconds - 5
    items, failures, kind = _fetch_all_arrivals(cfg, stops, deadline)
    duration = monotonic() - started
    count = len(stops)
    if failures == count:
        cache.set(ALL_ERROR_KEY, {"kind": kind, "at": timezone.now()}, cfg.arrivals_aggregate_error_ttl_seconds)
        logger.warning("transport_refresh source=%s result=failed stops=%s failures=%s duration=%.3f",
                       f"{SOURCE_ARRIVALS}:all", count, failures, duration)
        return _stale_all_or_raise(entry, age, cfg, _arrival_failure(kind, cfg.arrivals_aggregate_error_ttl_seconds),
                                   "failed")
    fields, rows = as_csv_rows(items)
    value = {"fetched_at": timezone.now(), "fields": fields, "rows": rows, "stops": count, "failures": failures}
    try:
        cache.set(ALL_KEY, value, cfg.arrivals_aggregate_ttl_seconds + cfg.arrivals_aggregate_max_stale_seconds)
    except RedisError:
        # The rows are good: answer with them even though the others cannot reuse them.
        logger.warning("Could not cache the arrivals aggregate", exc_info=True)
    log = logger.warning if failures else logger.info
    log("transport_refresh source=%s result=refreshed stops=%s rows=%s failures=%s duration=%.3f",
        f"{SOURCE_ARRIVALS}:all", count, len(rows), failures, duration)
    return value, _all_description(value, "rebuilt", 0.0)


def _fetch_all_arrivals(cfg, stops, deadline: float) -> Tuple[List[Dict[str, Any]], int, str]:
    """Rows (dicts, as the old fan-out built them) of every stop, the number of stops that failed and the kind of the
    last failure. A failed stop (error, invalid answer or no time left) only counts: it never fails the others."""
    from gaodcore_transports.views.zaragoza import arrival_time_configs  # pylint: disable=import-outside-toplevel

    zaragoza = zaragoza_config()
    configs = arrival_time_configs(stops, zaragoza.get_url)

    def one(config) -> List[Dict[str, Any]]:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise FetchError("error", "no time left in the rebuild")
        data = fetch_json(config.url, min(cfg.arrivals_timeout_seconds, remaining), cfg.arrivals_max_bytes)
        _status, items = _arrival_items(config.extra_data["stop_id"], data)
        return items

    def safe(config):
        try:
            return one(config), None
        except FetchError as err:
            return [], err.kind

    with ThreadPoolExecutor(max_workers=max(1, zaragoza.max_concurrency)) as pool:
        results = list(pool.map(safe, configs))
    kinds = [kind for _items, kind in results if kind is not None]
    items = [item for rows, kind in results if kind is None for item in rows]
    return items, len(kinds), (kinds[-1] if kinds else "error")


def _arrival_ttl(cfg, status: str) -> int:
    return cfg.arrivals_unknown_ttl_seconds if status == "unknown" else cfg.arrivals_ttl_seconds


def _arrival_answer(code: str, entry: Dict[str, Any], cache: str, age: float) -> ArrivalAnswer:
    return ArrivalAnswer(code, entry["status"], entry["fields"], entry["rows"], cache, age)


def _arrival_lock_seconds(cfg) -> int:
    """Expiry of the lock of a stop and of a slot: longer than the call they protect, so a dead worker frees them."""
    return math.ceil(cfg.arrivals_timeout_seconds) + 5


def lookup_arrivals(code: str) -> ArrivalAnswer:
    """Arrivals of stop ``code``: the shared cached value if fresh, else one call to the API.

    Only one worker calls the API for a stop at a time (lock per stop) and at most ``arrivals_max_concurrency`` calls
    run at once among all the workers (slots). Nobody waits for a lock: a request that cannot call reuses the last
    value if it is at most ``arrivals_max_stale_seconds`` past its TTL, or gets a quick 503 (``UpstreamBusy``). After a
    failed call the stop is not called again for ``arrivals_error_ttl_seconds``. A failure never becomes an empty
    success. Any Redis error is ``DataNotReady``.
    """
    cfg = internal_config()
    cache = transports_cache()
    try:
        entry = cache.get(ARRIVAL_KEY.format(code=code))
        age = _arrival_age(entry)
        if age is not None and age < _arrival_ttl(cfg, entry["status"]):
            return _arrival_answer(code, entry, "hit", age)
        failed = cache.get(ARRIVAL_ERROR_KEY.format(code=code))
        if failed is not None:
            remaining = cfg.arrivals_error_ttl_seconds - (timezone.now() - failed["at"]).total_seconds()
            return _stale_arrival_or_raise(code, entry, age, cfg, _arrival_failure(failed["kind"], remaining))

        lock_key = ARRIVAL_LOCK_KEY.format(code=code)
        token = uuid4().hex
        if not cache.add(lock_key, token, _arrival_lock_seconds(cfg)):
            return _stale_arrival_or_raise(code, entry, age, cfg, UpstreamBusy(cfg.arrivals_busy_retry_after_seconds), "busy")
        try:
            slot = _take_arrival_slot(cache, cfg, token)
            if slot is None:
                return _stale_arrival_or_raise(
                    code, entry, age, cfg, UpstreamBusy(cfg.arrivals_busy_retry_after_seconds), "capacity"
                )
            try:
                return _call_arrivals(cache, cfg, code, entry, age)
            finally:
                _release(cache, slot, token)
        finally:
            _release(cache, lock_key, token)
    except RedisError as err:
        logger.warning("The transports cache is not available for the arrivals of stop %s: %s: %s", code,
                       type(err).__name__, err)
        exc = DataNotReady(cfg.arrivals_busy_retry_after_seconds)
        exc.transport = "arrivals:unavailable"
        raise exc from err


def _arrival_age(entry: Optional[Dict[str, Any]]) -> Optional[float]:
    if entry is None:
        return None
    return (timezone.now() - entry["fetched_at"]).total_seconds()


def _take_arrival_slot(cache, cfg, token: str) -> Optional[str]:
    """Key of the first free slot (now ours), or None when ``arrivals_max_concurrency`` calls are running."""
    for index in range(cfg.arrivals_max_concurrency):
        key = ARRIVAL_SLOT_KEY.format(index=index)
        if cache.add(key, token, _arrival_lock_seconds(cfg)):
            return key
    return None


def _call_arrivals(cache, cfg, code: str, entry, age) -> ArrivalAnswer:
    """Call the API under the lock and a slot, and publish the answer; on failure remember it and serve the last value."""
    # Another worker may have answered (or failed) while this one was getting here.
    entry = cache.get(ARRIVAL_KEY.format(code=code))
    age = _arrival_age(entry)
    if age is not None and age < _arrival_ttl(cfg, entry["status"]):
        return _arrival_answer(code, entry, "hit", age)
    url = zaragoza_config().get_url("arrival_time", stop_id=code)
    started = monotonic()
    try:
        status, fields, rows = parse_arrivals(
            code, fetch_json(url, cfg.arrivals_timeout_seconds, cfg.arrivals_max_bytes)
        )
    except FetchError as err:
        cache.set(ARRIVAL_ERROR_KEY.format(code=code), {"kind": err.kind, "at": timezone.now()},
                  cfg.arrivals_error_ttl_seconds)
        logger.warning(
            "transport_call source=%s code=%s result=%s reason=%s duration=%.3f",
            SOURCE_ARRIVALS, code, err.kind, err.reason, monotonic() - started,
        )
        return _stale_arrival_or_raise(code, entry, age, cfg, _arrival_failure(err.kind, cfg.arrivals_error_ttl_seconds))
    duration = monotonic() - started
    value = {"fetched_at": timezone.now(), "status": status, "fields": fields, "rows": rows}
    try:
        cache.set(ARRIVAL_KEY.format(code=code), value, _arrival_ttl(cfg, status) + cfg.arrivals_max_stale_seconds)
    except RedisError:
        # The rows are good: answer with them even though the others cannot reuse them.
        logger.warning("Could not cache the arrivals of stop %s", code, exc_info=True)
    logger.info(
        "transport_call source=%s code=%s result=%s rows=%s duration=%.3f",
        SOURCE_ARRIVALS, code, status, len(rows), duration,
    )
    return ArrivalAnswer(code, status, fields, rows, "miss", 0.0)


def _arrival_failure(kind: str, wait: float):
    """The HTTP error of a failed call: 503 for a network/HTTP failure, 502 for an invalid answer."""
    wait = max(1, int(round(wait)))
    if kind == "invalid":
        exc = BadGateway()
    else:
        exc = ServiceUnavailable("Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE)
    exc.wait = wait
    exc.transport = f"arrivals:{kind}"
    return exc


def _stale_arrival_or_raise(code, entry, age, cfg, exc, reason: str = ""):
    """Serve the last value if still within the stale margin, else raise ``exc``."""
    if age is not None and age < _arrival_ttl(cfg, entry["status"]) + cfg.arrivals_max_stale_seconds:
        return _arrival_answer(code, entry, "stale", age)
    if reason:
        exc.transport = f"arrivals:{reason}"
    raise exc


def parse_arrivals(code: str, data: Any) -> Tuple[str, List[str], List[List[str]]]:
    """Status and rows of an answer of ``arrival_time/{code}``, with the columns and text values the HTTP connector
    read from ``arrival_time.csv``.

    @raise FetchError('invalid'): the answer is not an object with a list (or object) of arrival objects.
    """
    status, rows = _arrival_items(code, data)
    if status != "ok":
        return status, list(ARRIVAL_COLUMNS), []
    fields, text_rows = as_csv_rows(rows)
    return "ok", fields, text_rows


def _arrival_items(code: str, data: Any) -> Tuple[str, List[Dict[str, Any]]]:
    """Status (``ok``, ``empty``, ``unknown``) and the arrival objects of an answer, each with the stop code in
    ``stop_id`` and ``cod_parada``.

    @raise FetchError('invalid'): the answer is not an object with a list (or object) of arrival objects.
    """
    if not isinstance(data, dict):
        raise FetchError("invalid", "the answer is not an object")
    arrivals = data.get("arrival_time")
    if arrivals is None:
        arrivals = []
    if isinstance(arrivals, dict):
        arrivals = list(arrivals.values())
    if not isinstance(arrivals, list) or not all(isinstance(item, dict) for item in arrivals):
        raise FetchError("invalid", "arrival_time is not a list of objects")
    name = data.get("nombre")
    if isinstance(name, str) and name.strip().casefold() == UNKNOWN_STOP_NAME:
        return "unknown", []
    if not arrivals:
        return "empty", []
    return "ok", [{**item, "stop_id": code, "cod_parada": code} for item in arrivals]


def as_csv_rows(rows: List[Dict[str, Any]]) -> Tuple[List[str], List[List[str]]]:
    """Header and rows of ``rows`` after the round trip the HTTP connector made: rendered by the CSV renderer of
    ``arrival_time.csv`` (same normalization, sorted header, nested values flattened) and read back as text."""
    from utils import get_return_list  # pylint: disable=import-outside-toplevel

    text = CSVRenderer().render(get_return_list(rows)).decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    table = list(reader)
    if not table:
        return list(ARRIVAL_COLUMNS), []
    return table[0], table[1:]


# --------------------------------------------------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------------------------------------------------


def engine_for_uri(uri: str):
    """Engine of an internal source for the generic code (``connectors._get_engine``): health checks, the manager
    validator and ``show_columns``.

    Never calls the API: arrivals are an empty table with their columns (those callers only check that the source
    answers and what columns it has).

    @raise SnapshotUnavailable: stops without a usable copy.
    """
    source = source_of(uri)
    if source == SOURCE_STOPS:
        resource, _state = stops_resource()
        return resource.engine
    return engine_from_rows([], ARRIVAL_COLUMNS)


@dataclass
class OpenedInternal:
    resource: OpenResource
    description: str


def open_for_request(uri: str, filters, like, fields, sort, timer=None) -> OpenedInternal:
    """Open an internal source for ``preview``/``download`` (the caller disposes the engine).

    ``filters``, ``like``, ``fields`` and ``sort`` are for the sources that decide what to read from the query.

    @raise DataNotReady: stops without a usable copy, or the shared cache down (503 with Retry-After).
    @raise FieldNoExistsError, SortFieldNoExistsError: arrivals with an unknown column or ``id`` without a stop (400).
    @raise UpstreamBusy, ServiceUnavailable, BadGateway: arrivals that cannot be called or whose call failed.
    @raise UpstreamBusy, BadGateway, ...: see ``open_all_arrivals`` for arrivals without a stop.
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
    plan = plan_arrivals(filters, like, fields, sort)
    resource, description = arrivals_resource(plan, timer)
    return OpenedInternal(resource, description)
