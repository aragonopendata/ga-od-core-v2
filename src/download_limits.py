"""Limits for large downloads, built on PostgreSQL session advisory locks of the configuration database.

A large download costs minutes of a worker and gigabytes of memory, and some clients repeat the same one several
times at once. Two limits protect the rest of the service:

* L2, no identical download at the same time: a second request for the same normalized URL gets 429.
* L3, a ceiling of simultaneous large downloads per resource and in total: without a free slot the request gets 503.

Each limit is a ``pg_try_advisory_lock(int4, int4)`` that is never waited for. Session locks are shared by every
worker and released by PostgreSQL itself when the connection dies, so a killed worker (out of memory, timeout) never
leaves a lock behind. The locks are taken in a fixed order (L2, per resource, total) and released together.

Django keeps its connection open between requests (``CONN_MAX_AGE``), so the locks are released explicitly, when the
server finishes sending the response (see ``DownloadPermit.attach``).
"""

import hashlib
import logging
import struct
import threading
from typing import Iterable, List, Optional, Tuple

from django.conf import settings
from django.db import connections

from exceptions import DownloadInProgress, TooManyDownloads

logger = logging.getLogger(__name__)

# Lock classes (first int4 of the key) owned by this project.
LOCK_CLASS_DUPLICATE = 0x67410001
LOCK_CLASS_RESOURCE = 0x67410002
LOCK_CLASS_TOTAL = 0x67410003

# Query parameters that only name the file or choose the format; the resolved format is part of the key instead.
_IGNORED_PARAMS = frozenset({"resource_id", "view_id", "name", "nameRes", "formato", "format"})

_warned_unsupported = False


def limiter_connection():
    """Connection whose session holds the locks: the default Django connection (tests replace this function)."""
    return connections["default"]


def normalized_url(resource_id: int, query_params, format: str) -> str:
    """Identity of a download: resource, resolved format and the other parameters, sorted.

    ``name``/``nameRes`` only change the file name and the several spellings of the format collapse into ``format``.
    """
    params = sorted(
        (key, tuple(sorted(query_params.getlist(key)))) for key in query_params.keys() if key not in _IGNORED_PARAMS
    )
    return f"{resource_id}|{format}|{params!r}"


def url_lock_key(url: str) -> int:
    """First 32 bits of the SHA-256 of ``url`` as a signed integer (``int4``)."""
    return struct.unpack(">i", hashlib.sha256(url.encode("utf-8")).digest()[:4])[0]


class DownloadPermit:
    """The advisory locks held by one large download; ``release`` is idempotent and never raises."""

    def __init__(self, connection, held: List[Tuple[int, int]]):
        self._connection = connection
        self._held = held
        self._lock = threading.Lock()

    def release(self) -> None:
        """Release every lock. If that fails, close the connection: PostgreSQL then drops all the session locks."""
        with self._lock:
            held, self._held = self._held, []
        if not held:
            return
        if getattr(self._connection, "connection", True) is None:
            return  # connection already closed: its locks are gone
        try:
            _unlock(self._connection, held)
        except Exception:  # noqa: BLE001 - releasing must never break the response
            logger.exception("Could not release the download locks; closing the connection to drop them")
            try:
                self._connection.close()
            except Exception:  # noqa: BLE001
                logger.exception("Could not close the connection holding the download locks")

    def attach(self, response) -> None:
        """Release the locks when the server finishes with ``response`` (``response.close()``).

        That covers DRF's lazy rendering and streaming responses such as ``FileResponse``. If the lazy rendering
        fails, Django sends an error response instead and never closes this one, so the locks are released there.
        """
        # pylint: disable=protected-access
        response._resource_closers.append(self.release)
        if getattr(response, "is_rendered", True):
            return
        original_render = response.render

        def render_or_release():
            try:
                return original_render()
            except BaseException:
                self.release()
                raise

        response.render = render_or_release


def _try_lock(cursor, lock_class: int, key: int) -> bool:
    cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", [lock_class, key])
    return bool(cursor.fetchone()[0])


def _unlock(connection, held: Iterable[Tuple[int, int]]) -> None:
    with connection.cursor() as cursor:
        for lock_class, key in held:
            cursor.execute("SELECT pg_advisory_unlock(%s, %s)", [lock_class, key])
            if not cursor.fetchone()[0]:
                logger.warning("Download lock (%s, %s) was not held by this session", lock_class, key)


def _take_slot(cursor, lock_class: int, base: int, slots: int, held: list) -> bool:
    """Take the first free slot ``base + n`` of ``lock_class``; True on success."""
    for slot in range(slots):
        key = base + slot
        if _try_lock(cursor, lock_class, key):
            held.append((lock_class, key))
            return True
    return False


def is_enabled(connection) -> bool:
    """Whether limits apply: switched on in the configuration and the configuration database is PostgreSQL."""
    global _warned_unsupported  # pylint: disable=global-statement
    if not settings.DOWNLOAD_LIMITS.enabled:
        return False
    if connection.vendor != "postgresql":
        if not _warned_unsupported:
            _warned_unsupported = True
            logger.warning(
                "Download limits are disabled: the configuration database is %s and they need PostgreSQL",
                connection.vendor,
            )
        return False
    return True


def is_heavy(resource_id: int, limit: Optional[int]) -> bool:
    """Large download: no limit (or one of at least ``heavy_rows``) on a resource known to be large."""
    config = settings.DOWNLOAD_LIMITS
    if limit is not None and limit < config.heavy_rows:
        return False
    if resource_id in config.heavy_resource_ids:
        return True
    from gaodcore_manager.models import ResourceSizeConfig  # pylint: disable=import-outside-toplevel

    registries = ResourceSizeConfig.objects.filter(resource_id=resource_id).values_list("registries", flat=True).first()
    return registries is not None and registries >= config.heavy_rows


def acquire(resource_id: int, limit: Optional[int], url: str, connection=None) -> Optional[DownloadPermit]:
    """Take the locks of a large download.

    @return: the permit to release when the download ends, or None when the request is not limited.
    @raise DownloadInProgress: an identical download is running (429).
    @raise TooManyDownloads: no free slot for this resource or overall (503).
    """
    connection = connection if connection is not None else limiter_connection()
    if not is_enabled(connection) or not is_heavy(resource_id, limit):
        return None
    config = settings.DOWNLOAD_LIMITS
    held: List[Tuple[int, int]] = []
    try:
        with connection.cursor() as cursor:
            if not _try_lock(cursor, LOCK_CLASS_DUPLICATE, url_lock_key(url)):
                raise DownloadInProgress(config.retry_after)
            held.append((LOCK_CLASS_DUPLICATE, url_lock_key(url)))
            if not _take_slot(cursor, LOCK_CLASS_RESOURCE, resource_id * 16, config.max_per_resource, held):
                raise TooManyDownloads(config.retry_after, "resource")
            if not _take_slot(cursor, LOCK_CLASS_TOTAL, 0, config.max_total, held):
                raise TooManyDownloads(config.retry_after, "total")
    except BaseException:
        DownloadPermit(connection, held).release()
        raise
    return DownloadPermit(connection, held)
