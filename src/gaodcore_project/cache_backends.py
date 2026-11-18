"""Cache configuration and the Redis backend that tolerates Redis being down.

Two aliases share one Redis server with different key prefixes:

- ``default``: the ``cache_page`` responses of the transport views. A Redis error is a cache miss (``get``) or a no-op
  (``set``), so the views answer without cache instead of failing.
- ``transports``: the shared state of the in-process transport sources (stops copy, arrivals, locks). Errors are
  raised: the sources turn them into 503.

Without ``common_config.redis_url`` the previous setup is kept: ``default`` in the ``django_cache`` table and
``transports`` in the memory of each process (no exclusion between workers).
"""

import logging
import threading
import time
from typing import Optional

from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.redis import RedisCache
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

TRANSPORTS_CACHE_ALIAS = "transports"

# Socket timeouts in seconds: a Redis that does not answer must not hold a worker.
DEFAULT_SOCKET_TIMEOUT = 0.5
TRANSPORTS_SOCKET_TIMEOUT = 1.0


def build_caches(redis_url: Optional[str]) -> dict:
    """Return the ``CACHES`` setting for the given Redis URL (``None``: no Redis)."""
    if not redis_url:
        return {
            "default": {
                "BACKEND": "django.core.cache.backends.db.DatabaseCache",
                "LOCATION": "django_cache",
            },
            TRANSPORTS_CACHE_ALIAS: {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                "LOCATION": "gaodcore-transports",
            },
        }
    return {
        "default": {
            "BACKEND": "gaodcore_project.cache_backends.TolerantRedisCache",
            "LOCATION": redis_url,
            "KEY_PREFIX": "gaodcore:page",
            "OPTIONS": {
                "socket_connect_timeout": DEFAULT_SOCKET_TIMEOUT,
                "socket_timeout": DEFAULT_SOCKET_TIMEOUT,
            },
        },
        TRANSPORTS_CACHE_ALIAS: {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": redis_url,
            "KEY_PREFIX": "gaodcore:transports",
            "OPTIONS": {
                "socket_connect_timeout": TRANSPORTS_SOCKET_TIMEOUT,
                "socket_timeout": TRANSPORTS_SOCKET_TIMEOUT,
            },
        },
    }


class TolerantRedisCache(RedisCache):
    """``RedisCache`` that treats a Redis error as a cache miss or a no-op.

    At most one WARNING every ``warning_interval`` seconds per process, so an outage does not flood the logs.
    """

    warning_interval = 60.0

    def __init__(self, server, params):
        super().__init__(server, params)
        self._warning_lock = threading.Lock()
        self._last_warning = float("-inf")
        self._suppressed = 0

    def _failed(self, operation: str, exc: Exception) -> None:
        now = time.monotonic()
        with self._warning_lock:
            if now - self._last_warning < self.warning_interval:
                self._suppressed += 1
                return
            suppressed, self._suppressed = self._suppressed, 0
            self._last_warning = now
        logger.warning(
            "Redis cache unavailable, serving without cache: operation=%s error=%s: %s suppressed=%d",
            operation,
            type(exc).__name__,
            exc,
            suppressed,
        )

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        try:
            return super().add(key, value, timeout, version)
        except RedisError as exc:
            self._failed("add", exc)
            return False

    def get(self, key, default=None, version=None):
        try:
            return super().get(key, default, version)
        except RedisError as exc:
            self._failed("get", exc)
            return default

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        try:
            super().set(key, value, timeout, version)
        except RedisError as exc:
            self._failed("set", exc)

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        try:
            return super().touch(key, timeout, version)
        except RedisError as exc:
            self._failed("touch", exc)
            return False

    def delete(self, key, version=None):
        try:
            return super().delete(key, version)
        except RedisError as exc:
            self._failed("delete", exc)
            return False

    def get_many(self, keys, version=None):
        try:
            return super().get_many(keys, version)
        except RedisError as exc:
            self._failed("get_many", exc)
            return {}

    def has_key(self, key, version=None):
        try:
            return super().has_key(key, version)
        except RedisError as exc:
            self._failed("has_key", exc)
            return False

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        try:
            return super().set_many(data, timeout, version)
        except RedisError as exc:
            self._failed("set_many", exc)
            return list(data)

    def delete_many(self, keys, version=None):
        try:
            super().delete_many(keys, version)
        except RedisError as exc:
            self._failed("delete_many", exc)
