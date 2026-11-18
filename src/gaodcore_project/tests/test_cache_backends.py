"""The page cache keeps the views working when Redis is down; the cache setup follows common_config.redis_url."""

import logging
import socket

import pytest
from django.core.cache import caches
from django.http import HttpResponse
from django.test import RequestFactory, override_settings
from django.views.decorators.cache import cache_page

from gaodcore_project.cache_backends import TRANSPORTS_CACHE_ALIAS, TolerantRedisCache, build_caches


def _closed_port() -> int:
    """A local TCP port with nothing listening: connecting to it is refused at once."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def dead_redis_caches():
    url = f"redis://127.0.0.1:{_closed_port()}/0"
    config = build_caches(url)
    with override_settings(CACHES=config):
        yield
    for alias in config:
        caches[alias].close()


def test_build_caches_without_redis_keeps_database_cache():
    config = build_caches(None)

    assert config["default"]["BACKEND"] == "django.core.cache.backends.db.DatabaseCache"
    assert config["default"]["LOCATION"] == "django_cache"
    assert config[TRANSPORTS_CACHE_ALIAS]["BACKEND"] == "django.core.cache.backends.locmem.LocMemCache"


def test_build_caches_with_redis_uses_one_server_with_two_prefixes():
    config = build_caches("redis://redis-gaodcore:6379/0")

    assert config["default"]["BACKEND"] == "gaodcore_project.cache_backends.TolerantRedisCache"
    assert config[TRANSPORTS_CACHE_ALIAS]["BACKEND"] == "django.core.cache.backends.redis.RedisCache"
    assert {c["LOCATION"] for c in config.values()} == {"redis://redis-gaodcore:6379/0"}
    assert config["default"]["KEY_PREFIX"] != config[TRANSPORTS_CACHE_ALIAS]["KEY_PREFIX"]
    for alias_config in config.values():
        assert alias_config["OPTIONS"]["socket_timeout"] <= 1
        assert alias_config["OPTIONS"]["socket_connect_timeout"] <= 1


def test_tolerant_cache_turns_connection_errors_into_misses(dead_redis_caches):
    cache = caches["default"]
    assert isinstance(cache, TolerantRedisCache)

    assert cache.get("key") is None
    assert cache.get("key", "fallback") == "fallback"
    cache.set("key", "value", 10)
    assert cache.add("key", "value", 10) is False
    assert cache.get_many(["a", "b"]) == {}
    assert cache.has_key("key") is False
    assert cache.delete("key") is False
    assert cache.touch("key", 10) is False
    assert cache.set_many({"a": 1}, 10) == ["a"]
    cache.delete_many(["a"])


def test_transports_cache_raises_on_connection_errors(dead_redis_caches):
    from redis.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        caches[TRANSPORTS_CACHE_ALIAS].get("key")


def test_tolerant_cache_limits_warnings(dead_redis_caches, caplog):
    cache = caches["default"]
    caplog.set_level(logging.WARNING, logger="gaodcore_project.cache_backends")

    for _ in range(5):
        cache.get("key")

    warnings = [r for r in caplog.records if r.name == "gaodcore_project.cache_backends"]
    assert len(warnings) == 1
    assert "operation=get" in warnings[0].getMessage()


def test_cache_page_view_answers_without_redis(dead_redis_caches):
    calls = []

    @cache_page(60)
    def view(request):
        calls.append(1)
        return HttpResponse(b"body")

    request_factory = RequestFactory()
    first = view(request_factory.get("/view"))
    second = view(request_factory.get("/view"))

    assert first.status_code == second.status_code == 200
    assert first.content == second.content == b"body"
    assert len(calls) == 2  # nothing could be cached


def test_cache_page_view_is_cached_with_local_memory_cache():
    calls = []

    @cache_page(60)
    def view(request):
        calls.append(1)
        return HttpResponse(b"body")

    request_factory = RequestFactory()
    caches["default"].clear()
    view(request_factory.get("/cached-view"))
    second = view(request_factory.get("/cached-view"))

    assert second.content == b"body"
    assert len(calls) == 1
