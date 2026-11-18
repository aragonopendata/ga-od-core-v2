"""Fixtures for the internal transport sources: a local Zaragoza API, configuration and a guard against self-calls."""

import urllib.request

import pytest
from django.conf import settings

import connectors
from gaodcore_project.cache_backends import TRANSPORTS_CACHE_ALIAS
from gaodcore_project.config import ZaragozaInternalSources, ZaragozaTransport
from gaodcore_transports import sources
from gaodcore_transports.tests.consorcio_mock import ConsorcioMock

STOPS_URI = "internal://transports/zaragoza/stops"
ARRIVALS_URI = "internal://transports/zaragoza/arrival_time"


@pytest.fixture
def consorcio():
    mock = ConsorcioMock().start()
    yield mock
    mock.stop()


@pytest.fixture
def transports_cache():
    """The shared ``transports`` cache, emptied before and after the test."""
    from django.core.cache import caches

    cache = caches[TRANSPORTS_CACHE_ALIAS]
    cache.clear()
    yield cache
    cache.clear()


@pytest.fixture
def internal_config(monkeypatch, consorcio):
    """Point the internal sources at the local API; ``internal_config.set(**values)`` changes the settings."""
    current = {"values": {}}

    def zaragoza_config():
        real = settings.CONFIG.projects.transport.zaragoza
        return ZaragozaTransport(base_url=consorcio.base_url, max_concurrency=5, endpoints=real.endpoints)

    def internal():
        return ZaragozaInternalSources(**current["values"])

    monkeypatch.setattr(sources, "zaragoza_config", zaragoza_config)
    monkeypatch.setattr(sources, "internal_config", internal)

    class Handle:
        @staticmethod
        def set(**values):
            current["values"] = {**current["values"], **values}

    return Handle()


@pytest.fixture
def self_calls(monkeypatch, consorcio):
    """Fail on any outbound call that is not to the local API (such as the public URL of GA_OD_Core) and on any use
    of the HTTP connector. Every attempt is recorded and the test fails at teardown too."""
    attempts = []
    real_urlopen = urllib.request.urlopen

    def guarded_urlopen(url, *args, **kwargs):
        target = url if isinstance(url, str) else url.full_url
        if not target.startswith(consorcio.base_url):
            attempts.append(target)
            raise AssertionError(f"unexpected outbound call (self-call?): {target}")
        return real_urlopen(url, *args, **kwargs)

    def forbidden(*args, **kwargs):
        attempts.append("HTTP connector (self-call)")
        raise AssertionError("the HTTP connector must not be used by the internal sources")

    monkeypatch.setattr(urllib.request, "urlopen", guarded_urlopen)
    monkeypatch.setattr(connectors, "_get_engine_from_api", forbidden)
    yield attempts
    assert attempts == [], f"outbound calls outside the local API: {attempts}"


@pytest.fixture
def stops_source(transports_cache, internal_config, self_calls):
    """Everything the stops tests need: empty shared cache, local API, configuration and the guards."""
    return transports_cache
