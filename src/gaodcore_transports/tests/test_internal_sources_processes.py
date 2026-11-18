"""The internal transport sources with independent processes and a real Redis.

The functional tests use ``LocMemCache`` and threads of one process. Here every worker is a separate Python process
(as the Gunicorn workers are) with its own Django and its own Redis connections, all on one ``redis:7-alpine``
container, and the Consorcio is the local mock of the pytest process, which counts the calls it receives.

Redis is started with the generic ``testcontainers`` ``DockerContainer`` (like ``connectors/conftest.py``), not with
``RedisContainer`` (it imports ``redis.asyncio``, absent from redis-py 4.1) nor ``pytest_docker_fixtures`` (its
file-lock serialisation between xdist workers is of no use: these tests run serially). Without Docker they are skipped.

Each worker answers through the real DRF exception handler, so what is checked is the HTTP status and ``error_code``
the public route will give. Run serially (no ``-n``).
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
import redis
from django.core.cache import CacheHandler
from django.test.utils import override_settings
from django.utils import timezone
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory
from rest_framework.views import APIView

from gaodcore_project.cache_backends import TRANSPORTS_CACHE_ALIAS, build_caches
from gaodcore_transports import sources
from gaodcore_transports.tests.conftest import ARRIVALS_URI, STOPS_URI
from gaodcore_transports.tests.consorcio_mock import arrival
from gaodcore_transports.views import zaragoza

WORKER = Path(__file__).with_name("proc_worker.py")
PROCESSES = 8
READY_TIMEOUT = 60
RESULT_TIMEOUT = 60


@pytest.fixture(scope="module")
def redis_url():
    """URL of a ``redis:7-alpine`` container started once for the module."""
    try:
        import docker  # pylint: disable=import-outside-toplevel
        from testcontainers.core.container import DockerContainer  # pylint: disable=import-outside-toplevel
        from testcontainers.core.waiting_utils import wait_for_logs  # pylint: disable=import-outside-toplevel

        docker.from_env().ping()
    except Exception as err:  # pylint: disable=broad-except
        pytest.skip(f"Docker is not available, cannot start a Redis container: {err}")
    container = DockerContainer("redis:7-alpine").with_exposed_ports(6379)
    try:
        container.start()
    except Exception as err:  # pylint: disable=broad-except
        pytest.skip(f"Could not start the redis:7-alpine container: {err}")
    try:
        wait_for_logs(container, "Ready to accept connections", timeout=60)
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
    finally:
        container.stop()


@pytest.fixture
def shared(redis_url):
    """The ``transports`` cache seen from the pytest process (for seeding), empty before and after each test."""
    client = redis.Redis.from_url(redis_url)
    client.flushdb()
    yield CacheHandler(build_caches(redis_url))[TRANSPORTS_CACHE_ALIAS]
    client.flushdb()
    client.close()


@pytest.fixture
def mock(consorcio):
    return consorcio


def run_workers(redis_url, consorcio, internal, jobs):
    """Start one process per job, release them all at the same instant and return their JSON answers.

    A job is ``(mode, argument)``. Every process finishes its start-up (Django, Redis connection) and says READY; the
    start line is written to all of them only then, so the requests overlap within milliseconds.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("POSTGRES_")}
    env.setdefault("DJANGO_SETTINGS_MODULE", "gaodcore_project.test_settings")
    processes = []
    try:
        for mode, argument in jobs:
            command = [sys.executable, str(WORKER), redis_url, consorcio.base_url, json.dumps(internal), mode]
            if argument is not None:
                command.append(argument)
            processes.append(subprocess.Popen(  # pylint: disable=consider-using-with
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
            ))
        deadline = time.monotonic() + READY_TIMEOUT
        for process in processes:
            line = process.stdout.readline()
            assert line.strip() == "READY", f"worker did not start: {line!r} {process.stderr.read()[-2000:]}"
            assert time.monotonic() < deadline, "workers took too long to start"
        for process in processes:
            process.stdin.write("go\n")
            process.stdin.flush()
        results = []
        for process in processes:
            out, err = process.communicate(timeout=RESULT_TIMEOUT)
            assert process.returncode == 0, f"worker failed: {err[-2000:]}"
            results.append(json.loads(out.strip().splitlines()[-1]))
        return results
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()


def codes(results):
    """``status`` or ``status:error_code`` of every answer, sorted."""
    return sorted(f"{r['status']}:{r['error_code']}" if r["error_code"] else str(r["status"]) for r in results)


def count(results, status, error_code=None):
    return sum(1 for r in results if r["status"] == status and r["error_code"] == error_code)


def stops_rows(total):
    return [{"stop_id": str(100 + index), "stop_name": f"Stop {index}", "lat": 41.6, "lon": -0.9}
            for index in range(total)]


def seed_stops(shared, rows, age_seconds):
    shared.set(sources.STOPS_KEY, {"fetched_at": timezone.now() - timedelta(seconds=age_seconds), "rows": rows}, None)


def test_stops_without_a_copy_are_downloaded_once(redis_url, shared, mock):
    mock.delays["stops"] = 1.0
    results = run_workers(redis_url, mock, {}, [("stops", None)] * PROCESSES)
    assert mock.hits == {"stops": 1}
    assert sum(1 for r in results if (r["state"] or "") == "stops:downloaded:age=0") == 1
    assert all(r["rows"] == len(mock.stops) for r in results if r["status"] == 200)
    # Whoever did not get the lock fails at once; a late one may find the fresh copy.
    assert set(codes(results)) <= {"200", "503:DATA_NOT_READY"}, codes(results)
    assert count(results, 503, "DATA_NOT_READY") >= 1
    assert all(r["retry_after"] == "10" for r in results if r["status"] == 503)
    assert shared.get(sources.STOPS_LOCK_KEY) is None  # released


def test_expired_stops_copy_is_refreshed_once_and_everybody_gets_rows(redis_url, shared, mock):
    seed_stops(shared, stops_rows(2), age_seconds=4000)  # older than 3600 s, within the stale margin
    mock.delays["stops"] = 1.0
    results = run_workers(redis_url, mock, {}, [("stops", None)] * PROCESSES)
    assert mock.hits == {"stops": 1}
    assert codes(results) == ["200"] * PROCESSES
    assert sum(1 for r in results if (r["state"] or "") == "stops:refreshed:age=0") == 1
    assert {r["rows"] for r in results} <= {2, len(mock.stops)}  # the old copy (2) or the refreshed one
    assert len(shared.get(sources.STOPS_KEY)["rows"]) == len(mock.stops)


def test_arrivals_of_one_stop_are_called_once(redis_url, shared, mock):
    seed_stops(shared, mock.stops, age_seconds=0)
    mock.delays["arrival_time/40"] = 1.0
    results = run_workers(redis_url, mock, {}, [("arrivals", "40")] * PROCESSES)
    assert mock.hits == {"arrival_time/40": 1}
    assert sum(1 for r in results if (r["state"] or "").startswith("arrivals:miss:ok:code=40")) == 1
    assert set(codes(results)) <= {"200", "503:UPSTREAM_BUSY"}, codes(results)
    assert count(results, 503, "UPSTREAM_BUSY") >= 1
    assert all(r["rows"] == 2 for r in results if r["status"] == 200)
    assert shared.get(sources.ARRIVAL_KEY.format(code="40")) is not None


def test_simultaneous_calls_for_different_stops_stay_within_the_ceiling(redis_url, shared, mock):
    stops = [str(200 + index) for index in range(PROCESSES)]
    mock.arrivals.update({code: {"nombre": code, "arrival_time": [arrival("1", "00:01:00")]} for code in stops})
    mock.delay = 1.0
    results = run_workers(redis_url, mock, {"arrivals_max_concurrency": 2}, [("arrivals", code) for code in stops])
    calls = mock.total_hits("arrival_time/")
    assert 1 <= calls <= 2 * PROCESSES
    assert mock.max_in_flight <= 2
    assert count(results, 200) == calls  # every answer with rows is a call of its own: nothing else is cached
    assert count(results, 503, "UPSTREAM_BUSY") == PROCESSES - calls >= 1, codes(results)
    assert all(count_ == 1 for count_ in mock.hits.values())


def test_aggregate_is_rebuilt_once(redis_url, shared, mock):
    rows = stops_rows(8)
    seed_stops(shared, rows, age_seconds=0)
    mock.arrivals.update({r["stop_id"]: {"nombre": "x", "arrival_time": [arrival("1", "00:01:00")]} for r in rows})
    mock.delay = 0.3
    results = run_workers(redis_url, mock, {}, [("aggregate", None)] * PROCESSES)
    assert mock.hits == {f"arrival_time/{r['stop_id']}": 1 for r in rows}
    assert sum(1 for r in results if (r["state"] or "").startswith("arrivals:all:rebuilt")) == 1
    assert set(codes(results)) <= {"200", "503:UPSTREAM_BUSY"}, codes(results)
    assert count(results, 503, "UPSTREAM_BUSY") >= 1
    assert all(r["rows"] == len(rows) for r in results if r["status"] == 200)
    assert shared.get(sources.ALL_LOCK_KEY) is None


# --------------------------------------------------------------------------------------------------------------------
# Redis down
# --------------------------------------------------------------------------------------------------------------------


@pytest.fixture
def redis_down(internal_config):
    """``CACHES`` pointing at a port where nothing listens (the connection is refused at once)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with override_settings(CACHES=build_caches(f"redis://127.0.0.1:{port}/0")):
        yield


def answer_for(uri, filters):
    class SourceView(APIView):
        authentication_classes = []
        permission_classes = []

        def get(self, request):
            opened = sources.open_for_request(uri, filters, {}, [], [])
            opened.resource.engine.dispose()
            return Response({"state": opened.description})

    return SourceView.as_view()(APIRequestFactory().get("/source"))


@pytest.mark.parametrize(
    "uri, filters",
    [(STOPS_URI, {}), (ARRIVALS_URI, {"stop_id": "40"}), (ARRIVALS_URI, {})],
    ids=["2187-stops", "2190-one-stop", "2190-aggregate"],
)
def test_redis_down_is_503_data_not_ready_never_500(redis_down, mock, self_calls, uri, filters):
    response = answer_for(uri, filters)
    assert response.status_code == 503
    assert response.data["error_code"] == "DATA_NOT_READY"
    assert response["Retry-After"]
    assert mock.hits == {}  # nothing is called without the shared state: no lock, no way to bound the calls


@pytest.mark.parametrize("view, patched", [("lines", "get_lines"), ("stops", "_get_stops")], ids=["lines", "stops"])
def test_cache_page_views_answer_without_cache_when_redis_is_down(
    redis_down, db, client, monkeypatch, view, patched
):
    calls = []

    def upstream():
        calls.append(1)
        return [{"id": "1"}]

    monkeypatch.setattr(zaragoza, patched, upstream)
    for _ in range(2):
        response = client.get(f"/GA_OD_Core/gaodcore-transports/zaragoza/{view}")
        assert response.status_code == 200
        assert response.json() == [{"id": "1"}]
    assert len(calls) == 2  # not cached: the second request called the upstream again
