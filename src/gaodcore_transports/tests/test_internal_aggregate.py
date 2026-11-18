"""Resource 2190 without a stop (``internal://transports/zaragoza/arrival_time``): the aggregate of every stop.

Built on demand in the request that gets ``lock:all`` and shared through the ``transports`` cache (memory of the process
in tests). The Consorcio is the local mock: the stops copy has the stops 40, 55 and 1011 (55 has no arrivals).
"""

import csv
import io
import threading
from datetime import timedelta
from unittest import mock

import pytest
from django.core.cache import cache
from django.utils import timezone
from pydantic import ValidationError
from redis.exceptions import ConnectionError as RedisConnectionError

from gaodcore_project.config import ZaragozaInternalSources
from gaodcore_transports import sources
from gaodcore_transports.tests.consorcio_mock import arrival
from gaodcore_transports.tests.test_internal_arrivals import answer, open_arrivals, read, rows_of
from gaodcore_transports.views import zaragoza

CSV_URL = "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time.csv"


@pytest.fixture(autouse=True)
def arrivals_source(transports_cache, internal_config, self_calls):
    """Empty shared cache, local API, configuration and the guards against self-calls."""
    return transports_cache


def age_all(cache_, seconds):
    value = cache_.get(sources.ALL_KEY)
    value["fetched_at"] = timezone.now() - timedelta(seconds=seconds)
    cache_.set(sources.ALL_KEY, value, 3600)


def calls(consorcio):
    return {path: count for path, count in consorcio.hits.items() if path.startswith("arrival_time/")}


def test_rows_are_those_the_arrival_time_view_and_the_csv_renderer_produce(consorcio, monkeypatch, client, db):
    async def fake_download_async(_session, url, auth=None):
        return consorcio.arrivals.get(url.rsplit("/", 1)[1], {"arrival_time": []})

    monkeypatch.setattr(zaragoza, "_get_stops", lambda: consorcio.stops)
    monkeypatch.setattr(zaragoza, "download_async", fake_download_async)
    cache.clear()
    response = client.get(CSV_URL)
    assert response.status_code == 200
    table = list(csv.reader(io.StringIO(response.content.decode("utf-8"))))

    opened = open_arrivals({})
    try:
        description = opened.description
        columns = [c.name for c in opened.resource.model.columns]
        rows = rows_of(opened)
    finally:
        opened.resource.engine.dispose()
    assert description == "arrivals:all:rebuilt:age=0:failures=0"
    assert columns == table[0]
    assert [[row[name] for name in columns] for row in rows] == table[1:]
    assert len(rows) == 3  # stop 40 (2), stop 55 (none) and stop 1011 (1)
    assert {row["stop_id"] for row in rows} == {"40", "1011"}
    assert all(row["stop_id"] == row["cod_parada"] for row in rows)


def test_fresh_aggregate_is_served_without_calling(consorcio):
    assert read({})[0] == "arrivals:all:rebuilt:age=0:failures=0"
    consorcio.reset_counters()
    description, rows = read({"id_linea": "111"})
    assert description.startswith("arrivals:all:hit:age=")
    assert len(rows) == 3  # the whole aggregate: the generic code applies the filters afterwards
    assert consorcio.total_hits() == 0


def test_rebuild_calls_every_stop_of_the_copy_once_within_the_concurrency(consorcio):
    for index in range(10):
        consorcio.stops.append({"stop_id": str(2000 + index), "stop_name": "x", "lat": 1, "lon": 1})
    consorcio.delay = 0.05
    read({})
    assert len(calls(consorcio)) == 13 and set(calls(consorcio).values()) == {1}
    assert consorcio.hits["stops"] == 1
    assert 1 < consorcio.max_in_flight <= 5


def test_expired_aggregate_is_rebuilt_by_one_request_and_the_rest_serve_the_previous_one(arrivals_source, consorcio):
    read({})
    age_all(arrivals_source, 301)
    consorcio.arrivals["40"]["arrival_time"].append(arrival("113", "00:30:00"))
    consorcio.reset_counters()
    release, entered = threading.Event(), threading.Event()

    def hold(path):
        if path.startswith("arrival_time/"):
            entered.set()
            release.wait(5)

    consorcio.on_request = hold
    results = {}
    rebuilder = threading.Thread(target=lambda: results.update(first=read({})))
    rebuilder.start()
    assert entered.wait(5)
    others = [read({}) for _ in range(3)]
    assert all(d.startswith("arrivals:all:stale-busy:age=30") and len(r) == 3 for d, r in others)
    release.set()
    rebuilder.join(10)
    assert results["first"][0] == "arrivals:all:rebuilt:age=0:failures=0"
    assert len(results["first"][1]) == 4
    assert calls(consorcio) == {"arrival_time/40": 1, "arrival_time/55": 1, "arrival_time/1011": 1}
    assert read({})[0].startswith("arrivals:all:hit")


def test_aggregate_past_the_stale_margin_is_not_served(arrivals_source, consorcio):
    read({})
    age_all(arrivals_source, 300 + 600 + 1)
    assert arrivals_source.add(sources.ALL_LOCK_KEY, "someone else", 30)
    response = answer({})
    assert response.status_code == 503 and response.data["error_code"] == "UPSTREAM_BUSY"


def test_without_an_aggregate_and_the_lock_taken_the_answer_is_503_upstream_busy(arrivals_source, consorcio):
    assert arrivals_source.add(sources.ALL_LOCK_KEY, "someone else", 30)
    response = answer({})
    assert response.status_code == 503
    assert response.data["error_code"] == "UPSTREAM_BUSY"
    assert response["Retry-After"] == "5"
    assert consorcio.total_hits() == 0
    assert arrivals_source.get(sources.ALL_LOCK_KEY) == "someone else"  # not ours: not released


def test_the_lock_is_released_after_the_rebuild(arrivals_source, consorcio):
    read({})
    assert arrivals_source.get(sources.ALL_LOCK_KEY) is None


def test_one_stop_failing_does_not_fail_the_aggregate_and_is_counted(arrivals_source, consorcio, caplog):
    consorcio.failures["arrival_time/40"] = "500"
    with caplog.at_level("INFO", logger=sources.logger.name):
        description, rows = read({})
    assert description == "arrivals:all:rebuilt:age=0:failures=1"
    assert {row["stop_id"] for row in rows} == {"1011"}
    lines = [r.getMessage() for r in caplog.records if ":all " in r.getMessage()]
    assert len(lines) == 1 and "stops=3" in lines[0] and "failures=1" in lines[0]
    assert read({})[0] == "arrivals:all:hit:age=0:failures=1"  # published, served from the cache


@pytest.mark.parametrize("failure", ["invalid", "cut", "500"])
def test_every_kind_of_failure_of_a_stop_is_tolerated(consorcio, failure):
    consorcio.failures["arrival_time/1011"] = failure
    description, rows = read({})
    assert description.endswith("failures=1") and {row["stop_id"] for row in rows} == {"40"}


def test_unknown_stop_and_stop_without_arrivals_are_not_failures(consorcio):
    consorcio.arrivals.pop("1011")  # the API answers "Parada Desconocida"
    description, rows = read({})
    assert description.endswith("failures=0") and {row["stop_id"] for row in rows} == {"40"}


@pytest.mark.parametrize(
    ("failure", "status", "code"),
    [("500", 503, "CONNECTION_UNAVAILABLE"), ("invalid", 502, "BAD_GATEWAY")],
    ids=["unavailable", "invalid"],
)
def test_every_stop_failing_without_a_previous_aggregate_is_an_error_and_is_not_retried_at_once(
    arrivals_source, consorcio, failure, status, code
):
    for stop in consorcio.stops:
        consorcio.failures[f"arrival_time/{stop['stop_id']}"] = failure
    response = answer({})
    assert (response.status_code, response.data["error_code"]) == (status, code)
    assert int(response["Retry-After"]) >= 1
    assert arrivals_source.get(sources.ALL_KEY) is None
    before = calls(consorcio)
    assert len(before) == 3
    response = answer({})
    assert response.status_code == status
    assert calls(consorcio) == before  # backing off: no new calls
    assert arrivals_source.get(sources.ALL_LOCK_KEY) is None


def test_every_stop_failing_serves_the_previous_aggregate_within_the_margin(arrivals_source, consorcio):
    read({})
    age_all(arrivals_source, 400)
    for stop in consorcio.stops:
        consorcio.failures[f"arrival_time/{stop['stop_id']}"] = "500"
    description, rows = read({})
    assert description == "arrivals:all:stale-failed:age=400:failures=0"
    assert len(rows) == 3
    consorcio.reset_counters()
    assert read({})[0].startswith("arrivals:all:stale-failed")
    assert consorcio.total_hits() == 0
    # Once the backoff is over (and the stop recovered) it is rebuilt again.
    arrivals_source.delete(sources.ALL_ERROR_KEY)
    consorcio.failures.clear()
    assert read({})[0] == "arrivals:all:rebuilt:age=0:failures=0"


def test_without_a_stops_copy_the_answer_is_503_data_not_ready(consorcio):
    consorcio.failures["stops"] = "500"
    response = answer({})
    assert response.status_code == 503 and response.data["error_code"] == "DATA_NOT_READY"
    assert response["Retry-After"] == "10"
    assert calls(consorcio) == {}


def test_without_a_stops_copy_the_previous_aggregate_is_served(arrivals_source, consorcio):
    read({})
    age_all(arrivals_source, 400)
    arrivals_source.delete(sources.STOPS_KEY)
    consorcio.failures["stops"] = "500"
    assert read({})[0].startswith("arrivals:all:stale-nostops:age=400")


@pytest.mark.parametrize("operation", ["get", "add"])
def test_redis_error_answers_503_data_not_ready_never_500(arrivals_source, consorcio, operation):
    with mock.patch.object(arrivals_source, operation, side_effect=RedisConnectionError("down")):
        response = answer({})
    assert response.status_code == 503 and response.data["error_code"] == "DATA_NOT_READY"
    assert response["Retry-After"] == "5"
    assert consorcio.total_hits() == 0


def test_redis_error_while_publishing_still_answers_with_the_rebuilt_rows(arrivals_source, consorcio):
    real_set = arrivals_source.set

    def set_(key, *args, **kwargs):
        if key == sources.ALL_KEY:
            raise RedisConnectionError("down")
        return real_set(key, *args, **kwargs)

    with mock.patch.object(arrivals_source, "set", side_effect=set_):
        description, rows = read({})
    assert description.startswith("arrivals:all:rebuilt") and len(rows) == 3


def test_queries_with_a_stop_never_touch_the_aggregate(arrivals_source, consorcio):
    with mock.patch.object(arrivals_source, "add", wraps=arrivals_source.add) as spy:
        read({"cod_parada": 40})
        read({"stop_id": "1011"})
    assert all(c.args[0].startswith("lock:arrival:") for c in spy.call_args_list)
    assert arrivals_source.get(sources.ALL_KEY) is None
    assert "stops" not in consorcio.hits


def test_the_aggregate_does_not_use_the_cache_of_one_stop(arrivals_source, consorcio):
    read({})
    assert arrivals_source.get(sources.ARRIVAL_KEY.format(code="40")) is None


def test_value_is_cached_with_its_ttl_plus_the_stale_margin(arrivals_source, consorcio):
    with mock.patch.object(arrivals_source, "set", wraps=arrivals_source.set) as spy:
        read({})
    stored = {c.args[0]: c for c in spy.call_args_list}[sources.ALL_KEY]
    assert stored.args[2] == 300 + 600


def test_rebuild_stops_calling_before_the_lock_expires(consorcio, internal_config):
    internal_config.set(arrivals_aggregate_lock_seconds=9, arrivals_timeout_seconds=3)  # deadline 4 s after the start
    consorcio.delay = 1.5
    for index in range(20):
        consorcio.stops.append({"stop_id": str(2000 + index), "stop_name": "x", "lat": 1, "lon": 1})
    description, rows = read({})
    assert 0 < int(description.rsplit("failures=", 1)[1]) < 23  # the stops left without time are failures
    assert len(rows) >= 1


def test_the_lock_must_outlive_a_call():
    with pytest.raises(ValidationError, match="arrivals_aggregate_lock_seconds"):
        ZaragozaInternalSources(arrivals_aggregate_lock_seconds=8, arrivals_timeout_seconds=3)
    assert ZaragozaInternalSources().arrivals_aggregate_lock_seconds == 60
