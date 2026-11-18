"""Resource 2190 (``internal://transports/zaragoza/arrival_time``) served in process, stop by stop.

The state lives in the ``transports`` cache (memory of the process in tests); the Consorcio is a local mock whose
numbering is that of the real API: 510 has arrivals but is not in ``/stops``, 1011 is a different place in each.
Answers are checked through the real DRF exception handler, with the same mapping of unknown columns as the public routes.
"""

import threading
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest import mock

import pytest
from django.utils import timezone
from redis.exceptions import ConnectionError as RedisConnectionError
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory
from rest_framework.views import APIView

import connectors
from gaodcore.views import _get_data_public_error
from gaodcore_transports import sources
from gaodcore_transports.tests.conftest import ARRIVALS_URI
from gaodcore_transports.tests.consorcio_mock import arrival

COLUMNS = [
    "TR", "bus", "cod_parada", "departure_time", "direction", "id", "id_linea", "inc", "name", "remaining_time",
    "route", "stop_id", "url",
]


@pytest.fixture(autouse=True)
def arrivals_source(transports_cache, internal_config, self_calls):
    """Empty shared cache, local API, configuration and the guards against self-calls."""
    return transports_cache


def open_arrivals(filters=None, like=None, fields=(), sort=()):
    return sources.open_for_request(ARRIVALS_URI, filters or {}, like or {}, list(fields), list(sort))


def rows_of(opened):
    with opened.resource.engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(opened.resource.model.select())]  # pylint: disable=protected-access


def read(filters=None, **kwargs):
    """Open and read in the calling thread (the in-memory SQLite table belongs to the thread that built it)."""
    opened = open_arrivals(filters, **kwargs)
    try:
        return opened.description, rows_of(opened)
    finally:
        opened.resource.engine.dispose()


def answer(filters, **kwargs):
    """What the public routes will do: open the source (``kwargs``: like, fields, sort) and let DRF answer."""

    class ArrivalsView(APIView):
        authentication_classes = []
        permission_classes = []

        def get(self, request):
            opened = _get_data_public_error(open_arrivals, filters, **kwargs)
            opened.resource.engine.dispose()
            return Response({"state": opened.description})

    return ArrivalsView.as_view()(APIRequestFactory().get("/arrivals"))


def age_value(cache, code, seconds):
    key = sources.ARRIVAL_KEY.format(code=code)
    value = cache.get(key)
    value["fetched_at"] = timezone.now() - timedelta(seconds=seconds)
    cache.set(key, value, 3600)


def test_columns_contract_matches_the_csv_read_by_the_old_connector():
    assert list(sources.ARRIVAL_COLUMNS) == COLUMNS


def test_code_absent_from_stops_is_answered_from_arrival_time(consorcio):
    description, rows = read({"cod_parada": 510})
    assert description == "arrivals:miss:ok:code=510:age=0"
    assert len(rows) == 1
    assert list(rows[0]) == COLUMNS
    assert rows[0]["stop_id"] == rows[0]["cod_parada"] == "510"
    assert rows[0]["remaining_time"] == "00:03:05"
    assert all(isinstance(value, str) for value in rows[0].values())  # text, as read from the CSV
    assert consorcio.hits == {"arrival_time/510": 1}


def test_numbering_collision_uses_only_arrival_time(consorcio):
    _, rows = read({"stop_id": "1011"})
    assert [row["id_linea"] for row in rows] == ["601"]  # Lugarico de Cerdán's line, not the airport's
    assert "stops" not in consorcio.hits


def test_the_call_goes_to_the_consorcio_with_the_code(consorcio):
    read({"stop_id": 40})
    assert list(consorcio.hits) == ["arrival_time/40"]


@pytest.mark.parametrize(
    ("code", "status", "ttl"),
    [("720", "unknown", 300 + 30), ("55", "empty", 60 + 30), ("40", "ok", 60 + 30)],
    ids=["unknown", "empty", "with-arrivals"],
)
def test_value_is_cached_with_its_ttl_plus_the_stale_margin(arrivals_source, consorcio, code, status, ttl):
    with mock.patch.object(arrivals_source, "set", wraps=arrivals_source.set) as spy:
        read({"cod_parada": code})
    calls = {c.args[0]: c for c in spy.call_args_list}
    stored = calls[sources.ARRIVAL_KEY.format(code=code)]
    assert stored.args[1]["status"] == status
    assert stored.args[2] == ttl


def test_unknown_stop_and_stop_without_arrivals_are_empty_and_cached(consorcio):
    assert read({"cod_parada": 720})[1] == []
    assert read({"cod_parada": 55})[1] == []
    assert read({"cod_parada": 720})[0].startswith("arrivals:hit:unknown")
    assert read({"stop_id": 55})[0].startswith("arrivals:hit:empty")
    assert consorcio.hits == {"arrival_time/720": 1, "arrival_time/55": 1}
    # An empty answer keeps the columns: filters, fields and sort on them are valid.
    opened = open_arrivals({"cod_parada": 55, "id_linea": "1"}, fields=["name"], sort=[SimpleNamespace(field="name")])
    assert [c.name for c in opened.resource.model.columns] == COLUMNS
    opened.resource.engine.dispose()


def test_equivalent_filters_share_one_call(consorcio):
    first = read({"cod_parada": 40})[1]
    assert len(first) == 2
    assert read({"cod_parada": "40"})[1] == first
    assert read({"stop_id": 40})[1] == first
    assert read({"stop_id": "40", "cod_parada": 40})[1] == first
    assert consorcio.hits == {"arrival_time/40": 1}


def test_fresh_value_is_refreshed_after_its_ttl(arrivals_source, consorcio):
    read({"cod_parada": 40})
    consorcio.arrivals["40"]["arrival_time"].append(arrival("113", "00:30:00"))
    assert len(read({"cod_parada": 40})[1]) == 2  # cached
    age_value(arrivals_source, "40", 61)
    assert len(read({"cod_parada": 40})[1]) == 3
    assert consorcio.hits == {"arrival_time/40": 2}


def test_id_with_a_stop_is_one_more_filter():
    plan = sources.plan_arrivals({"cod_parada": 40, "id": "0"}, {}, [], [])
    assert plan == sources.ArrivalPlan(code="40")
    assert len(read({"cod_parada": 40, "id": "0"})[1]) == 2  # the rows are those of the stop; the filter is generic


@pytest.mark.parametrize(
    "filters",
    [{"id": 40}, {"id": "40"}, {"id": 40, "id_linea": "111"}, {"id": 40, "stop_id": [40, 55]},
     {"id": 40, "cod_parada": {"$gt": 1}}],
    ids=["int", "text", "other-column", "list-of-stops", "operator"],
)
def test_id_without_a_stop_is_400_invalid_field_without_calling(consorcio, filters):
    response = answer(filters)
    assert response.status_code == 400
    assert response.data["error_code"] == "VALIDATION_ERROR"
    assert [e["code"] for e in response.data["errors"]["non_field_errors"]] == ["INVALID_FIELD"]  # as an unknown column
    assert "stop_id or cod_parada" in str(response.data)
    assert consorcio.total_hits() == 0


@pytest.mark.parametrize(
    ("filters", "kwargs", "code"),
    [
        ({"parada": 720}, {}, "INVALID_FIELD"),
        ({"cod_parada": 40, "parada": 40}, {}, "INVALID_FIELD"),
        ({"cod_parada": 40}, {"fields": ["nope"]}, "INVALID_FIELD"),
        ({"cod_parada": 40}, {"sort": [SimpleNamespace(field="nope")]}, "INVALID_SORT"),
        ({"cod_parada": 40}, {"like": {"nope": "x"}}, "INVALID_FIELD"),
    ],
    ids=["parada", "parada-with-stop", "field", "sort", "like"],
)
def test_invalid_columns_are_400_without_calling(consorcio, filters, kwargs, code):
    response = answer(filters, **kwargs)
    assert response.status_code == 400
    assert response.data["error_code"] == "VALIDATION_ERROR"
    assert [e["code"] for e in response.data["errors"]["non_field_errors"]] == [code]
    assert consorcio.total_hits() == 0


@pytest.mark.parametrize(
    "filters",
    [{"cod_parada": "abc"}, {"cod_parada": "040"}, {"cod_parada": 40.0}, {"cod_parada": True}, {"cod_parada": 0},
     {"cod_parada": None}, {"stop_id": 40, "cod_parada": 55}, {"cod_parada": "1" * 7}],
    ids=["text", "leading-zero", "float", "bool", "zero", "null", "two-stops", "too-long"],
)
def test_equalities_no_row_can_match_are_empty_without_calling(consorcio, filters):
    description, rows = read(filters)
    assert rows == [] and description == "arrivals:none:impossible"
    assert consorcio.total_hits() == 0


@pytest.mark.parametrize(
    "filters", [{}, {"id_linea": "111"}, {"stop_id": [40, 55]}, {"cod_parada": {"$gt": 1}}],
    ids=["no-filter", "other-column", "list-of-stops", "operator"],
)
def test_query_without_a_stop_is_the_aggregate_hook(consorcio, filters):
    with mock.patch.object(sources, "open_all_arrivals", wraps=sources.open_all_arrivals) as hook:
        opened = open_arrivals(filters)
    opened.resource.engine.dispose()
    hook.assert_called_once()
    assert opened.description.startswith("arrivals:all:rebuilt")


def test_network_error_is_a_503_not_an_empty_success(arrivals_source, consorcio):
    consorcio.failures["arrival_time/40"] = "500"
    response = answer({"cod_parada": 40})
    assert response.status_code == 503
    assert response.data["error_code"] == "CONNECTION_UNAVAILABLE"
    assert int(response["Retry-After"]) >= 1
    assert arrivals_source.get(sources.ARRIVAL_KEY.format(code="40")) is None  # nothing cached as a success
    # Within the error TTL: no new call.
    assert answer({"cod_parada": 40}).status_code == 503
    assert consorcio.hits == {"arrival_time/40": 1}
    assert arrivals_source.get(sources.ARRIVAL_LOCK_KEY.format(code="40")) is None
    # After it, the stop is called again and answers.
    consorcio.failures.clear()
    arrivals_source.delete(sources.ARRIVAL_ERROR_KEY.format(code="40"))
    assert len(read({"cod_parada": 40})[1]) == 2


def test_error_is_remembered_for_its_ttl(arrivals_source, consorcio):
    consorcio.failures["arrival_time/40"] = "500"
    with mock.patch.object(arrivals_source, "set", wraps=arrivals_source.set) as spy:
        answer({"cod_parada": 40})
    stored = {c.args[0]: c for c in spy.call_args_list}[sources.ARRIVAL_ERROR_KEY.format(code="40")]
    assert stored.args[1]["kind"] == "error" and stored.args[2] == 10


@pytest.mark.parametrize("failure", ["invalid", "cut"])
def test_invalid_or_cut_answer_is_a_502_or_503(consorcio, failure):
    consorcio.failures["arrival_time/40"] = failure
    response = answer({"cod_parada": 40})
    assert response.status_code == (502 if failure == "invalid" else 503)
    assert response.data["error_code"] == ("BAD_GATEWAY" if failure == "invalid" else "CONNECTION_UNAVAILABLE")
    assert int(response["Retry-After"]) >= 1


@pytest.mark.parametrize(
    "body",
    [{"nombre": "x", "arrival_time": "not a list"}, {"nombre": "x", "arrival_time": [1]}, ["not", "an", "object"]],
    ids=["text", "not-objects", "not-an-object"],
)
def test_invalid_shape_is_a_502(consorcio, body):
    consorcio.arrivals["40"] = body
    assert answer({"cod_parada": 40}).status_code == 502


def test_slow_api_is_cut_at_the_call_bound(consorcio, internal_config):
    internal_config.set(arrivals_timeout_seconds=1)
    consorcio.delays["arrival_time/40"] = 5
    started = time.monotonic()
    assert answer({"cod_parada": 40}).status_code == 503
    assert time.monotonic() - started < 2.5


def test_trickling_api_is_cut_at_the_call_bound(consorcio, internal_config):
    internal_config.set(arrivals_timeout_seconds=1)
    consorcio.failures["arrival_time/40"] = "hang"
    consorcio.hang_seconds = 5
    started = time.monotonic()
    assert answer({"cod_parada": 40}).status_code == 503
    assert time.monotonic() - started < 2.5


def test_locks_expire_after_the_call_bound(arrivals_source, consorcio, internal_config):
    internal_config.set(arrivals_timeout_seconds=3)
    with mock.patch.object(arrivals_source, "add", wraps=arrivals_source.add) as spy:
        read({"cod_parada": 40})
    assert [(c.args[0], c.args[2]) for c in spy.call_args_list] == [
        (sources.ARRIVAL_LOCK_KEY.format(code="40"), 8),
        (sources.ARRIVAL_SLOT_KEY.format(index=0), 8),
    ]


def test_expired_value_is_reused_briefly_when_the_call_fails(arrivals_source, consorcio):
    assert len(read({"cod_parada": 40})[1]) == 2
    age_value(arrivals_source, "40", 70)
    consorcio.failures["arrival_time/40"] = "500"
    description, rows = read({"cod_parada": 40})  # 70 s < 60 + 30: stale value
    assert description.startswith("arrivals:stale:ok") and len(rows) == 2
    arrivals_source.delete(sources.ARRIVAL_ERROR_KEY.format(code="40"))
    age_value(arrivals_source, "40", 95)
    assert answer({"cod_parada": 40}).status_code == 503  # past the stale margin


def test_recent_error_serves_the_stale_value_without_calling(arrivals_source, consorcio):
    read({"cod_parada": 40})
    age_value(arrivals_source, "40", 70)
    consorcio.failures["arrival_time/40"] = "500"
    read({"cod_parada": 40})
    consorcio.reset_counters()
    assert read({"cod_parada": 40})[0].startswith("arrivals:stale:ok")
    assert consorcio.total_hits() == 0


def test_lock_of_the_stop_held_by_another_serves_the_stale_value(arrivals_source, consorcio):
    read({"cod_parada": 40})
    age_value(arrivals_source, "40", 70)
    consorcio.reset_counters()
    lock = sources.ARRIVAL_LOCK_KEY.format(code="40")
    assert arrivals_source.add(lock, "someone else", 30)
    assert read({"cod_parada": 40})[0].startswith("arrivals:stale:ok")
    assert consorcio.total_hits() == 0
    assert arrivals_source.get(lock) == "someone else"  # not ours to release


def test_lock_of_the_stop_held_by_another_without_a_value_is_503_upstream_busy(arrivals_source, consorcio):
    assert arrivals_source.add(sources.ARRIVAL_LOCK_KEY.format(code="40"), "someone else", 30)
    response = answer({"cod_parada": 40})
    assert response.status_code == 503
    assert response.data["error_code"] == "UPSTREAM_BUSY"
    assert response["Retry-After"] == "2"
    assert consorcio.total_hits() == 0


def test_with_both_slots_taken_the_stale_value_is_served_or_503_upstream_busy(arrivals_source, consorcio):
    for index in range(2):
        assert arrivals_source.add(sources.ARRIVAL_SLOT_KEY.format(index=index), "someone else", 30)
    response = answer({"cod_parada": 40})
    assert response.status_code == 503 and response.data["error_code"] == "UPSTREAM_BUSY"
    assert response["Retry-After"] == "2"
    assert arrivals_source.get(sources.ARRIVAL_LOCK_KEY.format(code="40")) is None  # the stop lock is given back
    assert consorcio.total_hits() == 0
    # With an expired value still within the margin it is served instead.
    arrivals_source.delete(sources.ARRIVAL_SLOT_KEY.format(index=1))
    read({"cod_parada": 40})
    age_value(arrivals_source, "40", 70)
    arrivals_source.add(sources.ARRIVAL_SLOT_KEY.format(index=1), "someone else", 30)
    consorcio.reset_counters()
    assert read({"cod_parada": 40})[0].startswith("arrivals:stale:ok")
    assert consorcio.total_hits() == 0


def test_a_free_slot_is_used_when_the_first_is_taken(arrivals_source, consorcio):
    assert arrivals_source.add(sources.ARRIVAL_SLOT_KEY.format(index=0), "someone else", 30)
    assert len(read({"cod_parada": 40})[1]) == 2
    assert arrivals_source.get(sources.ARRIVAL_SLOT_KEY.format(index=0)) == "someone else"
    assert arrivals_source.get(sources.ARRIVAL_SLOT_KEY.format(index=1)) is None


def test_concurrent_requests_for_one_stop_make_one_call(consorcio):
    consorcio.delays["arrival_time/40"] = 0.5
    results = []

    def request():
        results.append(answer({"cod_parada": 40}).status_code)

    first = threading.Thread(target=request)
    first.start()
    time.sleep(0.2)  # the first one is calling
    others = [threading.Thread(target=request) for _ in range(4)]
    for thread in others:
        thread.start()
    for thread in [first, *others]:
        thread.join(10)
    assert sorted(results) == [200, 503, 503, 503, 503]  # the others had no previous value
    assert consorcio.hits == {"arrival_time/40": 1}


def test_calls_for_different_stops_are_capped_at_two_at_once(consorcio):
    for code in ("40", "55", "510"):
        consorcio.delays[f"arrival_time/{code}"] = 0.5
    results = {}

    def request(code):
        response = answer({"cod_parada": code})
        results[code] = (response.status_code, response.data.get("error_code"))

    threads = [threading.Thread(target=request, args=(code,)) for code in ("40", "55", "510")]
    for thread in threads:
        thread.start()
        time.sleep(0.1)
    for thread in threads:
        thread.join(10)
    assert consorcio.max_in_flight == 2
    assert sorted(results.values(), key=str) == [(200, None), (200, None), (503, "UPSTREAM_BUSY")]


def test_locks_are_released_after_success_and_failure(arrivals_source, consorcio):
    read({"cod_parada": 40})
    consorcio.failures["arrival_time/55"] = "500"
    answer({"cod_parada": 55})
    for key in (sources.ARRIVAL_LOCK_KEY.format(code="40"), sources.ARRIVAL_LOCK_KEY.format(code="55"),
                sources.ARRIVAL_SLOT_KEY.format(index=0), sources.ARRIVAL_SLOT_KEY.format(index=1)):
        assert arrivals_source.get(key) is None


@pytest.mark.parametrize("operation", ["get", "add"])
def test_redis_error_answers_503_data_not_ready_never_500(arrivals_source, consorcio, operation):
    with mock.patch.object(arrivals_source, operation, side_effect=RedisConnectionError("down")):
        response = answer({"cod_parada": 40})
    assert response.status_code == 503
    assert response.data["error_code"] == "DATA_NOT_READY"
    assert response["Retry-After"] == "2"
    assert consorcio.total_hits() == 0


def test_redis_error_exception_carries_the_transport_description(arrivals_source):
    with mock.patch.object(arrivals_source, "get", side_effect=RedisConnectionError("down")):
        with pytest.raises(sources.DataNotReady) as err:
            open_arrivals({"cod_parada": 40})
    assert err.value.transport == "arrivals:unavailable"


def test_redis_error_while_publishing_still_answers_with_the_rows(arrivals_source, consorcio):
    with mock.patch.object(arrivals_source, "set", side_effect=RedisConnectionError("down")):
        description, rows = read({"cod_parada": 40})
    assert description.startswith("arrivals:miss:ok") and len(rows) == 2


def test_engine_for_uri_never_calls_the_api(consorcio):
    engine = sources.engine_for_uri(ARRIVALS_URI)
    model = connectors._get_model(engine=engine, object_location=None, object_location_schema=None)  # pylint: disable=protected-access
    assert [c.name for c in model.columns] == COLUMNS
    assert consorcio.total_hits() == 0


def test_parse_arrivals_gives_the_text_the_csv_connector_read():
    status, fields, rows = sources.parse_arrivals(
        "40", {"nombre": "x", "arrival_time": {"a": {**arrival("111", "00:07:12"), "inc": None, "extra": {"k": 1}}}}
    )
    assert status == "ok"
    assert fields == sorted(fields)
    assert "stop_id" in fields and "cod_parada" in fields
    row = dict(zip(fields, rows[0]))
    assert row["stop_id"] == row["cod_parada"] == "40"
    assert row["inc"] == ""
    assert all(isinstance(value, str) for value in row.values())


def test_parse_arrivals_unknown_and_empty_keep_the_columns():
    assert sources.parse_arrivals("720", {"nombre": "Parada Desconocida", "arrival_time": []}) == (
        "unknown", COLUMNS, [])
    assert sources.parse_arrivals("55", {"nombre": "x", "arrival_time": None}) == ("empty", COLUMNS, [])
