"""Resource 2187 (``internal://transports/zaragoza/stops``) served in process from the shared copy.

The copy lives in the ``transports`` cache (memory of the process in tests); the Consorcio is a local mock.
"""

import threading
from datetime import timedelta
from unittest import mock

import pytest
from django.utils import timezone
from redis.exceptions import ConnectionError as RedisConnectionError
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory
from rest_framework.views import APIView

import connectors
from gaodcore_transports import sources
from gaodcore_transports.tests.conftest import ARRIVALS_URI, STOPS_URI

pytestmark = pytest.mark.usefixtures("stops_source")


def open_stops():
    return sources.open_for_request(STOPS_URI, {}, {}, [], [])


def rows_of(opened):
    with opened.resource.engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(opened.resource.model.select())]  # pylint: disable=protected-access


def open_and_read():
    """Open and read in the calling thread: the in-memory SQLite table belongs to the thread that built it."""
    opened = open_stops()
    return opened.description, rows_of(opened)


def age_copy(cache, seconds):
    entry = cache.get(sources.STOPS_KEY)
    entry["fetched_at"] = timezone.now() - timedelta(seconds=seconds)
    cache.set(sources.STOPS_KEY, entry, None)


class StopsView(APIView):
    """What the public routes will do: open the source and let the exception handler answer."""

    authentication_classes = []
    permission_classes = []

    def get(self, request):
        opened = open_stops()
        opened.resource.engine.dispose()
        return Response({"state": opened.description})


def get_stops_view():
    return StopsView.as_view()(APIRequestFactory().get("/stops"))


def test_without_a_copy_the_request_downloads_it_and_answers(transports_cache, consorcio):
    opened = open_stops()
    assert opened.description == "stops:downloaded:age=0"
    assert rows_of(opened) == consorcio.stops  # same columns, order and values as the HTTP connector read them
    assert consorcio.hits == {"stops": 1}
    assert transports_cache.get(sources.STOPS_LOCK_KEY) is None  # lock released
    assert len(transports_cache.get(sources.STOPS_KEY)["rows"]) == 3


def test_columns_and_types_are_those_of_the_http_connector(consorcio, internal_config):
    """The rows go through the same code as an HTTP source, so the table equals the one the connector built."""
    opened = open_stops()
    http_engine = connectors.engine_from_rows(list(consorcio.stops))
    http_model = connectors._get_model(  # pylint: disable=protected-access
        engine=http_engine, object_location=None, object_location_schema=None
    )
    assert [(c.name, str(c.type)) for c in opened.resource.model.columns] == [
        (c.name, str(c.type)) for c in http_model.columns
    ]


def test_fresh_copy_is_served_without_calling_the_api(transports_cache, consorcio):
    open_stops()
    consorcio.reset_counters()
    opened = open_stops()
    assert opened.description.startswith("stops:fresh:")
    assert consorcio.total_hits() == 0


def test_stale_copy_is_refreshed_by_one_request_and_the_rest_serve_the_previous_one(transports_cache, consorcio):
    open_stops()
    age_copy(transports_cache, 2 * 3600)
    consorcio.stops.append({"stop_id": "60", "stop_name": "Ctra. Logroño, 74", "lat": 41.6, "lon": -0.9})
    release = threading.Event()
    entered = threading.Event()

    def hold(path):
        entered.set()
        release.wait(5)

    consorcio.on_request = hold
    results = {}
    refresher = threading.Thread(target=lambda: results.update(first=open_and_read()))
    refresher.start()
    assert entered.wait(5)
    # While the first request refreshes, the others serve the previous copy and do not call.
    others = [open_stops() for _ in range(3)]
    assert all(o.description.startswith("stops:stale:") and len(rows_of(o)) == 3 for o in others)
    release.set()
    refresher.join(5)
    assert results["first"][0] == "stops:refreshed:age=0"
    assert len(results["first"][1]) == 4
    assert consorcio.hits == {"stops": 2}  # the first download and the refresh
    assert open_stops().description.startswith("stops:fresh:")


def test_copy_past_the_stale_margin_counts_as_missing(transports_cache, consorcio, internal_config):
    open_stops()
    age_copy(transports_cache, 3600 + 86400 + 60)
    opened = open_stops()
    assert opened.description == "stops:downloaded:age=0"
    assert consorcio.hits == {"stops": 2}


def test_without_a_copy_the_others_get_503_with_retry_after_while_one_downloads(transports_cache, consorcio):
    release = threading.Event()
    entered = threading.Event()

    def hold(path):
        entered.set()
        release.wait(5)

    consorcio.on_request = hold
    results = {}
    downloader = threading.Thread(target=lambda: results.update(first=open_and_read()))
    downloader.start()
    assert entered.wait(5)
    response = get_stops_view()
    assert response.status_code == 503
    assert response["Retry-After"] == "10"
    assert response.data["error_code"] == "DATA_NOT_READY"
    with pytest.raises(sources.DataNotReady) as err:
        open_stops()
    assert err.value.transport == "stops:missing:age=-"
    release.set()
    downloader.join(5)
    assert len(results["first"][1]) == 3
    assert consorcio.hits == {"stops": 1}


def test_lock_held_by_another_serves_the_stale_copy_without_calling(transports_cache, consorcio):
    open_stops()
    age_copy(transports_cache, 2 * 3600)
    consorcio.reset_counters()
    assert transports_cache.add(sources.STOPS_LOCK_KEY, "someone else", 30)
    opened = open_stops()
    assert opened.description.startswith("stops:stale:")
    assert consorcio.total_hits() == 0
    assert transports_cache.get(sources.STOPS_LOCK_KEY) == "someone else"  # not ours to release


def test_lock_held_by_another_without_a_copy_answers_503_with_retry_after(transports_cache, consorcio):
    assert transports_cache.add(sources.STOPS_LOCK_KEY, "someone else", 30)
    response = get_stops_view()
    assert response.status_code == 503
    assert response["Retry-After"] == "10"
    assert consorcio.total_hits() == 0


@pytest.mark.parametrize("failure", ["500", "invalid", "cut", "hang", "truncated"])
def test_failed_download_keeps_the_copy_and_backs_off(transports_cache, consorcio, internal_config, failure):
    open_stops()
    before = transports_cache.get(sources.STOPS_KEY)
    age_copy(transports_cache, 2 * 3600)
    aged = transports_cache.get(sources.STOPS_KEY)
    internal_config.set(stops_timeout_seconds=1)
    consorcio.hang_seconds = 3
    if failure == "truncated":
        consorcio.stops = consorcio.stops[:1]  # 1 of 3 rows: below stops_min_ratio
    else:
        consorcio.failures["stops"] = failure
    consorcio.reset_counters()
    opened = open_stops()
    assert opened.description.startswith("stops:stale:")
    assert rows_of(opened)[0]["stop_id"] == "40"
    assert consorcio.total_hits() == 1
    after = transports_cache.get(sources.STOPS_KEY)
    assert after["rows"] == before["rows"] and after["fetched_at"] == aged["fetched_at"]  # copy and its date kept
    assert transports_cache.get(sources.STOPS_BACKOFF_KEY) == 1
    assert transports_cache.get(sources.STOPS_LOCK_KEY) is None  # released also on failure
    # During the backoff no request calls the API, whatever the traffic asks.
    for _ in range(5):
        assert open_stops().description.startswith("stops:stale:")
    assert consorcio.total_hits() == 1


def test_backoff_doubles_on_consecutive_failures_up_to_the_maximum(transports_cache, consorcio, internal_config):
    open_stops()
    age_copy(transports_cache, 2 * 3600)
    consorcio.failures["stops"] = "500"
    waits = []
    for _ in range(6):
        with mock.patch.object(transports_cache, "set", wraps=transports_cache.set) as spy:
            open_stops()
        waits.append(next(c.args[2] for c in spy.call_args_list if c.args[0] == sources.STOPS_BACKOFF_KEY))
        transports_cache.delete(sources.STOPS_BACKOFF_KEY)  # the wait has passed
    assert waits == [60, 120, 240, 480, 900, 900]


def test_download_after_the_backoff_clears_the_failures(transports_cache, consorcio):
    open_stops()
    age_copy(transports_cache, 2 * 3600)
    consorcio.failures["stops"] = "500"
    open_stops()
    transports_cache.delete(sources.STOPS_BACKOFF_KEY)
    consorcio.failures.clear()
    assert open_stops().description == "stops:refreshed:age=0"
    assert transports_cache.get(sources.STOPS_FAILURES_KEY) is None


def test_failed_download_without_a_copy_answers_503_and_the_next_waits_for_the_backoff(transports_cache, consorcio):
    consorcio.failures["stops"] = "500"
    response = get_stops_view()
    assert response.status_code == 503 and response["Retry-After"] == "10"
    assert transports_cache.get(sources.STOPS_KEY) is None
    consorcio.failures.clear()
    assert get_stops_view().status_code == 503  # backoff: not even tried
    assert consorcio.hits == {"stops": 1}


@pytest.mark.parametrize(
    ("data", "previous", "ok"),
    [
        pytest.param({"stops": [{"stop_id": "1"}]}, 0, True, id="first-copy"),
        pytest.param({"stops": [{"stop_id": str(i)} for i in range(5)]}, 10, True, id="half-of-previous"),
        pytest.param({"stops": [{"stop_id": str(i)} for i in range(4)]}, 10, False, id="below-ratio"),
        pytest.param({"stops": []}, 0, False, id="empty"),
        pytest.param({"nope": 1}, 0, False, id="no-stops-key"),
        pytest.param([], 0, False, id="not-an-object"),
        pytest.param({"stops": [{"name": "x"}]}, 0, False, id="no-stop-id"),
        pytest.param({"stops": [{"stop_id": "1", "nested": {"a": 1}}]}, 0, False, id="nested-value"),
    ],
)
def test_validation(data, previous, ok):
    if ok:
        assert sources._validate_stops(data, previous, 0.5) == data["stops"]  # pylint: disable=protected-access
    else:
        with pytest.raises(sources.FetchError) as err:
            sources._validate_stops(data, previous, 0.5)  # pylint: disable=protected-access
        assert err.value.kind == "invalid"


@pytest.mark.parametrize("operation", ["get", "add"])
def test_redis_error_answers_503_data_not_ready_never_500(transports_cache, consorcio, operation):
    with mock.patch.object(transports_cache, operation, side_effect=RedisConnectionError("down")):
        response = get_stops_view()
    assert response.status_code == 503
    assert response["Retry-After"] == "10"
    assert response.data["error_code"] == "DATA_NOT_READY"
    assert consorcio.total_hits() == 0


def test_redis_error_exception_is_data_not_ready(transports_cache):
    with mock.patch.object(transports_cache, "get", side_effect=RedisConnectionError("down")):
        with pytest.raises(sources.DataNotReady) as err:
            open_stops()
    assert err.value.transport == "stops:unavailable:age=-"
    assert err.value.get_codes() == "DATA_NOT_READY"


def test_redis_error_while_publishing_still_answers_with_the_downloaded_rows(transports_cache, consorcio):
    with mock.patch.object(transports_cache, "set", side_effect=RedisConnectionError("down")):
        opened = open_stops()
    assert len(rows_of(opened)) == 3
    assert transports_cache.get(sources.STOPS_LOCK_KEY) is None


def test_engine_for_uri_serves_the_columns_for_health_checks(transports_cache, consorcio):
    engine = sources.engine_for_uri(STOPS_URI)
    model = connectors._get_model(engine=engine, object_location=None, object_location_schema=None)  # pylint: disable=protected-access
    assert [c.name for c in model.columns] == ["stop_id", "stop_name", "lat", "lon"]


def test_unknown_internal_source_and_arrivals_without_a_stop_are_clear_errors():
    with pytest.raises(connectors.NotImplementedSchemaError, match="does not exist"):
        sources.source_of("internal://transports/zaragoza/other")
    with pytest.raises(connectors.NotImplementedSchemaError, match="not supported yet"):
        sources.open_for_request(ARRIVALS_URI, {}, {}, [], [])
    assert sources.is_internal(STOPS_URI) and not sources.is_internal("https://example.org/")
