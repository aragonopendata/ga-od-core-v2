"""Large downloads: no identical download at once (429) and a ceiling of simultaneous ones (503).

The locks live in PostgreSQL, so these tests use two real connections to the ``postgres:13`` container: ``other``
plays the worker that is already downloading and ``mine`` the worker that receives the request.
"""

import logging
from unittest.mock import PropertyMock, patch
from urllib.parse import urlencode

import pytest
from django.db.utils import ConnectionHandler
from django.http import QueryDict
from sqlalchemy import create_engine, text

import download_limits
from conftest import DB_NAME, DB_PASSWORD, DB_USERNAME
from connectors import DriverConnectionError
from gaodcore import views as gaodcore_views
from gaodcore_manager.models import ConnectorConfig, ResourceConfig, ResourceSizeConfig
from gaodcore_project.config import DownloadLimitsConfig

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"


def _limits(settings, **kwargs):
    settings.DOWNLOAD_LIMITS = DownloadLimitsConfig(**kwargs)


@pytest.fixture
def pg_connection(pg):
    """Factory of independent Django connections to the PostgreSQL container (one per simulated worker)."""
    host, port = pg
    created = []

    def make():
        handler = ConnectionHandler({
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": DB_NAME,
                "USER": DB_USERNAME,
                "PASSWORD": DB_PASSWORD,
                "HOST": host,
                "PORT": port,
            }
        })
        created.append(handler["default"])
        return created[-1]

    yield make
    for connection in created:
        connection.close()


@pytest.fixture
def mine(pg_connection):
    """The connection of the worker under test; every download request uses it."""
    connection = pg_connection()
    with patch.object(download_limits, "limiter_connection", return_value=connection):
        yield connection


@pytest.fixture
def other(pg_connection, mine):
    """The connection of another worker."""
    return pg_connection()


@pytest.fixture
def worker(pg_connection, mine):
    """Factory of further workers: advisory locks are re-entrant inside one session, so each held download needs its
    own connection, as it has its own worker in production."""
    return pg_connection


@pytest.fixture
def sources(tmp_path):
    """Two resources backed by SQLite files, with a known number of rows stored (large by default)."""
    def make(name, rows_stored):
        uri = f"sqlite:///{tmp_path / (name + '.db')}"
        engine = create_engine(uri)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)"))
            conn.execute(text("INSERT INTO things VALUES (1, 'a'), (2, 'b')"))
        engine.dispose()
        connector = ConnectorConfig.objects.create(name=f"connector_{name}", uri=uri, enabled=True)
        resource = ResourceConfig.objects.create(
            name=name, connector_config=connector, object_location="things", enabled=True
        )
        if rows_stored is not None:
            ResourceSizeConfig.objects.create(resource_id=resource, registries=rows_stored, size=1)
        return resource

    return make


@pytest.fixture
def big(sources):
    return sources("big", 500_000)


@pytest.fixture
def big2(sources):
    return sources("big2", 500_000)


def hold(connection, resource, fmt="json", **params):
    """Start a large download of ``resource`` on ``connection``: what another worker would be doing."""
    query = QueryDict(urlencode({"resource_id": resource.id, **params}))
    url = download_limits.normalized_url(resource.id, query, fmt)
    # Forced: the other worker holds it whatever this test configured for the request under test.
    with patch.object(download_limits, "is_enabled", return_value=True), \
            patch.object(download_limits, "is_heavy", return_value=True):
        return download_limits.acquire(resource.id, None, url, connection=connection)


def free(connection, lock_class, key):
    """Whether ``connection`` can take (and gives back) the advisory lock; checks that a lock is not held."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", [lock_class, key])
        taken = cursor.fetchone()[0]
        if taken:
            cursor.execute("SELECT pg_advisory_unlock(%s, %s)", [lock_class, key])
    return taken


def get(client, resource, **params):
    params = {"resource_id": resource.id, "formato": "json", "sort": "id", **params}
    return client.get(DOWNLOAD, params)


@pytest.fixture
def query_calls():
    """Counts the queries run against the source without altering them."""
    with patch.object(gaodcore_views, "get_resource_data", wraps=gaodcore_views.get_resource_data) as spy:
        yield spy


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def timing_lines():
    handler = _ListHandler()
    gaodcore_views.timing_logger.addHandler(handler)
    yield lambda: [r.getMessage() for r in handler.records if r.getMessage().startswith("download_timing ")]
    gaodcore_views.timing_logger.removeHandler(handler)


pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _keep_stored_sizes():
    """The two rows of the test tables would otherwise replace the large size stored for the resource."""
    with patch.object(gaodcore_views, "update_resource_size"):
        yield


# --- L2: identical downloads ---------------------------------------------------------------------------------------

def test_identical_download_in_progress_gets_429(client, settings, big, other, query_calls, timing_lines):
    _limits(settings)
    hold(other, big, sort="id")

    response = get(client, big)

    assert response.status_code == 429
    assert response["Retry-After"] == "120"
    assert response["Content-Type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 429 and body["error_code"] == "DOWNLOAD_IN_PROGRESS"
    assert "otra petición idéntica" in body["detail"] and "identical request" in body["detail"]
    query_calls.assert_not_called()
    assert timing_lines()[0].endswith(" limited=duplicate")
    assert " status=429 " in timing_lines()[0]


def test_retry_after_comes_from_the_configuration(client, settings, big, other):
    _limits(settings, retry_after=45)
    hold(other, big, sort="id")
    assert get(client, big)["Retry-After"] == "45"


def test_other_file_name_is_still_the_same_download(client, settings, big, other, query_calls):
    _limits(settings)
    hold(other, big, name="first", sort="id")

    response = get(client, big, name="second", nameRes="third")

    assert response.status_code == 429
    query_calls.assert_not_called()


def test_format_is_resolved_not_spelled(client, settings, big, other):
    _limits(settings)
    hold(other, big, fmt="json", sort="id")

    # Same resolved format through the Accept header instead of the formato parameter.
    params = {"resource_id": big.id, "sort": "id"}
    response = client.get(DOWNLOAD, params, HTTP_ACCEPT="application/json")

    assert response.status_code == 429


def test_other_format_or_filter_is_another_download(client, settings, big, other, query_calls):
    _limits(settings)
    hold(other, big, sort="id")

    assert get(client, big, formato="csv").status_code == 200
    assert get(client, big, filters='{"id": 1}').status_code == 200
    assert query_calls.call_count == 2


def test_duplicate_is_allowed_again_when_the_other_worker_dies(client, settings, big, other):
    _limits(settings)
    hold(other, big, sort="id")
    assert get(client, big).status_code == 429

    other.close()  # a killed worker: PostgreSQL drops the locks of its session

    assert get(client, big).status_code == 200


# --- L3: ceiling of large downloads --------------------------------------------------------------------------------

def test_no_free_slot_for_the_resource_gets_503(client, settings, big, worker, query_calls, timing_lines):
    _limits(settings, max_per_resource=2, max_total=10)
    hold(worker(), big, sort="id", filters='{"id": 1}')
    hold(worker(), big, sort="id", filters='{"id": 2}')

    response = get(client, big)

    assert response.status_code == 503
    assert response["Retry-After"] == "120"
    body = response.json()
    assert body["error_code"] == "TOO_MANY_DOWNLOADS"
    assert "Demasiadas descargas grandes" in body["detail"] and "Too many large downloads" in body["detail"]
    query_calls.assert_not_called()
    assert timing_lines()[0].endswith(" limited=resource")


def test_another_resource_still_has_slots(client, settings, big, big2, other):
    _limits(settings, max_per_resource=1, max_total=10)
    hold(other, big, sort="id")

    assert get(client, big, filters='{"id": 1}').status_code == 503
    assert get(client, big2).status_code == 200


def test_no_free_total_slot_gets_503(client, settings, big, big2, worker, query_calls, timing_lines):
    _limits(settings, max_per_resource=2, max_total=2)
    hold(worker(), big, sort="id")
    hold(worker(), big2, sort="id")

    response = get(client, big, filters='{"id": 1}')

    assert response.status_code == 503
    assert response["Retry-After"] == "120"
    query_calls.assert_not_called()
    assert timing_lines()[0].endswith(" limited=total")


def test_a_refused_request_leaves_no_lock_behind(client, settings, big, big2, worker, other):
    _limits(settings, max_per_resource=2, max_total=2)
    hold(worker(), big, sort="id")
    hold(worker(), big2, sort="id")
    query = QueryDict(urlencode({"resource_id": big.id, "sort": "id", "filters": '{"id": 1}'}))
    key = download_limits.url_lock_key(download_limits.normalized_url(big.id, query, "json"))

    assert get(client, big, filters='{"id": 1}').status_code == 503

    # The duplicate lock and the per-resource slot taken before the total check failed were given back.
    assert free(other, download_limits.LOCK_CLASS_DUPLICATE, key)
    assert free(other, download_limits.LOCK_CLASS_RESOURCE, big.id * 16 + 1)


def test_slot_is_free_again_when_the_response_is_closed(client, settings, big, other):
    _limits(settings, max_per_resource=1, max_total=1)

    assert get(client, big).status_code == 200
    assert get(client, big, filters='{"id": 1}').status_code == 200  # the test client closes the response
    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert free(other, download_limits.LOCK_CLASS_RESOURCE, big.id * 16)


def test_streaming_response_holds_its_slot_until_it_is_closed(client, settings, big, other):
    _limits(settings, max_per_resource=1, max_total=1)

    xlsx = get(client, big, formato="xlsx")
    assert xlsx.status_code == 200 and xlsx.streaming
    assert not free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    with patch.object(download_limits, "limiter_connection", return_value=other):  # another worker
        assert get(client, big, filters='{"id": 1}').status_code == 503

    xlsx.getvalue()  # the server has sent the file and closes the response

    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert get(client, big, filters='{"id": 1}').status_code == 200


def test_slot_is_free_again_after_an_error_response(client, settings, big, other):
    _limits(settings, max_per_resource=1, max_total=1)
    with patch.object(gaodcore_views, "get_resource_data", side_effect=DriverConnectionError("down")):
        assert get(client, big).status_code == 503

    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert get(client, big).status_code == 200


def test_slot_is_free_again_after_an_unexpected_exception(client, settings, big, other):
    _limits(settings, max_per_resource=1, max_total=1)
    with patch.object(gaodcore_views, "get_resource_data", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            get(client, big)

    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert free(other, download_limits.LOCK_CLASS_RESOURCE, big.id * 16)


def test_slot_is_free_again_when_the_lazy_render_fails(client, settings, big, other):
    """Django answers a failing render with an error response and never closes the original one."""
    _limits(settings, max_per_resource=1, max_total=1)
    with patch("rest_framework.response.Response.rendered_content", new_callable=PropertyMock) as rendered:
        rendered.side_effect = RuntimeError("render failed")
        with pytest.raises(RuntimeError):
            get(client, big)

    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert free(other, download_limits.LOCK_CLASS_RESOURCE, big.id * 16)


def test_failing_release_closes_the_connection(settings, big, mine, other, caplog):
    _limits(settings)
    permit = hold(mine, big, sort="id")

    with patch.object(download_limits, "_unlock", side_effect=RuntimeError("boom")):
        permit.release()

    assert mine.connection is None  # closing the session dropped its locks
    assert free(other, download_limits.LOCK_CLASS_TOTAL, 0)
    assert "Could not release the download locks" in caplog.text
    permit.release()  # idempotent


# --- what is never limited -----------------------------------------------------------------------------------------

@pytest.fixture
def saturated(settings, big, other):
    """Every limit exhausted for ``big`` by another worker, including the identical download."""
    _limits(settings, max_per_resource=1, max_total=1)
    hold(other, big, sort="id")


def test_saturation_blocks_a_large_download(client, saturated, big):
    assert get(client, big).status_code == 429
    assert get(client, big, filters='{"id": 1}').status_code == 503


def test_preview_is_never_limited(client, saturated, big):
    response = client.get(PREVIEW, {"resource_id": big.id, "formato": "json", "sort": "id"})
    assert response.status_code == 200


def test_head_is_never_limited(client, saturated, big, query_calls):
    response = client.head(DOWNLOAD, {"resource_id": big.id, "formato": "json", "sort": "id"})
    assert response.status_code == 200


def test_small_limit_is_never_limited(client, saturated, big):
    assert get(client, big, limit=10).status_code == 200
    assert get(client, big, limit=99_999).status_code == 200


def test_limit_from_heavy_rows_on_is_limited(client, saturated, big):
    assert get(client, big, limit=100_000).status_code in (429, 503)
    assert get(client, big, limit=1_000_000).status_code in (429, 503)


def test_small_resource_is_never_limited(client, settings, sources, other):
    _limits(settings, max_per_resource=1, max_total=1)
    small = sources("small", 99_999)
    hold(other, small, sort="id")

    assert get(client, small).status_code == 200


def test_resource_without_a_stored_size_is_not_large(client, settings, sources, other):
    _limits(settings, max_per_resource=1, max_total=1)
    unknown = sources("unknown", None)
    hold(other, unknown, sort="id")

    assert get(client, unknown).status_code == 200


def test_listed_resource_is_large_without_a_stored_size(client, settings, sources, other):
    unknown = sources("listed", None)
    _limits(settings, heavy_resource_ids=[unknown.id])
    hold(other, unknown, sort="id")

    assert get(client, unknown).status_code == 429


def test_heavy_rows_is_configurable(client, settings, sources, other):
    small = sources("modest", 50)
    _limits(settings, heavy_rows=10)
    hold(other, small, sort="id")

    assert get(client, small).status_code == 429
    assert get(client, small, limit=5).status_code == 200


def test_disabled_limits_never_limit(client, settings, big, other):
    _limits(settings, enabled=False, max_per_resource=1, max_total=1)
    hold(other, big, sort="id")

    assert get(client, big).status_code == 200


def test_sqlite_configuration_database_never_limits(client, settings, big, caplog):
    _limits(settings, max_per_resource=1, max_total=1)
    # No ``mine`` fixture: the limiter uses Django's default connection, which is SQLite here.
    assert get(client, big).status_code == 200
    assert get(client, big, filters='{"id": 1}').status_code == 200


# --- helpers -------------------------------------------------------------------------------------------------------

def test_normalized_url_ignores_names_and_orders_parameters():
    first = QueryDict("resource_id=1&b=2&a=1&name=x&formato=csv")
    second = QueryDict("a=1&nameRes=y&view_id=1&b=2&format=json")
    assert download_limits.normalized_url(1, first, "csv") == download_limits.normalized_url(1, second, "csv")
    assert download_limits.normalized_url(1, first, "csv") != download_limits.normalized_url(1, first, "json")
    assert download_limits.normalized_url(1, first, "csv") != download_limits.normalized_url(2, first, "csv")


def test_url_lock_key_fits_an_int4():
    for url in ("a", "b", "c" * 1000):
        assert -2**31 <= download_limits.url_lock_key(url) < 2**31
