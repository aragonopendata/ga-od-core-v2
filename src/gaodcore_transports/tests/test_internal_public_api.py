"""Resources 2187 and 2190 through the real public routes (``/GA_OD_Core/preview``, ``download``, ``show_columns``).

The connectors have an ``internal://`` URI, the Consorcio is a local mock and the ``self_calls`` guard fails any call
to a public URL or use of the HTTP connector.
"""

import io
import json
import logging
from unittest import mock

import pytest
from django.test import Client
from openpyxl import load_workbook

from gaodcore import views as gaodcore_views
from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_transports import sources
from gaodcore_transports.tests.conftest import ARRIVALS_URI, STOPS_URI

pytestmark = [pytest.mark.django_db, pytest.mark.usefixtures("transports_cache", "internal_config", "self_calls")]

PREVIEW = "/GA_OD_Core/preview"
DOWNLOAD = "/GA_OD_Core/download"
SHOW_COLUMNS = "/GA_OD_Core/show_columns"
CODE_40 = '{"cod_parada": 40}'
FORMATS = ["csv", "scsv", "json", "xml", "yaml", "xlsx"]


def make_resource(name, uri):
    connector = ConnectorConfig.objects.create(name=f"connector_{name}", uri=uri, enabled=True)
    return ResourceConfig.objects.create(name=name, connector_config=connector, object_location=None, enabled=True)


@pytest.fixture
def stops_resource():
    return make_resource("stops", STOPS_URI)


@pytest.fixture
def arrivals_resource():
    return make_resource("arrivals", ARRIVALS_URI)


@pytest.fixture(autouse=True)
def _no_size_bookkeeping():
    with mock.patch.object(gaodcore_views, "update_resource_size"):
        yield


@pytest.fixture
def timing_lines():
    records = []

    class Handler(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    handler = Handler(logging.INFO)
    gaodcore_views.timing_logger.addHandler(handler)
    yield lambda: [line for line in records if line.startswith("download_timing ")]
    gaodcore_views.timing_logger.removeHandler(handler)


@pytest.fixture
def engines(monkeypatch):
    """The engines the internal sources hand out and whether each was disposed."""
    opened = []
    real = sources.open_for_request

    def spy(*args, **kwargs):
        result = real(*args, **kwargs)
        engine = result.resource.engine
        state = {"disposed": 0}
        original = engine.dispose

        def dispose(*a, **k):
            state["disposed"] += 1
            return original(*a, **k)

        engine.dispose = dispose
        opened.append(state)
        return result

    monkeypatch.setattr(sources, "open_for_request", spy)
    return opened


def body_of(response):
    if getattr(response, "streaming", False):
        return b"".join(response.streaming_content)
    return response.content


def fetch(endpoint, resource, fmt="json", **params):
    response = Client().get(endpoint, {"resource_id": resource.id, "format": fmt, **params})
    return response, body_of(response)


def json_rows(endpoint, resource, **params):
    response, body = fetch(endpoint, resource, "json", **params)
    assert response.status_code == 200, body
    return json.loads(body)


def test_preview_of_stops(stops_resource, consorcio):
    rows = json_rows(PREVIEW, stops_resource, sort="stop_id")
    assert [row["stop_id"] for row in rows] == ["1011", "40", "55"]
    assert consorcio.total_hits("stops") == 1


def test_preview_of_one_stop_arrivals(arrivals_resource, consorcio):
    rows = json_rows(PREVIEW, arrivals_resource, filters='{"cod_parada": 40}', sort="id_linea")
    assert [row["id_linea"] for row in rows] == ["111", "112"]
    assert {row["cod_parada"] for row in rows} == {"40"}
    assert consorcio.total_hits("arrival_time/") == 1


def test_download_of_arrivals_filters_and_paginates_like_any_resource(arrivals_resource):
    rows = json_rows(DOWNLOAD, arrivals_resource, filters='{"cod_parada": 40}', sort="id_linea", limit=1, offset=1)
    assert [row["id_linea"] for row in rows] == ["112"]


@pytest.mark.parametrize("endpoint", [PREVIEW, DOWNLOAD], ids=["preview", "download"])
@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("kind", ["stops", "arrivals"])
def test_every_format_works_and_the_engine_is_disposed(kind, fmt, endpoint, stops_resource, arrivals_resource,
                                                       engines, timing_lines):
    resource, params = (stops_resource, {}) if kind == "stops" else (arrivals_resource, {"filters": CODE_40})
    response, body = fetch(endpoint, resource, fmt, **params)
    assert response.status_code == 200
    if fmt == "xlsx":
        sheet = load_workbook(io.BytesIO(body)).active
        assert sheet.max_row > 1
    elif fmt == "json":
        assert json.loads(body)
    else:
        assert body
    assert [state["disposed"] for state in engines] == [1]
    (line,) = timing_lines()
    assert " transport=" in line and f" endpoint={endpoint.rsplit('/', 1)[1]} " in line


def test_streamed_download_disposes_the_engine_when_the_sending_ends(stops_resource, engines):
    response = Client().get(DOWNLOAD, {"resource_id": stops_resource.id, "format": "csv"})
    assert response.status_code == 200 and response.streaming
    iterator = iter(response.streaming_content)
    next(iterator)
    assert [state["disposed"] for state in engines] == [0]
    b"".join(iterator)
    assert [state["disposed"] for state in engines] == [1]


def test_timing_line_ends_with_the_transport_description(arrivals_resource, stops_resource, timing_lines):
    json_rows(PREVIEW, stops_resource)
    json_rows(PREVIEW, arrivals_resource, filters='{"cod_parada": 40}')
    stops_line, arrivals_line = timing_lines()
    assert " transport=stops" in stops_line and " transport=arrivals" in arrivals_line
    for line in (stops_line, arrivals_line):
        assert line.index(" maxrss_kb=") < line.index(" transport=")


def test_timing_line_of_an_error_carries_the_transport_of_the_exception(arrivals_resource, timing_lines, consorcio):
    consorcio.failures["arrival_time/40"] = "500"
    response, _ = fetch(PREVIEW, arrivals_resource, filters='{"cod_parada": 40}')
    assert response.status_code in (502, 503)
    (line,) = timing_lines()
    assert " status=%s " % response.status_code in line and " transport=" in line


def test_timing_line_of_a_normal_resource_has_no_transport(tmp_path, timing_lines):
    from sqlalchemy import create_engine, text

    uri = f"sqlite:///{tmp_path / 'plain.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY)"))
        conn.execute(text("INSERT INTO things VALUES (1)"))
    engine.dispose()
    connector = ConnectorConfig.objects.create(name="plain", uri=uri, enabled=True)
    plain = ResourceConfig.objects.create(
        name="plain", connector_config=connector, object_location="things", enabled=True
    )
    assert json_rows(PREVIEW, plain) == [{"id": 1}]
    (line,) = timing_lines()
    assert "transport=" not in line


@pytest.mark.parametrize("endpoint", [PREVIEW, DOWNLOAD], ids=["preview", "download"])
@pytest.mark.parametrize("kind", ["stops", "arrivals"])
def test_head_validates_but_never_opens_the_source(kind, endpoint, stops_resource, arrivals_resource, consorcio):
    resource = stops_resource if kind == "stops" else arrivals_resource
    with mock.patch.object(sources, "open_for_request", side_effect=AssertionError("opened")), \
            mock.patch.object(sources, "engine_for_uri", side_effect=AssertionError("opened")):
        response = Client().head(endpoint, {"resource_id": resource.id, "format": "csv"})
    assert response.status_code == 200
    assert consorcio.total_hits() == 0


@pytest.mark.parametrize(
    "params",
    [
        {"filters": '{"parada": 40}'},
        {"fields": "parada", "filters": '{"cod_parada": 40}'},
        {"sort": "parada", "filters": '{"cod_parada": 40}'},
    ],
    ids=["filter", "fields", "sort"],
)
def test_unknown_column_is_the_usual_400_without_calls(params, arrivals_resource, consorcio, tmp_path):
    response, body = fetch(PREVIEW, arrivals_resource, **params)
    assert response.status_code == 400
    assert response["Content-Type"].startswith("application/problem+json")
    problem = json.loads(body)
    codes = [error["code"] for error in problem["errors"]["non_field_errors"]]
    assert codes == ["INVALID_SORT" if "sort" in params else "INVALID_FIELD"]
    assert consorcio.total_hits() == 0
    # Same problem document as a resource of any other kind.
    from sqlalchemy import create_engine, text

    uri = f"sqlite:///{tmp_path / 'plain.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (cod_parada INTEGER)"))
    engine.dispose()
    connector = ConnectorConfig.objects.create(name="plain", uri=uri, enabled=True)
    plain = ResourceConfig.objects.create(
        name="plain", connector_config=connector, object_location="things", enabled=True
    )
    plain_response, plain_body = fetch(PREVIEW, plain, **params)
    assert plain_response.status_code == 400
    assert json.loads(plain_body) == problem


def test_filter_on_id_without_a_stop_is_400_without_calls(arrivals_resource, consorcio):
    response, body = fetch(PREVIEW, arrivals_resource, filters='{"id": 5}')
    assert response.status_code == 400
    assert json.loads(body)["errors"]["non_field_errors"][0]["code"] == "INVALID_FIELD"
    assert consorcio.total_hits() == 0


def test_arrivals_of_every_stop_are_served_from_the_aggregate(arrivals_resource, consorcio):
    rows = json_rows(PREVIEW, arrivals_resource)
    assert {row["id_linea"] for row in rows} >= {"111", "112"}
    assert consorcio.total_hits("stops") == 1


def test_data_not_ready_is_503_with_retry_after(stops_resource, consorcio):
    consorcio.failures["stops"] = "500"
    response, body = fetch(PREVIEW, stops_resource)
    assert response.status_code == 503
    assert json.loads(body)["error_code"] == "DATA_NOT_READY"
    assert int(response["Retry-After"]) > 0


def test_upstream_busy_is_503_with_retry_after(arrivals_resource, consorcio, transports_cache):
    transports_cache.set(sources.ARRIVAL_LOCK_KEY.format(code="40"), "another-worker", 60)
    response, body = fetch(PREVIEW, arrivals_resource, filters='{"cod_parada": 40}')
    assert response.status_code == 503
    assert json.loads(body)["error_code"] == "UPSTREAM_BUSY"
    assert int(response["Retry-After"]) > 0
    assert consorcio.total_hits() == 0


def test_failed_call_is_503_with_retry_after_and_the_transport_in_the_log(arrivals_resource, consorcio, timing_lines):
    consorcio.failures["arrival_time/40"] = "500"
    response, body = fetch(PREVIEW, arrivals_resource, filters='{"cod_parada": 40}')
    assert response.status_code == 503
    assert json.loads(body)["error_code"] == "CONNECTION_UNAVAILABLE"
    assert int(response["Retry-After"]) > 0
    (line,) = timing_lines()
    assert line.endswith(" transport=arrivals:error")


def test_show_columns_of_stops_and_arrivals(stops_resource, arrivals_resource, consorcio):
    stops = Client().get(SHOW_COLUMNS, {"resource_id": stops_resource.id})
    assert stops.status_code == 200
    assert {"stop_id", "stop_name"} <= {column["COLUMN_NAME"] for column in json.loads(stops.content)}
    hits = consorcio.total_hits("arrival_time/")
    arrivals = Client().get(SHOW_COLUMNS, {"resource_id": arrivals_resource.id})
    assert arrivals.status_code == 200
    assert [column["COLUMN_NAME"] for column in json.loads(arrivals.content)] == list(sources.ARRIVAL_COLUMNS)
    assert consorcio.total_hits("arrival_time/") == hits


def test_manager_validation_accepts_the_known_uris_and_rejects_unknown_ones():
    from rest_framework.exceptions import ValidationError

    from gaodcore_manager.validators import uri_validator

    uri_validator(STOPS_URI)
    uri_validator(ARRIVALS_URI)
    with pytest.raises(ValidationError) as caught:
        uri_validator("internal://transports/zaragoza/other")
    assert caught.value.detail  # "Schema of the URI is not available."
