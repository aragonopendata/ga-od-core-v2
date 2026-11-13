"""GeoJSON downloads (JSON of a resource with a geometry column) are streamed like plain JSON.

The source is a real PostGIS database (``postgis/postgis`` container), so the geometry is the text ``ST_AsGeoJSON``
returns and the body can be compared byte for byte with the one built whole (``DOWNLOAD_STREAMING = False``).
``client`` reads a streamed response to the end; ``streaming_client`` hands it back unread.
"""

import json
import logging
from unittest.mock import patch

import pytest
import sqlalchemy.exc
from rest_framework.renderers import JSONRenderer
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

import connectors
import download_stream
from download_stream import GeoJsonEncoder
from gaodcore import views as gaodcore_views
from gaodcore_manager.models import ConnectorConfig, ResourceConfig, ResourceSizeConfig

pytestmark = pytest.mark.django_db

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"
ROWS = 10
BLOCK_ROWS = 3
START = b'{"type":"FeatureCollection","features":['

_TABLE = """
CREATE TABLE places (
    id integer PRIMARY KEY,
    name text,
    price numeric(6, 2),
    ratio double precision,
    seen date,
    shape geometry(Geometry, -1)
)
"""


def _geometry(i):
    if i == 4:
        return None  # an unlocated feature
    if i % 3 == 0:  # a polygon with UTM-like coordinates, as the IGEAR resources
        x, y = 675000 + i * 1.25, 4612000 + i * 0.5
        return f"POLYGON(({x} {y},{x + 10} {y},{x + 10} {y + 10},{x} {y + 10},{x} {y}))"
    return f"POINT({-0.8773 - i / 100} {41.6561 + i / 100})"


@pytest.fixture(scope="module")
def postgis():
    with PostgresContainer("postgis/postgis:15-3.3") as container:
        url = container.get_connection_url()
        engine = create_engine(url)
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(text(_TABLE))
            conn.execute(text("CREATE TABLE empty_places (id integer PRIMARY KEY, shape geometry(Geometry, -1))"))
            for i in range(1, ROWS + 1):
                conn.execute(
                    text("INSERT INTO places VALUES (:i, :n, :p, :r, :d, ST_GeomFromText(:g))"),
                    {"i": i, "n": f"Ñandú, {i}\u2028", "p": i * 1.5, "r": i / 4, "d": f"2024-02-{i:02d}",
                     "g": _geometry(i)},
                )
        engine.dispose()
        yield url.replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(autouse=True)
def _small_blocks(settings):
    settings.DOWNLOAD_BLOCK_ROWS = BLOCK_ROWS
    settings.DOWNLOAD_STREAMING = True


def _resource(uri, location="places"):
    connector = ConnectorConfig.objects.create(name=f"connector_{location}", uri=uri, enabled=True)
    return ResourceConfig.objects.create(
        name=location, connector_config=connector, object_location=location, object_location_schema="public",
        enabled=True,
    )


@pytest.fixture
def resource(postgis):
    return _resource(postgis)


def get(client, resource, **params):
    return client.get(DOWNLOAD, {"resource_id": resource.id, "format": "json", "sort": "id", **params})


@pytest.fixture
def timing_lines():
    records = []

    class Handler(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    handler = Handler(logging.INFO)
    gaodcore_views.timing_logger.addHandler(handler)
    yield lambda: [r for r in records if r.startswith("download_timing ")]
    gaodcore_views.timing_logger.removeHandler(handler)


CASES = [
    pytest.param({}, id="whole"),
    pytest.param({"sort": "id desc", "limit": "4", "offset": "2"}, id="sort_limit_offset"),
    pytest.param({"filters": '{"id": 3}'}, id="filter_one_row"),
    pytest.param({"filters": '{"id": 99}'}, id="filter_no_rows"),
    pytest.param({"like": '{"name": "%2%"}'}, id="like"),
    pytest.param({"fields": "id", "columns": "x"}, id="fields_and_columns_are_ignored"),
    pytest.param({"name": "my_file"}, id="file_name"),
]


@pytest.mark.parametrize("params", CASES)
def test_body_is_the_same_streamed_or_built_whole(client, settings, resource, params):
    streamed = get(client, resource, **params)
    settings.DOWNLOAD_STREAMING = False
    whole = get(client, resource, **params)

    assert streamed.status_code == whole.status_code == 200
    assert streamed.content == whole.content
    assert streamed["Content-Type"] == whole["Content-Type"] == "application/json"
    assert streamed["Content-Disposition"] == whole["Content-Disposition"]


def test_body_is_the_feature_collection(client, resource):
    body = get(client, resource).content
    collection = json.loads(body)
    features = collection["features"]

    assert collection["type"] == "FeatureCollection" and len(features) == ROWS
    assert features[0]["properties"] == {
        "id": 1, "name": "Ñandú, 1", "price": "1.50", "ratio": "0.25", "seen": "2024-02-01",
    }
    assert features[0]["geometry"]["type"] == "Point"
    assert features[2]["geometry"]["type"] == "Polygon"
    assert features[3]["geometry"] is None
    # Control characters such as U+2028 are removed from the properties, streamed or not.
    assert "Ñandú".encode() in body and b"2028" not in body and "\u2028".encode() not in body


def test_download_is_streamed_in_blocks(streaming_client, resource):
    response = get(streaming_client, resource)
    assert isinstance(response, download_stream.StreamedDownloadResponse) and response.status_code == 200

    chunks = list(response.streaming_content)
    response.close()

    assert chunks[0].startswith(START + b'{"type":"Feature","geometry":{"type":"Point"')
    assert chunks[-1] == b"]}"
    assert len(chunks) == -(-ROWS // BLOCK_ROWS) + 1  # the blocks of rows and the closing bracket
    assert len(json.loads(b"".join(chunks))["features"]) == ROWS


def test_empty_result_is_an_empty_collection_and_completes(client, settings, postgis, timing_lines):
    empty = _resource(postgis, "empty_places")

    streamed = get(client, empty)
    settings.DOWNLOAD_STREAMING = False
    whole = get(client, empty)

    assert streamed.content == whole.content == START + b"]}"
    assert " rows=0 " in timing_lines()[0] and " result=complete " in timing_lines()[0]


def test_timing_line_and_no_stored_size(client, resource, timing_lines):
    body = get(client, resource).content

    (line,) = timing_lines()
    assert f" rows={ROWS} " in line and f" bytes={len(body)} " in line and " result=complete " in line
    assert not ResourceSizeConfig.objects.exists()  # a FeatureCollection never stored the size of the resource


def test_client_going_away_is_early_close_and_closes_the_cursor(streaming_client, resource, timing_lines):
    with patch.object(connectors.FeatureStream, "close", autospec=True, side_effect=connectors.FeatureStream.close) as close:
        response = get(streaming_client, resource)
        next(iter(response.streaming_content))
        response.close()
        response.close()

    assert close.call_count == 1
    (line,) = timing_lines()
    assert " result=early_close " in line and f" rows={BLOCK_ROWS} " in line


def test_failure_after_the_first_block_aborts_without_closing_the_collection(streaming_client, resource, timing_lines):
    real_fetch = connectors.FeatureStream.fetch
    calls = []

    def fetch(self, size, first=False):
        calls.append(first)
        if len(calls) == 2:
            raise RuntimeError("source went away")
        return real_fetch(self, size, first)

    response = get(streaming_client, resource)
    received = []
    with patch.object(connectors.FeatureStream, "fetch", fetch):
        with pytest.raises(RuntimeError, match="source went away"):
            for chunk in response.streaming_content:
                received.append(chunk)
    response.close()

    body = b"".join(received)
    assert body.startswith(START) and not body.endswith(b"]}") and b"RuntimeError" not in body
    (line,) = timing_lines()
    assert " result=error " in line and f" rows={BLOCK_ROWS * 2} " in line


def _problem(response):
    return response.status_code, response.json().get("error_code")


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
@pytest.mark.parametrize(
    "case,expected",
    [
        pytest.param("missing_object", (503, "OBJECT_UNAVAILABLE"), id="missing_object"),
        pytest.param("bad_sort", (400, "VALIDATION_ERROR"), id="bad_sort"),
        pytest.param("bad_filter_field", (400, "VALIDATION_ERROR"), id="bad_filter_field"),
        pytest.param("rejected_query", (503, "OBJECT_UNAVAILABLE"), id="rejected_query"),
    ],
)
def test_failure_before_the_first_block_returns_the_usual_error(streaming_client, settings, resource, case, expected,
                                                                streaming):
    settings.DOWNLOAD_STREAMING = streaming
    params = {}
    if case == "missing_object":
        resource.object_location = "nothing_here"
        resource.save()
    elif case == "bad_sort":
        params["sort"] = "nope"
    elif case == "bad_filter_field":
        params["filters"] = '{"nope": 1}'
    if case == "rejected_query":
        # The database rejects the query (here, a function it does not have): an unavailable object, as always.
        with patch.object(connectors.GeoFunc, "ST_AsGeoJSON", side_effect=sqlalchemy.func.st_nope):
            response = get(streaming_client, resource, **params)
    else:
        response = get(streaming_client, resource, **params)

    assert _problem(response) == expected


def test_failure_of_the_first_fetch_is_an_unavailable_object(streaming_client, resource):
    failure = sqlalchemy.exc.ProgrammingError("SELECT", {}, Exception("rejected"))
    with patch.object(connectors.FeatureStream, "fetch", autospec=True) as fetch:
        def first_fails(self, size, first=False):
            with connectors._feature_query_errors():
                raise failure
        fetch.side_effect = first_fails
        response = get(streaming_client, resource)

    assert _problem(response) == (503, "OBJECT_UNAVAILABLE")


def test_indent_preview_and_kill_switch_are_not_streamed(client, settings, resource):
    indented = client.get(
        DOWNLOAD, {"resource_id": resource.id, "format": "json", "sort": "id"},
        HTTP_ACCEPT="application/json; indent=2",
    )
    assert indented.status_code == 200 and not isinstance(indented, download_stream.StreamedDownloadResponse)
    assert indented.content.startswith(b"{\n")

    preview = client.get(PREVIEW, {"resource_id": resource.id, "format": "json"})
    assert preview.status_code == 200 and not isinstance(preview, download_stream.StreamedDownloadResponse)
    assert json.loads(preview.content)["type"] == "FeatureCollection"

    settings.DOWNLOAD_STREAMING = False
    with patch.object(gaodcore_views, "get_feature_rows") as rows:
        assert get(client, resource).status_code == 200
    rows.assert_not_called()


# --- The encoder alone -----------------------------------------------------------------------------------------------

def test_encoder_copies_the_geometry_and_writes_null_for_a_missing_one():
    encoder = GeoJsonEncoder(JSONRenderer())
    first, rows = encoder.encode([({"a": 1}, '{"type":"Point","coordinates":[1,2]}'), ({"a": "\u2029"}, None)])
    rest, more = encoder.encode([({"a": 3}, '{"type":"Point","coordinates":[0.00001,-5]}')])

    assert rows == 2 and more == 1
    body = first + rest + encoder.close()[0]
    assert body == (
        START + b'{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"a":1}},'
        b'{"type":"Feature","geometry":null,"properties":{"a":"\\u2029"}},'
        b'{"type":"Feature","geometry":{"type":"Point","coordinates":[0.00001,-5]},"properties":{"a":3}}]}'
    )


def test_encoder_without_rows_opens_the_collection_once():
    encoder = GeoJsonEncoder(JSONRenderer())
    assert encoder.encode([]) == (START, 0)
    encoder.encode([({}, None)])
    assert encoder.encode([]) == (b"", 0)


def test_encoder_refuses_a_renderer_that_is_not_compact():
    renderer = JSONRenderer()
    renderer.compact = False
    with pytest.raises(ValueError):
        GeoJsonEncoder(renderer)
