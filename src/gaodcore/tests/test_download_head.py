"""HEAD on download and preview answers with the headers of GET without querying the resource."""

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from connectors import DriverConnectionError, NoObjectError
from gaodcore import views as gaodcore_views

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"
FORMATS = [renderer.format for renderer in gaodcore_views.PUBLIC_DATA_RENDERER_CLASSES]


@pytest.fixture(autouse=True)
def _no_size_bookkeeping():
    with patch.object(gaodcore_views, "update_resource_size") as update:
        yield update


def _config(uri):
    return SimpleNamespace(
        id=1,
        name="things",
        object_location="things",
        object_location_schema=None,
        connector_config=SimpleNamespace(uri=uri),
    )


@pytest.fixture
def small_resource(tmp_path):
    uri = f"sqlite:///{tmp_path / 'small.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO things (id, name) VALUES (1, 'a'), (2, 'b')"))
    engine.dispose()
    config = _config(uri)
    with patch.object(gaodcore_views, "_get_resource", return_value=config):
        yield config


def _no_query():
    """Patches that fail if the request runs a query on the source (opening and reflecting it is allowed)."""
    failure = AssertionError("source queried")
    return [
        patch.object(gaodcore_views, name, side_effect=failure)
        for name in ("get_resource_data", "get_resource_data_feature", "get_GeoJson_resource")
    ]


@pytest.fixture
def query_must_not_run():
    patches = _no_query()
    for each in patches:
        each.start()
    yield
    for each in patches:
        each.stop()


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def timing_lines():
    """Messages of the download_timing logger, which does not propagate to the root logger."""
    handler = _ListHandler()
    gaodcore_views.timing_logger.addHandler(handler)
    yield lambda: [r.getMessage() for r in handler.records if r.getMessage().startswith("download_timing ")]
    gaodcore_views.timing_logger.removeHandler(handler)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
@pytest.mark.parametrize("fmt", FORMATS)
def test_head_sends_the_headers_of_get(client, small_resource, endpoint, fmt):
    params = {"resource_id": 1, "formato": fmt, "name": "things_file"}
    get = client.get(endpoint, params)
    assert get.status_code == 200

    patches = _no_query()
    for each in patches:
        each.start()
    try:
        head = client.head(endpoint, params)
    finally:
        for each in patches:
            each.stop()

    assert head.status_code == 200
    assert head["Content-Type"] == get["Content-Type"]
    assert head.get("Content-Disposition") == get.get("Content-Disposition")


@pytest.mark.django_db
def test_head_has_no_body_and_no_content_length(client, small_resource, query_must_not_run):
    response = client.head(DOWNLOAD, {"resource_id": 1, "formato": "json"})
    assert response.status_code == 200
    assert not response.has_header("Content-Length")
    assert b"".join(response.streaming_content) == b""


@pytest.mark.django_db
def test_head_does_not_store_the_size(client, small_resource, query_must_not_run, _no_size_bookkeeping):
    client.head(DOWNLOAD, {"resource_id": 1, "formato": "csv"})
    _no_size_bookkeeping.assert_not_called()


@pytest.mark.django_db
def test_head_accepts_the_view_id_alias(client, small_resource, query_must_not_run):
    assert client.head(DOWNLOAD, {"view_id": 1, "formato": "json"}).status_code == 200


@pytest.mark.django_db
@pytest.mark.parametrize(
    "params",
    [
        {"formato": "json"},  # no resource_id
        {"resource_id": "abc", "formato": "json"},
        {"resource_id": 1, "formato": "pdf"},
        {"resource_id": 1, "formato": "csv", "delimiter": ";;"},
        {"resource_id": 1, "formato": "json", "filters": "{not json"},
        {"resource_id": 1, "formato": "json", "offset": "x"},
    ],
)
def test_head_rejects_what_get_rejects(client, small_resource, params):
    get = client.get(DOWNLOAD, params)
    with patch.object(gaodcore_views, "open_resource", side_effect=AssertionError("source opened")):
        head = client.head(DOWNLOAD, params)
    assert get.status_code == 400
    assert head.status_code == get.status_code


@pytest.mark.django_db
def test_head_of_missing_resource_returns_the_get_error(client):
    # Real lookup: the test database has no ResourceConfig.
    params = {"resource_id": 999, "formato": "json"}
    with patch.object(gaodcore_views, "open_resource", side_effect=AssertionError("source opened")):
        head = client.head(DOWNLOAD, params)
    get = client.get(DOWNLOAD, params)
    assert head.status_code == get.status_code == 400


@pytest.mark.django_db
def test_head_of_missing_table_returns_the_get_error(client, tmp_path):
    uri = f"sqlite:///{tmp_path / 'empty.db'}"
    create_engine(uri).dispose()
    params = {"resource_id": 1, "formato": "json"}
    with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)):
        head = client.head(DOWNLOAD, params)
        get = client.get(DOWNLOAD, params)
    assert head.status_code == get.status_code == 503


@pytest.mark.django_db
@pytest.mark.parametrize("error", [DriverConnectionError("down"), NoObjectError("gone")])
def test_head_reports_an_unavailable_source(client, small_resource, error):
    with patch.object(gaodcore_views, "open_resource", side_effect=error):
        response = client.head(DOWNLOAD, {"resource_id": 1, "formato": "json"})
    assert response.status_code == 503


@pytest.mark.django_db
@pytest.mark.parametrize("uri", ["https://example.org/data.csv", "http://example.org/data.json"])
def test_head_does_not_open_http_sources(client, uri):
    # Opening an HTTP source downloads the whole remote file.
    with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)), \
            patch.object(gaodcore_views, "open_resource", side_effect=AssertionError("source opened")):
        response = client.head(DOWNLOAD, {"resource_id": 1, "formato": "json"})
    assert response.status_code == 200


@pytest.mark.django_db
def test_head_logs_a_timing_line_with_one_connect_and_no_rows(client, small_resource, query_must_not_run,
                                                              timing_lines):
    client.head(DOWNLOAD, {"resource_id": 1, "formato": "xml"})
    lines = timing_lines()
    assert len(lines) == 1
    assert "resource_id=1 endpoint=download format=xml status=200 rows=-1 connects=1" in lines[0]
    assert "bytes=-1" in lines[0]
