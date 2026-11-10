"""Filters on an HTTP source with no rows answer no rows instead of an unknown-column error."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import connectors
from gaodcore import views as gaodcore_views
from gaodcore.tests.slow_http_server import raw_http_server, respond_after_delay

PREVIEW = "/GA_OD_Core/preview"
DOWNLOAD = "/GA_OD_Core/download"


def _source(body: bytes, content_type: str):
    return respond_after_delay(
        b"HTTP/1.1 200 OK\r\n",
        f"Content-Length: {len(body)}\r\nContent-Type: {content_type}\r\n".encode(),
        body,
        delay_seconds=0,
    )


def _rows(body: bytes, content_type: str, filters):
    with raw_http_server(_source(body, content_type)) as url:
        return connectors.get_session_data(url, None, None, filters, "", [], [])


CSV = "text/csv; charset=utf-8"
JSON = "application/json"
ARRIVALS_CSV = b"TR,cod_parada,id,name,stop_id\r\nR,40,0,L\xc3\xadnea 111,40\r\nR,55,0,L\xc3\xadnea 111,55\r\n"


@pytest.mark.parametrize(
    "body, content_type",
    [(b"", CSV), (b"[]", JSON), (b"TR,id,stop_id\r\n", CSV)],
    ids=["empty-csv", "empty-json", "csv-header-only"],
)
@pytest.mark.parametrize(
    "filters",
    [{"cod_parada": 40}, {"parada": "40"}, {"cod_parada": {"$gt": 1}}],
    ids=["unknown", "unknown-string", "unknown-operator"],
)
def test_unknown_filter_on_an_empty_http_source_returns_no_rows(body, content_type, filters):
    assert _rows(body, content_type, filters) == []


def test_known_filter_on_a_csv_with_only_a_header_returns_no_rows():
    """A header without rows keeps its columns: they are valid filters."""
    assert _rows(b"TR,id,stop_id\r\n", CSV, {"stop_id": "40"}) == []


def test_unknown_filter_on_an_http_source_with_rows_is_still_an_error():
    with pytest.raises(connectors.FieldNoExistsError, match="parada"):
        _rows(ARRIVALS_CSV, CSV, {"parada": 40})


@pytest.mark.parametrize("value", [40, "40"], ids=["number", "string"])
def test_numeric_filter_matches_the_text_column_of_a_csv(value):
    rows = _rows(ARRIVALS_CSV, CSV, {"cod_parada": value})
    assert [row[1] for row in rows] == ["40"]


def _config(uri):
    return SimpleNamespace(
        id=2190,
        name="arrivals",
        object_location=None,
        object_location_schema=None,
        connector_config=SimpleNamespace(uri=uri),
    )


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [PREVIEW, DOWNLOAD])
def test_preview_of_an_empty_http_source_filtered_by_an_unknown_column_is_an_empty_200(client, endpoint):
    with raw_http_server(_source(b"", CSV)) as url:
        with patch.object(gaodcore_views, "_get_resource", return_value=_config(url)):
            response = client.get(
                endpoint,
                {"view_id": 2190, "formato": "json", "_pageSize": 100, "filters": '{"cod_parada": 40}'},
            )
    assert response.status_code == 200
    body = response.getvalue() if hasattr(response, "getvalue") else b"".join(response.streaming_content)
    assert json.loads(body) == []


@pytest.mark.django_db
def test_preview_of_an_http_source_with_rows_still_rejects_an_unknown_column(client):
    with raw_http_server(_source(ARRIVALS_CSV, CSV)) as url:
        with patch.object(gaodcore_views, "_get_resource", return_value=_config(url)):
            response = client.get(PREVIEW, {"view_id": 2190, "formato": "json", "filters": '{"parada": 40}'})
    assert response.status_code == 400
