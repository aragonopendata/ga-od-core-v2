"""The preview endpoint never returns more than ``_PREVIEW_LIMIT`` rows; download is not bounded."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from gaodcore import views as gaodcore_views

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"
TOTAL_ROWS = 1500


@pytest.fixture(autouse=True)
def _no_size_bookkeeping():
    with patch.object(gaodcore_views, "update_resource_size"):
        yield


@pytest.fixture
def big_resource(tmp_path):
    uri = f"sqlite:///{tmp_path / 'big.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(
            text("INSERT INTO things (id, name) VALUES (:id, :name)"),
            [{"id": i, "name": f"n{i}"} for i in range(1, TOTAL_ROWS + 1)],
        )
    engine.dispose()
    config = SimpleNamespace(
        id=1,
        name="things",
        object_location="things",
        object_location_schema=None,
        connector_config=SimpleNamespace(uri=uri),
    )
    with patch.object(gaodcore_views, "_get_resource", return_value=config):
        yield config


def _rows(client, endpoint, **params):
    response = client.get(endpoint, {"resource_id": 1, "format": "json", **params})
    assert response.status_code == 200, response.content
    return json.loads(response.content)


@pytest.mark.django_db
@pytest.mark.parametrize(
    "params,expected",
    [
        ({"limit": 50000}, 1000),
        ({"_pageSize": 50000}, 1000),
        ({}, 1000),
        ({"limit": 0}, 1000),
        ({"limit": -5}, 1000),
        ({"_pageSize": -5}, 1000),
        ({"limit": 10}, 10),
        ({"_pageSize": 10}, 10),
    ],
)
def test_preview_limit(client, big_resource, params, expected):
    assert len(_rows(client, PREVIEW, **params)) == expected


@pytest.mark.django_db
def test_preview_xlsx_is_bounded(client, big_resource):
    from io import BytesIO

    from openpyxl import load_workbook

    response = client.get(PREVIEW, {"resource_id": 1, "format": "xlsx", "limit": 50000})
    assert response.status_code == 200
    sheet = load_workbook(BytesIO(response.content)).active
    assert sheet.max_row == 1000 + 1  # header + rows


@pytest.mark.django_db
def test_preview_cap_can_be_patched(client, big_resource):
    with patch.object(gaodcore_views.DownloadView, "_PREVIEW_LIMIT", 7):
        assert len(_rows(client, PREVIEW)) == 7
        assert len(_rows(client, PREVIEW, limit=100)) == 7
        assert len(_rows(client, PREVIEW, limit=3)) == 3


@pytest.mark.django_db
def test_download_without_limit_returns_everything(client, big_resource):
    assert len(_rows(client, DOWNLOAD)) == TOTAL_ROWS


@pytest.mark.django_db
def test_download_limit_is_not_capped(client, big_resource):
    assert len(_rows(client, DOWNLOAD, limit=1200)) == 1200
    assert len(_rows(client, DOWNLOAD, _pageSize=1200)) == 1200


@pytest.mark.django_db
def test_preview_not_a_number_still_errors(client, big_resource):
    response = client.get(PREVIEW, {"resource_id": 1, "limit": "abc"})
    assert response.status_code == 400


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
def test_accept_html_without_format_returns_json_not_browsable_api(client, big_resource, endpoint):
    response = client.get(endpoint, {"resource_id": 1, "limit": 5}, HTTP_ACCEPT="text/html")
    assert response.status_code == 200
    assert "text/html" not in response["Content-Type"]
    assert "application/json" in response["Content-Type"]
    assert len(json.loads(response.content)) == 5


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
def test_explicit_formats_unchanged(client, big_resource, endpoint):
    csv_response = client.get(f"{endpoint}.csv", {"resource_id": 1, "limit": 3})
    assert csv_response.status_code == 200
    assert "text/csv" in csv_response["Content-Type"]
    assert csv_response.content.decode().splitlines()[0].replace('"', "") in ("id,name", "name,id")
    xlsx_response = client.get(endpoint, {"resource_id": 1, "limit": 3, "format": "xlsx"})
    assert xlsx_response.status_code == 200
    assert "spreadsheetml" in xlsx_response["Content-Type"]


def test_public_data_views_do_not_use_browsable_api():
    from rest_framework.renderers import BrowsableAPIRenderer
    from rest_framework.settings import api_settings

    expected = [r for r in api_settings.DEFAULT_RENDERER_CLASSES if r is not BrowsableAPIRenderer]
    for view in (gaodcore_views.DownloadView, gaodcore_views.ShowColumnsView, gaodcore_views.ResourcesView):
        assert list(view.renderer_classes) == expected
    assert BrowsableAPIRenderer in api_settings.DEFAULT_RENDERER_CLASSES


@pytest.mark.django_db
def test_swagger_ui_still_renders_html(client):
    response = client.get("/GA_OD_Core/ui/")
    assert response.status_code == 200
    assert "text/html" in response["Content-Type"]
