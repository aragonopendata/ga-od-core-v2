"""``update_resource_size`` stores the real size in bytes of the response and runs no extra query."""

from unittest.mock import patch

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext
from sqlalchemy import create_engine, text

from gaodcore import views as gaodcore_views
from gaodcore_manager.models import ConnectorConfig, ResourceConfig, ResourceSizeConfig

DOWNLOAD = "/GA_OD_Core/download"


@pytest.fixture
def resource(tmp_path):
    uri = f"sqlite:///{tmp_path / 'data.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)"))
        for i in range(1, 201):
            conn.execute(text("INSERT INTO things VALUES (:i, :n)"), {"i": i, "n": "x" * 50})
    engine.dispose()
    connector = ConnectorConfig.objects.create(name="size_connector", uri=uri, enabled=True)
    return ResourceConfig.objects.create(
        name="things", connector_config=connector, object_location="things", enabled=True
    )


def _download(client, resource, fmt):
    return client.get(DOWNLOAD, {"resource_id": resource.id, "format": fmt, "sort": "id"})


def _body(response):
    return response.getvalue() if response.streaming else response.content


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_size_is_the_body_length(client, resource, fmt):
    response = _download(client, resource, fmt)
    assert response.status_code == 200
    stored = ResourceSizeConfig.objects.get(resource_id=resource)
    assert stored.size == len(response.content)
    assert stored.registries == 200
    assert stored.size > 200 * 50  # the data, not the few KB of the list object


@pytest.mark.django_db
def test_size_of_xlsx_is_content_length(client, resource):
    response = _download(client, resource, "xlsx")
    assert response.status_code == 200 and response.streaming
    stored = ResourceSizeConfig.objects.get(resource_id=resource)
    assert stored.size == int(response["Content-Length"]) == len(_body(response))
    assert stored.registries == 200


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
def test_no_select_of_the_resource_and_one_row_per_resource(client, resource, fmt):
    _download(client, resource, fmt)  # first download creates the row
    with CaptureQueriesContext(connection) as queries:
        _download(client, resource, fmt)
    statements = [q["sql"] for q in queries.captured_queries]
    # One SELECT of the resource (the view's own) and the upsert: no second load for the size.
    assert sum('FROM "gaodcore_manager_resourceconfig"' in s for s in statements) == 1, statements
    assert not any('FROM "gaodcore_manager_resourcesizeconfig"' in s for s in statements), statements
    assert ResourceSizeConfig.objects.count() == 1


@pytest.mark.django_db
def test_unknown_size_keeps_the_previous_value(client, resource):
    _download(client, resource, "json")
    previous = ResourceSizeConfig.objects.get(resource_id=resource).size
    with patch.object(gaodcore_views, "response_size", return_value=-1):
        _download(client, resource, "csv")
    assert ResourceSizeConfig.objects.get(resource_id=resource).size == previous


def test_response_size_does_not_copy_the_body():
    from django.http import HttpResponse

    response = HttpResponse(content_type="text/csv")
    for chunk in (b"id,note\n", b"1,aaa\n", "2,ñ\n"):
        response.write(chunk)
    expected = len(response.content)
    with patch.object(HttpResponse, "content", property(lambda self: pytest.fail("body was joined"))):
        assert gaodcore_views.response_size(response) == expected


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_response_size_matches_content_length_of_body(client, resource, fmt):
    sizes = []
    real = gaodcore_views.response_size
    with patch.object(gaodcore_views, "response_size", side_effect=lambda r: sizes.append(real(r)) or sizes[-1]):
        response = _download(client, resource, fmt)
    assert sizes and all(size == len(response.content) for size in sizes)


@pytest.mark.django_db
def test_concurrent_first_insert_is_absorbed(resource):
    """Another download inserts the row after our UPDATE found none: the INSERT collides, no error, new values."""
    from django.db.models.query import QuerySet

    from connectors import update_resource_size

    real_update = QuerySet.update
    calls = []

    def racing_update(self, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            ResourceSizeConfig.objects.create(resource_id=resource, registries=1, size=1)
            return 0  # our UPDATE ran before the other download's INSERT
        return real_update(self, **kwargs)

    with patch.object(QuerySet, "update", racing_update):
        update_resource_size(resource, 7, 123)
    assert len(calls) == 2
    stored = ResourceSizeConfig.objects.get(resource_id=resource)
    assert (stored.registries, stored.size) == (7, 123)
    assert ResourceSizeConfig.objects.count() == 1


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_database_error_saving_the_size_keeps_the_download(client, resource, fmt, caplog):
    from django.db import OperationalError

    expected = _body(_download(client, resource, fmt))
    with patch("connectors._upsert_resource_size", side_effect=OperationalError("db gone")):
        with caplog.at_level("WARNING", logger="connectors"):
            response = _download(client, resource, fmt)
    assert response.status_code == 200
    assert _body(response) == expected
    warnings_ = [r for r in caplog.records if r.levelname == "WARNING" and "size" in r.getMessage()]
    assert warnings_ and str(resource.pk) in warnings_[0].getMessage()
