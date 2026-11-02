"""The download flow creates one engine and reflects the table once per request.

Every format goes through the same orchestration: a shared engine and model are opened once in the view, handed
to each step (XLSX pre-check, GeoJSON detection, data query) and the engine is disposed exactly once.
"""

import socket
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text

import connectors
from gaodcore import views as gaodcore_views
from gaodcore.tests.slow_http_server import drain_request, raw_http_server

ENDPOINT = "/GA_OD_Core/download"


@pytest.fixture(autouse=True)
def _no_size_bookkeeping():
    """The resource is not stored in the database, so skip the size bookkeeping."""
    with patch.object(gaodcore_views, "update_resource_size"):
        yield


def _resource_config(uri, object_location="things", schema=None):
    return SimpleNamespace(
        id=1,
        name="things",
        object_location=object_location,
        object_location_schema=schema,
        connector_config=SimpleNamespace(uri=uri),
    )


@pytest.fixture
def sqlite_uri(tmp_path):
    uri = f"sqlite:///{tmp_path / 'things.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO things (id, name) VALUES (1, 'a'), (2, 'b')"))
    engine.dispose()
    return uri


@pytest.fixture
def spies():
    """Count `_get_engine` / `_get_model` calls and the `dispose()` calls of every engine created."""
    state = SimpleNamespace(engines=[], disposes=0)
    real_get_engine = connectors._get_engine

    def get_engine(*args, **kwargs):
        engine = real_get_engine(*args, **kwargs)
        real_dispose = engine.dispose

        def dispose(*d_args, **d_kwargs):
            state.disposes += 1
            return real_dispose(*d_args, **d_kwargs)

        engine.dispose = dispose
        state.engines.append(engine)
        return engine

    with (
        patch.object(connectors, "_get_engine", side_effect=get_engine) as engine_spy,
        patch.object(connectors, "_get_model", wraps=connectors._get_model) as model_spy,
    ):
        state.engine_spy = engine_spy
        state.model_spy = model_spy
        yield state


@pytest.mark.django_db
@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
def test_single_engine_and_reflection(fmt, client, sqlite_uri, spies):
    with patch.object(gaodcore_views, "_get_resource", return_value=_resource_config(sqlite_uri)):
        response = client.get(ENDPOINT, {"resource_id": 1, "formato": fmt, "sort": ["id"]})

    assert response.status_code == 200, response.content
    assert spies.engine_spy.call_count == 1
    assert spies.model_spy.call_count == 1
    assert spies.disposes == 1


@pytest.mark.django_db
def test_engine_disposed_once_when_the_query_fails(client, sqlite_uri, spies):
    with patch.object(gaodcore_views, "_get_resource", return_value=_resource_config(sqlite_uri)):
        response = client.get(ENDPOINT, {"resource_id": 1, "formato": "json", "sort": ["nope"]})

    assert response.status_code == 400
    assert spies.engine_spy.call_count == 1
    assert spies.disposes == 1


@pytest.mark.django_db
def test_engine_disposed_once_when_the_table_does_not_exist(client, sqlite_uri, spies):
    config = _resource_config(sqlite_uri, object_location="missing")
    with patch.object(gaodcore_views, "_get_resource", return_value=config):
        response = client.get(ENDPOINT, {"resource_id": 1, "formato": "json"})

    assert response.status_code == 503
    assert spies.model_spy.call_count == 1
    assert spies.disposes == 1


@pytest.mark.django_db
def test_single_engine_and_reflection_geojson(client):
    model = Table(
        "places",
        MetaData(),
        Column("id", Integer),
        Column("name", String),
        Column("geom", Geometry("POINT", srid=4326)),
    )
    query = MagicMock()
    for method in ("filter_by", "filter", "order_by", "offset", "limit"):
        getattr(query, method).return_value = query
    query.all.return_value = [(1, "Zaragoza", '{"type":"Point","coordinates":[-0.87,41.65]}')]
    session = MagicMock()
    session.query.return_value = query
    engine = MagicMock()
    config = _resource_config("postgresql://u:p@example.invalid:5432/db", "places", "public")

    with (
        patch.object(gaodcore_views, "_get_resource", return_value=config),
        patch.object(connectors, "_get_engine", return_value=engine) as engine_spy,
        patch.object(connectors, "_get_model", return_value=model) as model_spy,
        patch.object(connectors, "sessionmaker", return_value=lambda: session),
    ):
        response = client.get(ENDPOINT, {"resource_id": 1, "formato": "json"})

    assert response.status_code == 200
    assert response.json()["type"] == "FeatureCollection"
    assert engine_spy.call_count == 1
    assert model_spy.call_count == 1
    assert engine.dispose.call_count == 1


@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
@pytest.mark.django_db
def test_http_resource_is_downloaded_once_per_request(fmt, client):
    body = b'[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]'
    requests = []

    def handler(conn: socket.socket) -> None:
        drain_request(conn)
        requests.append(1)
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            + b"Content-Length: %d\r\nConnection: close\r\n\r\n" % len(body)
            + body
        )

    with raw_http_server(handler) as url:
        config = _resource_config(url, object_location=None)
        with patch.object(gaodcore_views, "_get_resource", return_value=config):
            response = client.get(ENDPOINT, {"resource_id": 1, "formato": fmt, "sort": ["id"]})

    assert response.status_code == 200
    assert len(requests) == 1
