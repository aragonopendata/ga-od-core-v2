"""Sessions are closed and engines disposed on every error path of the data functions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy.exc
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, event, text

import connectors
from gaodcore import views as gaodcore_views

ENDPOINT = "/GA_OD_Core/download"


@pytest.fixture
def sqlite_uri(tmp_path):
    uri = f"sqlite:///{tmp_path / 'things.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT, geom TEXT)"))
        conn.execute(
            text("INSERT INTO things (id, name, geom) VALUES (1, 'a', :g)"),
            {"g": '{"type":"Point","coordinates":[1,2]}'},
        )
    engine.dispose()
    return uri


@pytest.fixture
def spies():
    """Real engines whose `dispose()` calls and checked-out connections at that moment are recorded."""
    state = SimpleNamespace(disposes=0, checked_out_at_dispose=[])
    real_get_engine = connectors._get_engine

    def get_engine(*args, **kwargs):
        engine = real_get_engine(*args, **kwargs)
        real_dispose = engine.dispose

        @event.listens_for(engine, "connect")
        def _functions(dbapi_conn, _record):
            dbapi_conn.create_function("AsGeoJSON", 1, lambda value: value)
            dbapi_conn.create_function("ST_AsEWKB", 1, lambda value: value)

        def dispose(*d_args, **d_kwargs):
            state.disposes += 1
            state.checked_out_at_dispose.append(engine.pool.checkedout())
            return real_dispose(*d_args, **d_kwargs)

        engine.dispose = dispose
        return engine

    with patch.object(connectors, "_get_engine", side_effect=get_engine):
        yield state


@pytest.fixture
def geo_model():
    model = Table(
        "things",
        MetaData(),
        Column("id", Integer),
        Column("name", String),
        Column("geom", Geometry("POINT", srid=4326)),
    )
    with patch.object(connectors, "_get_model", return_value=model):
        yield model


def _feature(uri, **kwargs):
    return connectors.get_resource_data_feature(
        uri, "things", None, filters={}, like={}, fields=[], sort=[], **kwargs
    )


def _fail_select_from_things():
    """Make every SELECT on the resource table raise an OperationalError after the cursor was requested."""

    def _raise(conn, cursor, statement, parameters, context, executemany):
        if "FROM things" in statement and statement.lstrip().upper().startswith("SELECT"):
            raise sqlalchemy.exc.OperationalError(statement, parameters, Exception("boom"))

    return _raise


def test_geojson_sql_error_closes_session(sqlite_uri, spies, geo_model):
    real = connectors._get_engine
    # spies already patched _get_engine; add the failing listener on top of it
    def get_engine(*args, **kwargs):
        engine = real(*args, **kwargs)
        event.listen(engine, "before_cursor_execute", _fail_select_from_things())
        return engine

    with patch.object(connectors, "_get_engine", side_effect=get_engine):
        with pytest.raises(sqlalchemy.exc.OperationalError):
            _feature(sqlite_uri)

    assert spies.disposes == 1
    assert spies.checked_out_at_dispose == [0]


def test_geojson_normalization_error_closes_session(sqlite_uri, spies, geo_model):
    with patch.object(connectors, "sanitize_control_charcters", side_effect=RuntimeError("bad")):
        with pytest.raises(RuntimeError):
            _feature(sqlite_uri)

    assert spies.disposes == 1
    assert spies.checked_out_at_dispose == [0]


def test_geojson_bad_geometry_json_closes_session(sqlite_uri, spies, geo_model):
    engine = create_engine(sqlite_uri)
    with engine.begin() as conn:
        conn.execute(text("UPDATE things SET geom = 'not json'"))
    engine.dispose()

    with pytest.raises(ValueError):
        _feature(sqlite_uri)

    assert spies.disposes == 1
    assert spies.checked_out_at_dispose == [0]


def test_geojson_shared_resource_is_not_disposed(sqlite_uri, spies, geo_model):
    with connectors.open_resource(sqlite_uri, "things", None) as resource:
        with patch.object(connectors, "sanitize_control_charcters", side_effect=RuntimeError):
            with pytest.raises(RuntimeError):
                _feature(sqlite_uri, resource=resource)
        assert spies.disposes == 0
        assert resource.engine.pool.checkedout() == 0
    assert spies.disposes == 1


def _data(uri, **kwargs):
    args = dict(
        uri=uri,
        object_location="things",
        object_location_schema=None,
        filters={},
        like={},
        fields=[],
        sort=[],
    )
    args.update(kwargs)
    return list(connectors.get_resource_data(**args))


def test_standalone_unknown_field_disposes_engine(sqlite_uri, spies):
    with pytest.raises(connectors.FieldNoExistsError):
        _data(sqlite_uri, fields=["nope"])
    assert spies.disposes == 1


def test_standalone_like_on_unknown_column_disposes_engine(sqlite_uri, spies):
    with pytest.raises(connectors.FieldNoExistsError):
        _data(sqlite_uri, like={"nope": "x"})
    assert spies.disposes == 1


def test_standalone_feature_like_on_unknown_column_disposes_engine(sqlite_uri, spies, geo_model):
    with pytest.raises(connectors.FieldNoExistsError):
        connectors.get_resource_data_feature(
            sqlite_uri, "things", None, filters={}, like={"nope": "x"}, fields=[], sort=[]
        )
    assert spies.disposes == 1


def test_shared_unknown_field_does_not_dispose(sqlite_uri, spies):
    with connectors.open_resource(sqlite_uri, "things", None) as resource:
        with pytest.raises(connectors.FieldNoExistsError):
            _data(sqlite_uri, fields=["nope"], resource=resource)
        assert spies.disposes == 0
    assert spies.disposes == 1


def test_validate_max_rows_allowed_failure_disposes_engine(sqlite_uri, spies):
    with pytest.raises(connectors.NoObjectError):
        connectors._validate_max_rows_allowed(sqlite_uri, "missing", None)
    assert spies.disposes == 1


def test_validate_max_rows_allowed_query_failure_disposes_engine(sqlite_uri, spies):
    with patch.object(connectors, "sessionmaker") as maker:
        session = MagicMock()
        session.__enter__.return_value = session
        session.query.side_effect = sqlalchemy.exc.ProgrammingError("s", {}, Exception("x"))
        maker.return_value = lambda: session
        with pytest.raises(connectors.NoObjectError):
            connectors._validate_max_rows_allowed(sqlite_uri, "things", None)
    assert spies.disposes == 1
    session.__exit__.assert_called_once()


def test_validate_resource_mssql_failure_disposes_engine():
    engine = MagicMock()
    with (
        patch.object(connectors, "_get_engine", return_value=engine),
        patch.object(connectors, "_get_model", side_effect=RuntimeError("reflect")),
    ):
        with pytest.raises(RuntimeError):
            connectors.validate_resource_mssql(
                uri="mssql+pyodbc://u:p@h/db", object_location="t", object_location_schema=None
            )
    assert engine.dispose.call_count == 1


def test_validate_resource_mssql_query_failure_disposes_engine(sqlite_uri, spies):
    with patch.object(connectors, "_get_columns", side_effect=RuntimeError("cols")):
        with pytest.raises(RuntimeError):
            connectors.validate_resource_mssql(
                uri=sqlite_uri, object_location="things", object_location_schema=None
            )
    assert spies.disposes == 1


@pytest.mark.django_db
def test_download_sql_error_after_execution_closes_session_and_disposes_once(
    client, sqlite_uri, spies
):
    config = SimpleNamespace(
        id=1,
        name="things",
        object_location="things",
        object_location_schema=None,
        connector_config=SimpleNamespace(uri=sqlite_uri),
    )
    real = connectors._get_engine

    def get_engine(*args, **kwargs):
        engine = real(*args, **kwargs)
        event.listen(engine, "before_cursor_execute", _fail_select_from_things())
        return engine

    with (
        patch.object(gaodcore_views, "update_resource_size"),
        patch.object(gaodcore_views, "_get_resource", return_value=config),
        patch.object(connectors, "_get_engine", side_effect=get_engine),
    ):
        response = client.get(ENDPOINT, {"resource_id": 1, "formato": "json"})

    assert response.status_code == 503
    assert "QUERY_ERROR" in response.content.decode(), response.content
    assert spies.disposes == 1
    assert spies.checked_out_at_dispose == [0]
