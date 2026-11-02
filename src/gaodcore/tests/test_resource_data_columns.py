"""``get_resource_data`` reflects the table once and takes column names from that reflection.

A SQLite file stands in for the origin; ``_get_engine`` and ``_get_model`` are patched so that
the number of engines and reflections can be counted.
"""

from unittest.mock import patch

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, MetaData, String, Table

import connectors

URI = "postgresql://user:password@localhost:5432/db"


def _make_table():
    return Table(
        "resource",
        MetaData(),
        Column("id", Integer),
        Column("name", String),
        Column("brand", String),
    )


@pytest.fixture
def origin(tmp_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'origin.db'}")
    table = _make_table()
    table.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            table.insert(),
            [
                {"id": 1, "name": "Corsa", "brand": "Opel"},
                {"id": 2, "name": "Clio", "brand": "Renault"},
            ],
        )
    engine.dispose()
    return str(tmp_path / "origin.db"), table


def _run(origin, fields=None, filters=None):
    path, table = origin
    with patch.object(
        connectors, "_get_engine", side_effect=lambda *a, **k: sqlalchemy.create_engine(f"sqlite:///{path}")
    ) as get_engine, patch.object(connectors, "_get_model", return_value=table) as get_model:
        rows = list(
            connectors.get_resource_data(
                uri=URI,
                object_location="resource",
                object_location_schema=None,
                filters=filters or {},
                like="",
                fields=fields or [],
                sort=[],
            )
        )
    return rows, get_engine, get_model


def test_single_engine_and_single_reflection(origin):
    _rows, get_engine, get_model = _run(origin)

    assert get_model.call_count == 1
    assert get_engine.call_count == 1


def test_keys_and_order_without_fields(origin):
    rows, _, _ = _run(origin)

    assert [list(r) for r in rows] == [["id", "name", "brand"]] * 2
    assert rows[0] == {"id": 1, "name": "Corsa", "brand": "Opel"}


def test_keys_and_order_follow_fields(origin):
    rows, _, _ = _run(origin, fields=["brand", "id"])

    assert [list(r) for r in rows] == [["brand", "id"]] * 2
    assert rows[1] == {"brand": "Renault", "id": 2}


def test_empty_result_returns_no_rows(origin):
    rows, get_engine, get_model = _run(origin, filters={"name": "nope"})

    assert rows == []
    assert get_model.call_count == 1


def test_empty_result_still_reports_column_names(origin):
    path, table = origin
    with patch.object(
        connectors, "_get_engine", side_effect=lambda *a, **k: sqlalchemy.create_engine(f"sqlite:///{path}")
    ), patch.object(connectors, "_get_model", return_value=table):
        data, names = connectors._get_session_data_and_columns(
            URI, "resource", None, {"name": "nope"}, "", ["brand", "id"], []
        )
        plain = connectors.get_session_data(URI, "resource", None, {"name": "nope"}, "", ["brand", "id"], [])

    assert list(data) == []
    assert names == ["brand", "id"]
    assert list(plain) == []
