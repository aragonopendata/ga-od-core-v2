"""MSSQL pagination: sort, limit and offset are applied, compiled with the MSSQL dialect."""

import pytest
from rest_framework.exceptions import ValidationError
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.dialects import mssql, postgresql
from sqlalchemy.orm import sessionmaker

import connectors
from connectors import OpenResource, OrderBy, _build_mssql_query, _get_columns, _query_session_data


def _table(with_pk=True, text_first=False, only_unsortable=False):
    cols = []
    if text_first or only_unsortable:
        cols.append(Column("notes", mssql.TEXT()))
        cols.append(Column("blob", mssql.IMAGE()))
    if not only_unsortable:
        cols.append(Column("id", Integer, primary_key=with_pk))
        cols.append(Column("name", String(20)))
    cols.append(Column("legacy", mssql.NTEXT()))
    return Table("things", MetaData(), *cols)


def _modern_dialect():
    """MSSQL 2012+ dialect, as a live connection configures it: OFFSET ... FETCH instead of ROW_NUMBER()."""
    dialect = mssql.dialect()
    dialect._supports_offset_fetch = True
    return dialect


def _sql(table, sort=(), limit=None, offset=0, dialect=None):
    column_dict = {c.key: c for c in table.columns}
    columns = _get_columns(column_dict, [])
    session = sessionmaker()()
    query = _build_mssql_query(session, table, column_dict, columns, {}, [], list(sort), limit, offset)
    return str(query.statement.compile(dialect=dialect or mssql.dialect(), compile_kwargs={"literal_binds": True})), query


def test_no_limit_no_offset_no_sort_is_plain_query():
    sql, _ = _sql(_table())
    assert "ORDER BY" not in sql
    assert "TOP" not in sql and "OFFSET" not in sql


def test_sort_without_pagination_applies_sort_only():
    sql, _ = _sql(_table(), sort=[OrderBy("name", False)])
    assert "ORDER BY things.name DESC" in sql
    assert "OFFSET" not in sql and "TOP" not in sql


def test_sort_on_unsortable_column_is_skipped():
    sql, _ = _sql(_table(text_first=True), sort=[OrderBy("notes", True)])
    assert "ORDER BY" not in sql


def test_unknown_sort_field_raises():
    with pytest.raises(connectors.SortFieldNoExistsError):
        _sql(_table(), sort=[OrderBy("nope", True)])


def test_limit_with_pk_orders_by_pk():
    sql, _ = _sql(_table(), limit=1000)
    assert "TOP" in sql and "ORDER BY things.id" in sql


def test_limit_and_offset_with_sort():
    sql, _ = _sql(_table(), sort=[OrderBy("name", True)], limit=10, offset=20, dialect=_modern_dialect())
    assert "ORDER BY things.name" in sql
    assert "OFFSET 20 ROWS" in sql and "FETCH FIRST 10 ROWS ONLY" in sql
    legacy, _ = _sql(_table(), sort=[OrderBy("name", True)], limit=10, offset=20)
    assert "ROW_NUMBER() OVER (ORDER BY things.name)" in legacy


def test_offset_without_pk_uses_first_sortable_column():
    sql, _ = _sql(_table(with_pk=False, text_first=True), limit=5, offset=5, dialect=_modern_dialect())
    assert "ORDER BY things.id" in sql
    assert "notes" not in sql.split("ORDER BY")[1]
    assert "OFFSET 5 ROWS" in sql


def test_offset_only_orders_and_offsets():
    sql, _ = _sql(_table(), offset=7, dialect=_modern_dialect())
    assert "ORDER BY things.id" in sql and "OFFSET 7 ROWS" in sql


def test_no_sortable_column_limit_uses_top():
    sql, _ = _sql(_table(only_unsortable=True), limit=10)
    assert "TOP 10" in sql and "ORDER BY" not in sql


def test_no_sortable_column_offset_uses_top_limit_plus_offset():
    sql, _ = _sql(_table(only_unsortable=True), limit=10, offset=5)
    assert "TOP 15" in sql and "OFFSET" not in sql and "ORDER BY" not in sql


def test_preview_cap_compiles_to_top_1000():
    sql, _ = _sql(_table(), limit=1000)
    assert "TOP 1000" in sql


def test_postgresql_sql_is_unchanged():
    # The generic path used by every other dialect builds ORDER BY / OFFSET / LIMIT the same way as before.
    table = _table()
    session = sessionmaker()()
    query = (
        session.query(table)
        .order_by(table.c.name)
        .with_entities(table.c.id.label("id"))
        .offset(5)
        .limit(10)
    )
    sql = str(query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert " ".join(sql.split()) == "SELECT things.id AS id FROM things ORDER BY things.name LIMIT 10 OFFSET 5"


def _run(tmp_path, ddl, limit, offset, sort=()):
    uri = "mssql+pyodbc://u:p@h/db"
    engine = create_engine(f"sqlite:///{tmp_path / 'x.db'}")
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("INSERT INTO t (id) VALUES (1),(2),(3),(4),(5)"))
    table = Table("t", MetaData(), autoload_with=engine)
    return _query_session_data(
        OpenResource(engine, table), uri, "t", None, {}, {}, [], list(sort), limit, offset, None
    )


def test_end_to_end_limit_offset(tmp_path):
    data, names = _run(tmp_path, "CREATE TABLE t (id INTEGER PRIMARY KEY)", 2, 1)
    assert names == ["id"]
    assert [row[0] for row in data] == [2, 3]


def test_end_to_end_unknown_sort_is_validation_error(tmp_path):
    with pytest.raises(ValidationError):
        _run(tmp_path, "CREATE TABLE t (id INTEGER PRIMARY KEY)", None, 0, [OrderBy("zz", True)])
