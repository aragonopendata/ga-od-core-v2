"""Server version reported by probe_uri (read from the connection the health check already opens)."""

import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine

from connectors import (
    NotImplementedSchemaError,
    ServerVersion,
    _server_version,
    is_database_uri,
    probe_uri,
    validate_uri,
)


@pytest.mark.parametrize(
    "uri",
    [
        "oracle://user:pw@host:1521/?service_name=X",
        "oracle+oracledb://user:pw@host:1521/?service_name=X",
        "mssql://user:pw@host/db",
        "mssql+pyodbc://user:pw@host/db",
        "postgresql://user:pw@host/db",
        "mysql://user:pw@host/db",
        "sqlite:////tmp/file.db",
    ],
)
def test_database_uris_including_aliases(uri):
    assert is_database_uri(uri)


@pytest.mark.parametrize(
    "uri",
    ["https://example.org/data.csv", "http://example.org/api", "internal://transports/zaragoza/stops"],
)
def test_non_database_uris(uri):
    assert not is_database_uri(uri)


def _connection(name, info, **attrs):
    return SimpleNamespace(dialect=SimpleNamespace(name=name, server_version_info=info, **attrs))


@pytest.mark.parametrize(
    "connection, expected",
    [
        (_connection("oracle", (19, 0, 0, 0, 0)), ServerVersion("oracle", "19.0.0.0.0")),
        (_connection("postgresql", (16, 4)), ServerVersion("postgresql", "16.4")),
        (_connection("mssql", (15, 0, 2000, 5)), ServerVersion("mssql", "15.0.2000.5")),
        # MySQL reports suffixes such as "5.7.44-log": only the numbers are kept.
        (_connection("mysql", (5, 7, 44, "log"), is_mariadb=False), ServerVersion("mysql", "5.7.44")),
        (_connection("mysql", (10, 6, 12, "MariaDB"), is_mariadb=True), ServerVersion("mariadb", "10.6.12")),
    ],
)
def test_server_version_from_dialect(connection, expected):
    assert _server_version(connection) == expected


@pytest.mark.parametrize("info", [None, (), ("unknown",)])
def test_server_without_version(info):
    assert _server_version(_connection("mssql", info)) is None


def test_server_version_never_raises():
    class BrokenDialect:
        name = "oracle"

        @property
        def server_version_info(self):
            raise RuntimeError("boom")

    assert _server_version(SimpleNamespace(dialect=BrokenDialect())) is None


def test_probe_sqlite_returns_its_version(tmp_path):
    uri = f"sqlite:///{tmp_path / 'probe.db'}"

    assert probe_uri(uri) == ServerVersion("sqlite", sqlite3.sqlite_version)
    assert validate_uri(uri) is None


def test_probe_http_source_has_no_version():
    """An API is loaded into a local SQLite: its version is ours, not the source's."""
    with patch("connectors._get_engine", return_value=create_engine("sqlite:///:memory:")):
        assert probe_uri("https://example.org/data.csv") is None


def test_probe_http_source_keeps_its_errors():
    with patch("connectors._get_engine", side_effect=ValueError("bad content")):
        with pytest.raises(ValueError):
            probe_uri("https://example.org/data.csv")


def test_probe_unknown_scheme_still_fails():
    with pytest.raises(NotImplementedSchemaError):
        validate_uri("internal://transports/zaragoza/stops")
