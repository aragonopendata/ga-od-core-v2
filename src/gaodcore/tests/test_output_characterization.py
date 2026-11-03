"""Characterization of the public output (JSON, CSV, SCSV, XML, YAML, XLSX, GeoJSON) as it is TODAY.

These tests pin the bytes (or, for XLSX, the cell values and types) that ``/GA_OD_Core/download`` and
``/GA_OD_Core/preview`` return, oddities included, so that changes to the serialization pipeline
(``connectors.get_resource_data``, ``utils.get_return_list``, ``utils.modify_header``, ``get_response_xlsx``)
cannot alter the public contract unnoticed. Oddities are marked with ``QUIRK``; they are documented, not endorsed.

Two data sources are used, none of them needs Docker:

* ``rich_resource``: an empty SQLite table is the shared engine/model, and ``connectors._get_session_data_and_columns``
  is patched to return Python values a real database driver can return (``Decimal``, tz-aware ``datetime``, ``UUID``,
  ``time``, ``bytes``, NaN ...). Everything after the query (normalization, serialization, renderers) is real.
* ``sqlite_resource``: a real SQLite table read through reflection and a real query.

Snapshot bodies live in ``fixtures_output/`` and are compared byte for byte.
"""

import datetime as dt
import decimal
import json
import uuid
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from geoalchemy2 import Geometry
from openpyxl import load_workbook
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text

import connectors
from gaodcore import views as gaodcore_views

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"
FIXTURES = Path(__file__).parent / "fixtures_output"
NAN = float("nan")

RICH_COLUMNS = [
    "id", "dec_int", "dec_frac", "dec_prec", "flt", "flt_int", "flt_big", "flt_nan", "d", "dt_naive", "dt_utc",
    "dt_tz", "t", "u", "nothing", "ctrl", "uni", "special", "raw_bytes", "flag",
]  # fmt: skip


def rich_rows():
    return [
        (
            1, decimal.Decimal("5"), decimal.Decimal("12.50"), decimal.Decimal("1.0000000000000001"), 1.5, 2.0, 1e20,
            NAN, dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 13, 45, 10, 123456),
            dt.datetime(2024, 2, 29, 13, 45, 10, tzinfo=dt.timezone.utc),
            dt.datetime(2024, 2, 29, 13, 45, 10, 500000, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            dt.time(8, 5, 3, 250000), uuid.UUID("12345678-1234-5678-1234-567812345678"), None,
            "a\x00b\nc\td\x7fe\x1b  ", "Ñandú ✓ 日本", 'x,y;z "q" <&>', b"caf\xc3\xa9", True,
        ),
        (
            2, decimal.Decimal("-3.00"), decimal.Decimal("-0.5"), decimal.Decimal("0.1234567890123456789"),
            0.1 + 0.2, -0.0, 123456789012.0, 2.5, dt.date(1999, 12, 31), dt.datetime(1999, 12, 31, 23, 59, 59),
            dt.datetime(1999, 12, 31, 23, 59, 59, 7, tzinfo=dt.timezone.utc),
            dt.datetime(1999, 12, 31, 23, 59, 59, tzinfo=dt.timezone(dt.timedelta(hours=-5, minutes=-30))),
            dt.time(0, 0, 0), uuid.UUID("00000000-0000-0000-0000-000000000000"), None,
            "  pad\r\nline2  ", "ß ñ é", "'single' & \"double\"", b"", False,
        ),
        (3,) + (None,) * 18 + (None,),
    ]  # fmt: skip


@pytest.fixture(autouse=True)
def _fixed_environment(settings):
    """The output must not depend on the machine: pin the time zone and skip the size bookkeeping."""
    settings.TIME_ZONE = "Europe/Madrid"
    with patch.object(gaodcore_views, "update_resource_size"):
        yield


def _config(uri, location="things", schema=None):
    return SimpleNamespace(
        id=1,
        name="things",
        object_location=location,
        object_location_schema=schema,
        connector_config=SimpleNamespace(uri=uri),
    )


def _sqlite(tmp_path, statements):
    uri = f"sqlite:///{tmp_path / 'data.db'}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    engine.dispose()
    return uri


def _patched_rows(rows, columns):
    return patch.object(
        connectors, "_get_session_data_and_columns", side_effect=lambda *a, **k: (list(rows), list(columns))
    )


@pytest.fixture
def rich_resource(tmp_path):
    uri = _sqlite(tmp_path, ["CREATE TABLE things (id INTEGER PRIMARY KEY)"])
    with (
        patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)),
        _patched_rows(rich_rows(), RICH_COLUMNS),
    ):
        yield


@pytest.fixture
def sqlite_resource(tmp_path):
    uri = _sqlite(
        tmp_path,
        [
            "CREATE TABLE things (id INTEGER PRIMARY KEY, price NUMERIC(10,2), ratio FLOAT, born DATE, "
            "seen DATETIME, at TIME, flag BOOLEAN, note TEXT, uid CHAR(36))",
            "INSERT INTO things VALUES (2, 3.50, 0.25, '2020-01-02', '2020-01-02 03:04:05.123456', '03:04:05', 0, "
            "'ñ ✓', NULL)",
            "INSERT INTO things VALUES (1, 7, 2.0, '2021-12-31', '2021-12-31 23:59:59', '23:59:59', 1, "
            "'a'||char(0)||'b'||char(10)||'c  ', '12345678-1234-5678-1234-567812345678')",
        ],
    )
    with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)):
        yield


def _get(client, endpoint, fmt, **params):
    return client.get(endpoint, {"resource_id": 1, "format": fmt, "sort": "id", **params})


def _snapshot(name):
    return (FIXTURES / name).read_bytes()


def _xlsx_cells(response):
    # XLSX is a streaming FileResponse, which has no ``.content``.
    sheet = load_workbook(BytesIO(b"".join(response.streaming_content))).active
    return [[(cell.value, cell.data_type) for cell in row] for row in sheet.iter_rows()]


# --------------------------------------------------------------------------------------------------------------------
# Rich dataset: every column type, every format, byte for byte.
# --------------------------------------------------------------------------------------------------------------------

BODY_FORMATS = [
    ("json", "application/json", "rich.json"),
    ("csv", "text/csv", "rich.csv"),
    ("scsv", "text/csv", "rich.scsv"),
    ("xml", "application/xml; charset=utf-8", "rich.xml"),
    ("yaml", "application/yaml; charset=utf-8", "rich.yaml"),
]


@pytest.mark.django_db
@pytest.mark.parametrize("fmt,content_type,snapshot", BODY_FORMATS)
def test_download_rich_body_is_byte_exact(client, rich_resource, fmt, content_type, snapshot):
    response = _get(client, DOWNLOAD, fmt)
    assert response.status_code == 200
    assert response["Content-Type"] == content_type
    assert response["Content-Disposition"] == f'attachment; filename="things.{fmt}"'
    assert response.content == _snapshot(snapshot)


@pytest.mark.django_db
@pytest.mark.parametrize("fmt,content_type,snapshot", BODY_FORMATS)
def test_preview_rich_body_is_the_same_without_attachment(client, rich_resource, fmt, content_type, snapshot):
    response = _get(client, PREVIEW, fmt)
    assert response.status_code == 200
    assert response["Content-Type"] == content_type
    assert not response.has_header("Content-Disposition")
    assert response.content == _snapshot(snapshot)


@pytest.mark.django_db
@pytest.mark.parametrize("zone", ["Europe/Madrid", "Pacific/Auckland", "UTC"])
def test_rich_output_does_not_depend_on_time_zone(client, rich_resource, settings, zone):
    # QUIRK: datetimes are never converted to TIME_ZONE; naive stays naive, aware keeps its own offset.
    settings.TIME_ZONE = zone
    assert _get(client, DOWNLOAD, "json").content == _snapshot("rich.json")


@pytest.mark.django_db
def test_geojson_and_json_disagree_on_float_with_decimals():
    """QUIRK (documented in the GeoJSON snapshot): a float with decimals is a string inside a FeatureCollection
    ("flt":"1.5") but a number in plain JSON ("flt":1.5). See ``fixtures_output/geojson.json``."""
    assert b'"flt":"1.5"' in _snapshot("geojson.json")
    assert b'"flt":1.5' in _snapshot("rich.json")


def test_yaml_quirk_time_of_day_is_unquoted():
    """QUIRK: ``00:00:00`` is emitted unquoted, which YAML 1.1 parsers read as the integer 0."""
    assert b"  t: 00:00:00\n" in _snapshot("rich.yaml")


@pytest.mark.django_db
def test_rich_json_semantics(client, rich_resource):
    """The same snapshot read as data, with the oddities spelled out."""
    rows = {row["id"]: row for row in json.loads(_get(client, DOWNLOAD, "json").content)}
    first, second, third = rows[1], rows[2], rows[3]
    # QUIRK: Decimal/float with a zero fractional part become int; the scale is lost (12.50 stays a string).
    assert first["dec_int"] == 5 and isinstance(first["dec_int"], int)
    assert second["dec_int"] == -3
    assert first["dec_frac"] == "12.50"  # QUIRK: Decimal with decimals is a string, floats are numbers.
    # QUIRK: Decimal goes through math.modf (a float): 1.0000000000000001 looks integral and becomes 1.
    assert first["dec_prec"] == 1
    assert second["dec_prec"] == "0.1234567890123456789"
    assert first["flt"] == 1.5 and second["flt"] == 0.30000000000000004
    assert first["flt_int"] == 2 and second["flt_int"] == 0  # QUIRK: 2.0 -> 2, -0.0 -> 0.
    assert first["flt_big"] == 10**20 and second["flt_big"] == 123456789012
    assert first["flt_nan"] is None  # QUIRK: NaN -> null.
    # QUIRK: milliseconds only (microseconds truncated), 'Z' only for exactly +00:00, other offsets kept.
    assert first["dt_naive"] == "2024-02-29T13:45:10.123"
    assert first["dt_utc"] == "2024-02-29T13:45:10Z"
    assert first["dt_tz"] == "2024-02-29T13:45:10.500+02:00"
    assert second["dt_utc"] == "1999-12-31T23:59:59.000Z"  # QUIRK: 7 microseconds show as ".000".
    assert second["dt_tz"] == "1999-12-31T23:59:59-05:30"
    assert first["t"] == "08:05:03.250" and second["t"] == "00:00:00"
    assert first["u"] == "12345678-1234-5678-1234-567812345678"
    # QUIRK: control characters are removed (tab and CR survive the regex) and the result is stripped.
    assert first["ctrl"] == "abc\tde"
    assert second["ctrl"] == "pad\rline2"  # "\n" is removed, "\r" is kept
    assert first["uni"] == "Ñandú ✓ 日本"
    assert first["raw_bytes"] == "café"  # QUIRK: bytes are decoded as UTF-8.
    assert first["flag"] is True and second["flag"] is False
    assert all(value is None for key, value in third.items() if key != "id")


# --------------------------------------------------------------------------------------------------------------------
# XLSX: cell values and types (openpyxl), not bytes.
# --------------------------------------------------------------------------------------------------------------------

@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
def test_rich_xlsx_cells(client, rich_resource, endpoint):
    response = _get(client, endpoint, "xlsx")
    assert response.status_code == 200
    assert "spreadsheetml" in response["Content-Type"]
    assert response["Content-Disposition"] == 'attachment; filename="things.xlsx"'  # also for preview
    cells = _xlsx_cells(response)
    assert cells[0] == [(name, "s") for name in RICH_COLUMNS]
    # QUIRK: dates, datetimes and times are TEXT cells (ISO strings), never Excel dates; UUID and Decimal with
    # decimals are text; booleans are numbers (1/0), not Excel booleans; NULL and NaN are empty cells.
    # The ctrl/uni/special strings are in the rows below.
    assert cells[1] == [
        (1, "n"), (5, "n"), ("12.50", "s"), (1, "n"), (1.5, "n"), (2, "n"), (10**20, "n"), (None, "n"),
        ("2024-02-29", "s"), ("2024-02-29T13:45:10.123", "s"), ("2024-02-29T13:45:10Z", "s"),
        ("2024-02-29T13:45:10.500+02:00", "s"), ("08:05:03.250", "s"), ("12345678-1234-5678-1234-567812345678", "s"),
        (None, "n"), ("abc\tde", "s"), ("Ñandú ✓ 日本", "s"), ('x,y;z "q" <&>', "s"), ("café", "s"), (1, "n"),
    ]  # fmt: skip
    assert cells[2] == [
        (2, "n"), (-3, "n"), ("-0.5", "s"), ("0.1234567890123456789", "s"), (0.3, "n"), (0, "n"),  # QUIRK: XLSX keeps 15 significant digits, 0.1+0.2 is 0.3
        # QUIRK: a carriage return is stored escaped as _x000D_ (xlsxwriter) and read back literally.
        (123456789012, "n"), (2.5, "n"), ("1999-12-31", "s"), ("1999-12-31T23:59:59", "s"),
        ("1999-12-31T23:59:59.000Z", "s"), ("1999-12-31T23:59:59-05:30", "s"), ("00:00:00", "s"),
        ("00000000-0000-0000-0000-000000000000", "s"), (None, "n"), ("pad_x000D_line2", "s"), ("ß ñ é", "s"),
        ("'single' & \"double\"", "s"), (None, "n"), (0, "n"),  # QUIRK: "" is an empty cell, like NULL
    ]  # fmt: skip
    assert cells[3] == [(3, "n")] + [(None, "n")] * 19
    assert len(cells) == 4


# --------------------------------------------------------------------------------------------------------------------
# columns (modify_header) and fields.
# --------------------------------------------------------------------------------------------------------------------

NULLABLE_COLUMNS = ["id", "maybe", "flag_n", "dec", "s"]
NULLABLE_ROWS = [
    (1, 10, True, decimal.Decimal("1.5"), "x"),
    (2, None, None, decimal.Decimal("2"), None),
    (3, 30, False, None, "z"),
]
RENAMED = {"columns": "a,b,c,d,e"}


@pytest.fixture
def nullable_resource(tmp_path):
    uri = _sqlite(tmp_path, ["CREATE TABLE things (id INTEGER PRIMARY KEY)"])
    with (
        patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)),
        _patched_rows(NULLABLE_ROWS, NULLABLE_COLUMNS),
    ):
        yield


@pytest.mark.django_db
def test_without_columns_nulls_stay_empty_and_ints_stay_ints(client, nullable_resource):
    assert _get(client, DOWNLOAD, "csv").content == (
        b"id,maybe,flag_n,dec,s\r\n1,10,True,1.5,x\r\n2,,,2,\r\n3,30,False,,z\r\n"
    )
    assert json.loads(_get(client, DOWNLOAD, "json").content) == [
        {"id": 1, "maybe": 10, "flag_n": True, "dec": "1.5", "s": "x"},
        {"id": 2, "maybe": None, "flag_n": None, "dec": 2, "s": None},
        {"id": 3, "maybe": 30, "flag_n": False, "dec": None, "s": "z"},
    ]


def _renamed_like_plain(client, fmt, header_from, header_to):
    """The plain response of the same request with only the header replaced."""
    plain = _get(client, DOWNLOAD, fmt).content
    return plain.replace(header_from, header_to, 1)


@pytest.mark.django_db
def test_columns_rename_csv_keeps_values_of_plain_output(client, nullable_resource):
    # With columns the body is exactly the plain one: only the header changes (10 stays 10, NULL stays empty).
    assert _get(client, DOWNLOAD, "csv", **RENAMED).content == _renamed_like_plain(
        client, "csv", b"id,maybe,flag_n,dec,s", b"a,b,c,d,e"
    )
    assert _get(client, DOWNLOAD, "csv", **RENAMED).content == (
        b"a,b,c,d,e\r\n1,10,True,1.5,x\r\n2,,,2,\r\n3,30,False,,z\r\n"
    )
    assert _get(client, DOWNLOAD, "scsv", **RENAMED).content == (
        b"a;b;c;d;e\r\n1;10;True;1.5;x\r\n2;;;2;\r\n3;30;False;;z\r\n"
    )


@pytest.mark.django_db
def test_columns_rename_xml_keeps_values_of_plain_output(client, nullable_resource):
    plain = _get(client, DOWNLOAD, "xml").content
    expected = plain
    for old, new in zip(NULLABLE_COLUMNS, "abcde"):
        expected = expected.replace(f"<{old}>".encode(), f"<{new}>".encode()).replace(
            f"</{old}>".encode(), f"</{new}>".encode()
        )
    assert _get(client, DOWNLOAD, "xml", **RENAMED).content == expected
    assert b"nan" not in expected and b"10.0" not in expected


@pytest.mark.django_db
def test_columns_rename_json_with_nullable_int_column_keeps_values(client, nullable_resource):
    plain = json.loads(_get(client, DOWNLOAD, "json").content)
    renamed = json.loads(_get(client, DOWNLOAD, "json", **RENAMED).content)
    assert renamed == [dict(zip("abcde", row.values())) for row in plain]
    assert [row["b"] for row in renamed] == [10, None, 30]


@pytest.mark.django_db
def test_columns_rename_xlsx_cells(client, nullable_resource):
    expected_rows = [
        [(1, "n"), (10, "n"), (1, "n"), ("1.5", "s"), ("x", "s")],
        [(2, "n"), (None, "n"), (None, "n"), (2, "n"), (None, "n")],
        [(3, "n"), (30, "n"), (0, "n"), (None, "n"), ("z", "s")],
    ]
    renamed = _xlsx_cells(_get(client, DOWNLOAD, "xlsx", **RENAMED))
    assert renamed[0] == [(name, "s") for name in "abcde"]
    assert renamed[1:] == expected_rows
    plain = _xlsx_cells(_get(client, DOWNLOAD, "xlsx"))
    assert plain[0] == [(name, "s") for name in NULLABLE_COLUMNS]
    assert plain[1:] == expected_rows  # QUIRK: bool column is numeric 1/0 and None is an empty cell, in both.


@pytest.mark.django_db
def test_columns_rename_without_nulls_keeps_native_types(client, tmp_path):
    columns = ["id", "ratio", "name", "flag", "price"]
    rows = [(1, 0.5, "a", True, decimal.Decimal("1.25")), (2, 1.5, "b", False, decimal.Decimal("3"))]
    uri = _sqlite(tmp_path, ["CREATE TABLE things (id INTEGER PRIMARY KEY)"])
    with (
        patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)),
        _patched_rows(rows, columns),
    ):
        response = _get(client, DOWNLOAD, "json", columns="i,r,n,f,p")
    assert response.content == (
        b'[{"i":1,"r":0.5,"n":"a","f":true,"p":"1.25"},{"i":2,"r":1.5,"n":"b","f":false,"p":3}]'
    )


@pytest.mark.django_db
def test_columns_count_must_match(client, rich_resource):
    response = _get(client, DOWNLOAD, "csv", columns="only_one")
    assert response.status_code == 400


# --------------------------------------------------------------------------------------------------------------------
# Real SQLite table through reflection: types as a driver gives them, fields and columns.
# --------------------------------------------------------------------------------------------------------------------

@pytest.mark.django_db
def test_sqlite_json(client, sqlite_resource):
    # QUIRKS: NUMERIC 7.00 -> 7 but 3.50 -> "3.50"; FLOAT 2.0 -> 2; control chars stripped; microseconds cut to ms;
    # SQLite has no time zone, so naive datetimes; BOOLEAN is a real JSON boolean.
    assert _get(client, DOWNLOAD, "json").content == (
        b'[{"id":1,"price":7,"ratio":2,"born":"2021-12-31","seen":"2021-12-31T23:59:59","at":"23:59:59",'
        b'"flag":true,"note":"abc","uid":"12345678-1234-5678-1234-567812345678"},'
        b'{"id":2,"price":"3.50","ratio":0.25,"born":"2020-01-02","seen":"2020-01-02T03:04:05.123",'
        b'"at":"03:04:05","flag":false,"note":"\\u00f1 \\u2713","uid":null}]'
    ).replace(b"\\u00f1 \\u2713", "ñ ✓".encode())


@pytest.mark.django_db
def test_sqlite_csv_and_xml(client, sqlite_resource):
    assert _get(client, DOWNLOAD, "csv").content == (
        "id,price,ratio,born,seen,at,flag,note,uid\r\n"
        "1,7,2,2021-12-31,2021-12-31T23:59:59,23:59:59,True,abc,12345678-1234-5678-1234-567812345678\r\n"
        "2,3.50,0.25,2020-01-02,2020-01-02T03:04:05.123,03:04:05,False,ñ ✓,\r\n"
    ).encode()
    assert _get(client, DOWNLOAD, "xml").content == (
        '<?xml version="1.0" encoding="utf-8"?>\n<root>'
        "<list-item><id>1</id><price>7</price><ratio>2</ratio><born>2021-12-31</born>"
        "<seen>2021-12-31T23:59:59</seen><at>23:59:59</at><flag>True</flag><note>abc</note>"
        "<uid>12345678-1234-5678-1234-567812345678</uid></list-item>"
        "<list-item><id>2</id><price>3.50</price><ratio>0.25</ratio><born>2020-01-02</born>"
        "<seen>2020-01-02T03:04:05.123</seen><at>03:04:05</at><flag>False</flag><note>ñ ✓</note>"
        "<uid></uid></list-item></root>"
    ).encode()


@pytest.mark.django_db
def test_sqlite_xlsx_cells(client, sqlite_resource):
    cells = _xlsx_cells(_get(client, DOWNLOAD, "xlsx"))
    assert cells[0] == [(n, "s") for n in ("id", "price", "ratio", "born", "seen", "at", "flag", "note", "uid")]
    assert cells[1] == [
        (1, "n"), (7, "n"), (2, "n"), ("2021-12-31", "s"), ("2021-12-31T23:59:59", "s"), ("23:59:59", "s"),
        (1, "n"), ("abc", "s"), ("12345678-1234-5678-1234-567812345678", "s"),
    ]  # fmt: skip
    assert cells[2] == [
        (2, "n"), ("3.50", "s"), (0.25, "n"), ("2020-01-02", "s"), ("2020-01-02T03:04:05.123", "s"),
        ("03:04:05", "s"), (0, "n"), ("ñ ✓", "s"), (None, "n"),
    ]  # fmt: skip


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
def test_fields_select_and_order_columns(client, sqlite_resource, endpoint):
    assert _get(client, endpoint, "json", fields="note,id").content == (
        '[{"note":"abc","id":1},{"note":"ñ ✓","id":2}]'.encode()
    )
    assert _get(client, endpoint, "csv", fields="note,id").content == "note,id\r\nabc,1\r\nñ ✓,2\r\n".encode()
    assert _get(client, endpoint, "csv", fields="note,id", columns="n,i").content == (
        "n,i\r\nabc,1\r\nñ ✓,2\r\n".encode()
    )
    assert _get(client, endpoint, "json", fields="note,id", columns="n,i").content == (
        '[{"n":"abc","i":1},{"n":"ñ ✓","i":2}]'.encode()
    )
    assert _get(client, endpoint, "xml", fields="note,id", columns="n,i").content == (
        '<?xml version="1.0" encoding="utf-8"?>\n<root><list-item><n>abc</n><i>1</i></list-item>'
        "<list-item><n>ñ ✓</n><i>2</i></list-item></root>"
    ).encode()


@pytest.mark.django_db
def test_fields_and_columns_xlsx(client, sqlite_resource):
    cells = _xlsx_cells(_get(client, DOWNLOAD, "xlsx", fields="note,id", columns="n,i"))
    assert cells == [[("n", "s"), ("i", "s")], [("abc", "s"), (1, "n")], [("ñ ✓", "s"), (2, "n")]]


@pytest.mark.django_db
def test_empty_result_per_format(client, tmp_path):
    uri = _sqlite(tmp_path, ["CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)"])
    with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)):
        assert _get(client, DOWNLOAD, "json").content == b"[]"
        # QUIRK: with no rows there is no header either in CSV and XLSX.
        assert _get(client, DOWNLOAD, "csv").content == b""
        assert _xlsx_cells(_get(client, DOWNLOAD, "xlsx")) == []


# --------------------------------------------------------------------------------------------------------------------
# GeoJSON through the endpoint (the connector level is covered in test_resource_data_feature.py).
# --------------------------------------------------------------------------------------------------------------------

POINT = '{"type":"Point","coordinates":[-0.87,41.65]}'


def _geo_rows():
    # Property columns first and the ST_AsGeoJSON text last, as the query returns them.
    return [
        (1, decimal.Decimal("5"), decimal.Decimal("12.50"), 1.5, 2.0, dt.date(2024, 2, 29),
         dt.datetime(2024, 2, 29, 13, 45, 10, 123456), dt.datetime(2024, 2, 29, 13, 45, 10, tzinfo=dt.timezone.utc),
         dt.time(8, 5, 3), "a\x00b\nc  ", None, True, POINT),
        (2, None, None, None, None, None, None, None, None, "Ñandú", None, False, None),
    ]  # fmt: skip


def _geo_model():
    return Table(
        "places",
        MetaData(),
        Column("id", Integer),
        Column("dec_int", Integer),
        Column("dec_frac", Integer),
        Column("flt", Integer),
        Column("flt_int", Integer),
        Column("d", Integer),
        Column("dt_naive", Integer),
        Column("dt_utc", Integer),
        Column("t", Integer),
        Column("txt", String),
        Column("nothing", String),
        Column("flag", Integer),
        Column("geom", Geometry("POINT", srid=4326)),
    )


def _get_geojson(client, rows, model):
    query = MagicMock()
    for method in ("filter_by", "filter", "order_by", "offset", "limit"):
        getattr(query, method).return_value = query
    query.all.return_value = rows
    session = MagicMock()
    session.query.return_value = query
    config = _config("postgresql://u:p@example.invalid:5432/db", "places", "public")
    with (
        patch.object(gaodcore_views, "_get_resource", return_value=config),
        patch.object(connectors, "_get_engine", return_value=MagicMock()),
        patch.object(connectors, "_get_model", return_value=model),
        patch.object(connectors, "sessionmaker", return_value=lambda: session),
    ):
        return client.get(DOWNLOAD, {"resource_id": 1, "format": "json"})


@pytest.mark.django_db
def test_geojson_uuid_property_crashes(client):
    # QUIRK: the FeatureCollection code sends UUID through math.modf, so a UUID property is a TypeError (500).
    model = Table("places", MetaData(), Column("id", Integer), Column("geom", Geometry("POINT", srid=4326)))
    with pytest.raises(TypeError):
        _get_geojson(client, [(uuid.UUID("12345678-1234-5678-1234-567812345678"), POINT)], model)


@pytest.mark.django_db
def test_geojson_feature_collection_with_null_geometry(client):
    response = _get_geojson(client, _geo_rows(), _geo_model())
    assert response.status_code == 200
    assert response["Content-Disposition"] == 'attachment; filename="things.json"'
    assert response.content == _snapshot("geojson.json")
