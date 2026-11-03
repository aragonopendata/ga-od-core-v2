"""The per-type normalization of ``connectors.get_resource_data`` must equal the old cell by cell logic."""

import datetime as dt
import decimal
import math
import re
import uuid
from unittest.mock import patch

import pytest
from django.utils.functional import lazy
from sqlalchemy.types import Numeric

import connectors


def _old_sanitize(text):
    if re.search(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\n]", str(text)):
        text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\n]", "", text)
    try:
        text = text.decode("utf-8")
    except Exception:
        pass
    if isinstance(text, str):
        text = text.strip()
    return text


def _old_normalize(column):
    if isinstance(column, uuid.UUID):
        return str(column)
    elif (
        (isinstance(column, (decimal.Decimal, connectors.Promise)))
        or (isinstance(column, float))
        or (isinstance(column, Numeric))
    ):
        parte_decimal, _ = math.modf(column)
        if parte_decimal == 0.0:
            return int(column)
        return _old_sanitize(column)
    return _old_sanitize(column)


class _StrSub(str):
    pass


class _IntSub(int):
    pass


class _FloatSub(float):
    pass


class _DecSub(decimal.Decimal):
    pass


class _Weird:
    def __str__(self):
        return "weird\x00 "


_lazy_int = lazy(lambda: 5, int)()
_D = decimal.Decimal

# reflected type -> values a driver may return for it, including values that do not fit the type
VALUES = {
    "Integer": [0, 1, -7, 2**70, True, False, None, "12", "a\x00b", 3.0, 3.5, _D("3"), b"7"],
    "Numeric": [_D("1"), _D("1.5"), _D("-0"), _D("1234567890.123456789012345678901"), _D("NaN"), 2, 2.0,
                2.5, None, "1.5", _D("1E+3"), _D("0.0000001")],
    "Float": [0.0, -0.0, 1.0, 1.5, -2.25, 1e300, 1e-300, float("nan"), 3, None, _D("2.5"), "x\n"],
    "String": ["", " pad ", "a\x00b\x01c", "tab\tcr\rkeep", "nl\nx\x7fy", "\x0b\x0c", "ñandú ", None, 5,
               _D("1.5"), _D("2"), b"bytes", b"\xff\xfe", b"a\x00b\n", 1.5, 2.0, uuid.UUID(int=3),
               dt.date(2024, 1, 2)],
    "NullType": [1, 1.5, 2.0, _D("9.99"), "text\x00", b"\xc3\xb1 ", None, True, uuid.UUID(int=1),
                 dt.datetime(2024, 1, 2, 3, 4, 5, 123456), dt.time(1, 2, 3), dt.timedelta(days=1, seconds=5)],
    "DateTime": [dt.datetime(2024, 1, 2, 3, 4, 5), dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                 dt.date(2024, 1, 2), dt.time(1, 2), dt.timedelta(hours=3), None, "2024-01-01", 5],
    "Boolean": [True, False, None, 0, 1, "t", _D("1")],
    "LargeBinary": [b"", b"abc", b"\x00\x01", b"\xff", b" x ", bytearray(b"ab"), memoryview(b"ab"), None, "str"],
    "Subclasses": [_StrSub(" a\x00 "), _IntSub(4), _FloatSub(2.0), _FloatSub(2.5), _DecSub("3"), _DecSub("3.5"),
                   _Weird(), object.__new__(_Weird), _lazy_int, Numeric()],
}
IDS = [(k, i) for k, vs in VALUES.items() for i in range(len(vs))]


def _same(a, b):
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    if isinstance(a, decimal.Decimal) and isinstance(b, decimal.Decimal) and a.is_nan() and b.is_nan():
        return True
    return type(a) is type(b) and a == b


def _run(fn, v):
    try:
        return ("ok", fn(v))
    except Exception as err:  # errors must be the same too
        return ("err", type(err))


@pytest.mark.parametrize("kind,idx", IDS, ids=[f"{k}-{i}" for k, i in IDS])
def test_value_normalization_matches_old(kind, idx):
    value = VALUES[kind][idx]
    expected = _run(_old_normalize, value)
    got = _run(lambda v: connectors._NORMALIZERS.get(type(v), connectors._normalize_value)(v), value)
    assert got[0] == expected[0]
    if expected[0] == "ok":
        assert _same(got[1], expected[1]), (value, got, expected)
    else:
        assert got[1] is expected[1]


def test_inf_is_null():
    # D11: the old normalization raised OverflowError (500) for these; now they are None, through both paths.
    for v in (float("inf"), float("-inf"), _D("Infinity"), _D("-Infinity")):
        assert _run(_old_normalize, v)[0] == "err"
        assert connectors._NORMALIZERS.get(type(v), connectors._normalize_value)(v) is None
        assert connectors._normalize_value(v) is None


def test_sanitize_matches_old():
    for v in ["a\x00b", " x ", b"\xc3\xb1", b"\xff", 3, None, "t\tr\r", "n\nm"]:
        assert _same(connectors.sanitize_control_charcters(v), _old_sanitize(v))


def test_get_resource_data_builds_final_dicts_in_one_pass():
    rows = [(v1, v2) for v1, v2 in zip(VALUES["Integer"][:6], VALUES["String"][:6])]
    with patch.object(connectors, "_get_session_data_and_columns", return_value=(rows, ["a", "b"])):
        out = list(connectors.get_resource_data(
            uri="x", object_location="t", object_location_schema=None, filters={}, like={}, fields=[], sort=[]))
    expected = [{"a": _old_normalize(a), "b": _old_normalize(b)} for a, b in rows]
    assert len(out) == len(expected)
    for got, exp in zip(out, expected):
        assert list(got) == list(exp)
        assert all(_same(got[k], exp[k]) for k in exp)
