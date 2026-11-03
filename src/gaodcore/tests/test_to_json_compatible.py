"""The single-pass conversion must equal the JSON round trip it replaces."""
import collections
import datetime
import decimal
import enum
import json
import math
import uuid

import pytest
from django.utils.functional import lazy

from utils import _fix_null_values_for_xlsx, _to_json_compatible, get_return_list, serializerJsonEncoder


def _round_trip(x):
    return json.loads(json.dumps(x, default=serializerJsonEncoder))


def _same(a, b):
    """Equality that also checks Python types and treats NaN as equal to NaN."""
    assert type(a) is type(b), (a, b)
    if isinstance(a, dict):
        assert list(a.keys()) == list(b.keys())
        for k in a:
            _same(a[k], b[k])
    elif isinstance(a, list):
        assert len(a) == len(b)
        for x, y in zip(a, b):
            _same(x, y)
    elif isinstance(a, float) and math.isnan(a):
        assert math.isnan(b)
    else:
        assert a == b
        if isinstance(a, float):
            assert math.copysign(1, a) == math.copysign(1, b)


class _Color(str, enum.Enum):
    RED = "red"


class _Num(enum.IntEnum):
    ONE = 1


class _F(float):
    pass


_lazy_str = lazy(lambda: "lazy", str)

VALUES = [
    decimal.Decimal("1.50"), decimal.Decimal("NaN"), 1.5, -0.0, 1e300, float("nan"), float("inf"), float("-inf"),
    datetime.date(2024, 1, 2), datetime.datetime(2024, 1, 2, 3, 4, 5), datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
    datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
    datetime.datetime(2024, 1, 2, 3, 4, 5, 999, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
    datetime.time(1, 2, 3), datetime.time(1, 2, 3, 456789), datetime.timedelta(days=1, seconds=5),
    uuid.UUID("12345678-1234-5678-1234-567812345678"), None, True, False, 0, 10**30, "text", "", "ñ",
    _Color.RED, _Num.ONE, _F(2.5), _lazy_str(),
    {"a": 1, "b": [1, (2, 3), {"c": decimal.Decimal("2")}]}, [1, "a", None, [datetime.date(2020, 1, 1)]],
    (1, 2), {}, [], collections.OrderedDict(a=1),
    {2.5: "b", True: "c", None: "d", "x": "e", 7: "f"}, {1: "a", "1": "b"}, {float("nan"): 1, float("inf"): 2},
    {_Color.RED: 1}, {_Num.ONE: 1},
]


@pytest.mark.parametrize("value", VALUES, ids=[f"value{i}" for i in range(len(VALUES))])
def test_matches_round_trip(value):
    _same(_to_json_compatible(value), _round_trip(value))
    _same(_to_json_compatible({"k": value}), _round_trip({"k": value}))
    old = _fix_null_values_for_xlsx(_round_trip([{"k": value}]), False)
    _same(get_return_list([{"k": value}])[0], old[0])


def test_whole_rows_match():
    rows = [{f"c{i}": v for i, v in enumerate(VALUES[:30])}] * 3
    _same(_to_json_compatible(rows), _round_trip(rows))


@pytest.mark.parametrize(
    "value",
    [
        b"bytes", {1, 2}, object(), {datetime.date(2020, 1, 1): 1}, {(1, 2): 1},
    ],
    ids=["bytes", "set", "object", "date_key", "tuple_key"],
)
def test_errors_match(value):
    with pytest.raises(Exception) as expected:
        _round_trip([{"k": value}])
    with pytest.raises(type(expected.value)) as got:
        get_return_list([{"k": value}])
    assert str(got.value) == str(expected.value)


def test_aware_time_is_iso_with_offset():
    # D11: a time with a zone used to be a ValueError; it is ISO with its offset (milliseconds only).
    tz = datetime.timezone(datetime.timedelta(hours=2))
    data = [{"a": datetime.time(1, 2, 3, tzinfo=tz), "b": datetime.time(1, 2, 3, 456789, tzinfo=tz)}]
    assert get_return_list(data) == [{"a": "01:02:03+02:00", "b": "01:02:03.456+02:00"}]
    assert get_return_list(data) == _round_trip(data)


def test_circular_reference_raises_value_error():
    loop = []
    loop.append(loop)
    with pytest.raises(ValueError, match="Circular"):
        _to_json_compatible({"k": loop})


def test_shared_but_not_circular_reference_is_fine():
    shared = [1]
    _same(_to_json_compatible({"a": shared, "b": shared}), _round_trip({"a": shared, "b": shared}))


@pytest.mark.parametrize("xlsx", [False, True])
def test_fix_null_values_inplace_equals_copy(xlsx):
    import copy
    rows = [{"id": 1, "size": 3, "flag": True, "n": None, "f": float("nan")}, {"id": None, "size": 2, "s": "a"}]
    expected = _fix_null_values_for_xlsx(copy.deepcopy(rows), xlsx)
    got = _fix_null_values_for_xlsx(copy.deepcopy(rows), xlsx, inplace=True)
    _same(got, expected)
