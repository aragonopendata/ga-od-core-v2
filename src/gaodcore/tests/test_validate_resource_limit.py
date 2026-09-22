"""Focused tests for the row limit `connectors.validate_resource()` forwards.

The internal helpers are mocked, so no database or HTTP origin is contacted.
The behavior under test is the wiring: the manual manager check asks for a
single row, while every other caller keeps the historical unlimited query.
"""

from unittest.mock import patch

import connectors

URI = "postgresql://username:password@example.invalid:5432/gaodcore"


def _call(**kwargs):
    with patch.object(connectors, "_validate_max_rows_allowed") as max_rows, patch.object(
        connectors, "get_resource_data", return_value=[{"id": 1}]
    ) as get_data:
        result = connectors.validate_resource(
            uri=URI,
            object_location="some_table",
            object_location_schema="public",
            **kwargs,
        )
    return result, max_rows, get_data


def test_validate_resource_forwards_an_explicit_limit():
    result, max_rows, get_data = _call(limit=1)

    # The row-count guard keeps running exactly as before the limit existed.
    max_rows.assert_called_once_with(
        URI, "some_table", object_location_schema="public", timeout=None
    )
    assert get_data.call_args.kwargs["limit"] == 1
    # The rows are handed back untouched.
    assert result is get_data.return_value


def test_validate_resource_is_unlimited_by_default():
    result, max_rows, get_data = _call()

    max_rows.assert_called_once_with(
        URI, "some_table", object_location_schema="public", timeout=None
    )
    assert get_data.call_args.kwargs["limit"] is None
    assert get_data.call_args.kwargs == {
        "uri": URI,
        "object_location": "some_table",
        "object_location_schema": "public",
        "filters": {},
        "like": "",
        "fields": [],
        "sort": [],
        "limit": None,
        "timeout": None,
    }
    assert result is get_data.return_value
