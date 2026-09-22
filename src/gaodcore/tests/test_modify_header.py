import pytest
from rest_framework.exceptions import ValidationError

from utils import modify_header


def test_modify_header_empty_return_list_with_matching_rename_is_noop():
    assert modify_header([], ["renamed"]) == []


def test_modify_header_empty_return_list_with_no_columns_is_noop():
    assert modify_header([], []) == []


def test_modify_header_empty_return_list_with_mismatched_columns_does_not_raise():
    """Empty results skip column validation entirely, even with a bad column count."""
    assert modify_header([], ["a", "b", "c"]) == []


def test_modify_header_empty_return_list_xlsx_is_noop():
    assert modify_header([], ["renamed"], format_is_xlsx=True) == []


def test_modify_header_non_empty_matching_columns_still_renames():
    result = modify_header([{"id": 1, "name": "foo"}], ["identifier", "full_name"])
    assert result == [{"identifier": 1, "full_name": "foo"}]


def test_modify_header_non_empty_mismatched_columns_still_raises():
    with pytest.raises(ValidationError) as exc_info:
        modify_header([{"id": 1, "name": "foo"}], ["only_one"])

    assert exc_info.value.get_codes() == ["INVALID_COLUMNS"]
