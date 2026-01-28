"""Tests for DictSerializer robust field extraction."""

from rest_framework.exceptions import ErrorDetail

from serializers import DictSerializer


class TestDictSerializerGetFields:
    """Tests for DictSerializer.get_fields() robust field extraction."""

    def test_dictionary_only_data(self):
        """Test baseline behavior with dictionary-only data."""
        data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2", "extra": "value"},
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        assert set(fields.keys()) == {"id", "name", "extra"}
        for field_name, field in fields.items():
            assert field.label == field_name

    def test_mixed_dictionary_and_errordetail_data(self):
        """Test handling of mixed dictionary and ErrorDetail data."""
        error_detail = ErrorDetail("Database connection failed", code="error")
        data = [
            {"id": 1, "name": "test1"},
            error_detail,
            {"id": 2, "status": "active"},
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        # Should extract fields from dictionaries only, ignoring ErrorDetail
        assert set(fields.keys()) == {"id", "name", "status"}

    def test_pure_errordetail_data(self):
        """Test handling of data containing only ErrorDetail objects."""
        data = [
            ErrorDetail("Error 1", code="error1"),
            ErrorDetail("Error 2", code="error2"),
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        # Should return empty dict, no crash
        assert fields == {}

    def test_other_non_dictionary_objects(self):
        """Test handling of various non-dictionary objects."""
        data = [
            {"id": 1, "name": "valid"},
            "string_item",
            123,
            ["list", "item"],
            None,
            {"id": 2, "other": "field"},
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        # Should extract fields from dictionaries only
        assert set(fields.keys()) == {"id", "name", "other"}

    def test_exception_during_keys_call(self):
        """Test handling of objects whose keys() method raises an exception."""

        class FaultyDict:
            def keys(self):
                raise RuntimeError("Simulated keys() failure")

        data = [
            {"id": 1, "name": "valid"},
            FaultyDict(),
            {"id": 2, "status": "active"},
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        # Should skip faulty object and continue
        assert set(fields.keys()) == {"id", "name", "status"}

    def test_none_data(self):
        """Test handling of None data."""
        serializer = DictSerializer(data=None)
        fields = serializer.get_fields()

        assert fields == {}

    def test_empty_list_data(self):
        """Test handling of empty list data."""
        serializer = DictSerializer(data=[])
        fields = serializer.get_fields()

        assert fields == {}

    def test_object_with_non_callable_keys_attribute(self):
        """Test handling of objects with keys attribute that is not callable."""

        class ObjectWithKeysAttribute:
            keys = "not_a_method"

        data = [
            {"id": 1, "name": "valid"},
            ObjectWithKeysAttribute(),
        ]
        serializer = DictSerializer(data=data)
        fields = serializer.get_fields()

        # Should skip object with non-callable keys attribute
        assert set(fields.keys()) == {"id", "name"}
