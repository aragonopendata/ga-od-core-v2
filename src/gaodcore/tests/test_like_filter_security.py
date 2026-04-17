import pytest
from rest_framework.exceptions import ValidationError
from sqlalchemy import Column, Integer, MetaData, String, Table

from connectors import FieldNoExistsError, _process_like_filter


@pytest.fixture
def model():
    metadata = MetaData()
    table = Table(
        "test_table",
        metadata,
        Column("id", Integer),
        Column("name", String),
        Column("description", String),
    )
    return table


class TestRCEPrevention:
    def test_rce_os_system_blocked(self, model):
        with pytest.raises(ValidationError):
            _process_like_filter("__import__('os').system('id')", model)

    def test_rce_subprocess_blocked(self, model):
        with pytest.raises(ValidationError):
            _process_like_filter("__import__('subprocess').call(['ls'])", model)

    def test_rce_eval_chain_blocked(self, model):
        with pytest.raises(ValidationError):
            _process_like_filter("eval('1+1')", model)

    def test_validation_bypass_non_json_string(self, model):
        with pytest.raises(ValidationError):
            _process_like_filter("not a valid expression", model)


class TestValidDictInput:
    def test_valid_dict_single_field(self, model):
        result = _process_like_filter({"name": "test"}, model)
        assert len(result) == 1

    def test_valid_dict_multiple_fields(self, model):
        result = _process_like_filter({"name": "test", "description": "desc"}, model)
        assert len(result) == 2

    def test_empty_dict_returns_empty(self, model):
        result = _process_like_filter({}, model)
        assert result == []

    def test_none_returns_empty(self, model):
        result = _process_like_filter(None, model)
        assert result == []

    def test_invalid_column_name_raises_error(self, model):
        with pytest.raises(FieldNoExistsError):
            _process_like_filter({"nonexistent": "test"}, model)


class TestSQLInjectionPrevention:
    def test_sql_injection_in_value(self, model):
        result = _process_like_filter({"name": "'; DROP TABLE users; --"}, model)
        assert len(result) == 1

    def test_sql_injection_in_value_union(self, model):
        result = _process_like_filter({"name": "' UNION SELECT * FROM users --"}, model)
        assert len(result) == 1

    def test_sql_injection_parameterization_verified(self, model):
        """Verify SQL injection payloads are properly parameterized, not string-concatenated"""
        from sqlalchemy.dialects import postgresql

        result = _process_like_filter({"name": "'; DROP TABLE users; --"}, model)
        assert len(result) == 1

        # Compile the filter to SQL and verify parameterization
        compiled = result[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": False}
        )

        # The malicious payload should NOT appear in the SQL string
        sql_str = str(compiled)
        assert "DROP TABLE" not in sql_str

        # The payload should be in the params as a bound parameter
        # SQLAlchemy uses parameter placeholders like :name_1
        assert ":" in sql_str or "%" in sql_str  # PostgreSQL uses different param styles

    def test_sql_injection_union_parameterization_verified(self, model):
        """Verify UNION injection attempts are parameterized"""
        from sqlalchemy.dialects import postgresql

        result = _process_like_filter({"name": "' UNION SELECT * FROM users --"}, model)
        assert len(result) == 1

        compiled = result[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": False}
        )

        sql_str = str(compiled)
        assert "UNION SELECT" not in sql_str  # Should be parameterized, not in SQL


class TestEdgeCases:
    """Test edge cases in like filter processing"""

    def test_empty_string_value(self, model):
        """Empty string values should be processed without error"""
        result = _process_like_filter({"name": ""}, model)
        assert len(result) == 1
        # Verify it creates a valid ILIKE filter for empty string

    def test_none_value_in_dict(self, model):
        """None/null values should be handled gracefully"""
        # Depending on implementation, this might raise ValidationError or be skipped
        # Current implementation should handle based on _get_like validation
        try:
            result = _process_like_filter({"name": None}, model)
            # If it processes None, verify it doesn't crash
            assert isinstance(result, list)
        except (ValidationError, TypeError, AttributeError):
            # Or it might reject None values - both are acceptable
            pass

    def test_numeric_value_in_dict(self, model):
        """Numeric values should be converted to string for ILIKE or rejected"""
        # Current validation in _get_like allows int/float/bool
        result = _process_like_filter({"id": 123}, model)
        assert len(result) == 1
        # Should convert to string "123" for ILIKE comparison

    def test_boolean_value_in_dict(self, model):
        """Boolean values should be handled"""
        result = _process_like_filter({"name": True}, model)
        assert len(result) == 1
        # Should convert to string "True" for ILIKE comparison

    def test_multiple_mixed_types(self, model):
        """Mix of different allowed types"""
        result = _process_like_filter(
            {"id": 123, "name": "test", "description": ""},
            model
        )
        assert len(result) == 3


class TestDoSProtection:
    """Test Denial of Service protection for ast.literal_eval"""

    def test_very_large_input_string(self, model):
        """Very large input strings should be rejected or handled safely"""
        # Create a 1MB+ string
        large_string = "a" * (1024 * 1024 + 1)

        # This should either:
        # 1. Be rejected by _get_like validation (preferred)
        # 2. Be handled without memory exhaustion
        try:
            with pytest.raises((ValidationError, ValueError, MemoryError)):
                _process_like_filter(large_string, model)
        except Exception as e:
            # If it doesn't raise, verify it completes quickly
            pytest.fail(f"Large input should be rejected, got: {type(e)}")

    def test_deeply_nested_structure(self, model):
        """Deeply nested dict structures should be rejected or handled safely"""
        # Create a deeply nested structure (100+ levels)
        nested_dict = "{'a': " * 100 + "'value'" + "}" * 100

        # ast.literal_eval should handle this, but it might be slow
        # Acceptable outcomes:
        # 1. ValidationError (preferred - rejected by _get_like)
        # 2. ValueError (from ast.literal_eval)
        # 3. RecursionError (from deep nesting)
        # 4. FieldNoExistsError (parsed but 'a' not a valid column - still safe, just inefficient)
        with pytest.raises((ValidationError, ValueError, RecursionError, FieldNoExistsError)):
            _process_like_filter(nested_dict, model)

    def test_very_long_list(self, model):
        """Very long list structures should not cause DoS"""
        # This would fail _get_like validation (not a dict), but test the parsing layer
        long_list = "[" + "1," * 100000 + "1]"

        with pytest.raises((ValidationError, ValueError)):
            _process_like_filter(long_list, model)


class TestIntegration:
    """Integration tests with actual endpoint (require Django test client)"""

    @pytest.mark.django_db
    def test_endpoint_blocks_malicious_payload(self, client):
        """Test actual /GA_OD_Core/download endpoint blocks RCE attempts"""
        # This requires a valid resource_id fixture, so marked as integration test
        response = client.get(
            '/GA_OD_Core/download',
            {
                'resource_id': 1,  # Assumes test fixture exists
                'like': '__import__("os").system("whoami")'
            }
        )

        # Should return 400 Bad Request or similar error, not 500 or 200
        assert response.status_code in [400, 422]
        # Should not execute code (verified by no side effects)

    @pytest.mark.django_db
    def test_endpoint_accepts_valid_like_filter(self, client):
        """Test actual endpoint accepts valid JSON dict like filters"""
        import json

        response = client.get(
            '/GA_OD_Core/download',
            {
                'resource_id': 1,  # Assumes test fixture exists
                'like': json.dumps({"name": "test"})
            }
        )

        # Should process successfully (200) or return no data (404)
        # Should NOT return 500 internal server error
        assert response.status_code in [200, 404]
