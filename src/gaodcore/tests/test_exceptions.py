"""Tests for custom exception classes and exception handlers."""

from rest_framework.exceptions import ValidationError

from exceptions import ServiceUnavailable, ErrorCodes, BadGateway
from gaodcore.exception_handlers import custom_exception_handler


class TestErrorCodes:
    """Tests for ErrorCodes constants."""

    def test_connection_unavailable_code(self):
        assert ErrorCodes.CONNECTION_UNAVAILABLE == "CONNECTION_UNAVAILABLE"

    def test_object_unavailable_code(self):
        assert ErrorCodes.OBJECT_UNAVAILABLE == "OBJECT_UNAVAILABLE"

    def test_query_error_code(self):
        assert ErrorCodes.QUERY_ERROR == "QUERY_ERROR"

    def test_schema_not_implemented_code(self):
        assert ErrorCodes.SCHEMA_NOT_IMPLEMENTED == "SCHEMA_NOT_IMPLEMENTED"

    def test_bad_gateway_code(self):
        assert ErrorCodes.BAD_GATEWAY == "BAD_GATEWAY"


class TestServiceUnavailable:
    """Tests for ServiceUnavailable exception."""

    def test_default_status_code(self):
        exc = ServiceUnavailable()
        assert exc.status_code == 503

    def test_default_detail(self):
        exc = ServiceUnavailable()
        assert exc.detail == "Service temporarily unavailable."

    def test_custom_detail(self):
        exc = ServiceUnavailable("Database connection failed.")
        assert exc.detail == "Database connection failed."

    def test_custom_code(self):
        exc = ServiceUnavailable(
            "Connection failed.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        )
        assert exc.get_codes() == ErrorCodes.CONNECTION_UNAVAILABLE

    def test_inherits_from_api_exception(self):
        from rest_framework.exceptions import APIException

        assert issubclass(ServiceUnavailable, APIException)


class TestBadGateway:
    """Tests for BadGateway exception."""

    def test_default_status_code(self):
        exc = BadGateway()
        assert exc.status_code == 502

    def test_default_code(self):
        exc = BadGateway()
        assert exc.default_code == ErrorCodes.BAD_GATEWAY


class TestCustomExceptionHandler:
    """Tests for custom exception handler."""

    def test_adds_error_code_to_response(self):
        exc = ServiceUnavailable(
            "Connection failed.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        )
        response = custom_exception_handler(exc, {})

        assert response is not None
        assert "error_code" in response.data
        assert response.data["error_code"] == ErrorCodes.CONNECTION_UNAVAILABLE

    def test_adds_status_to_response(self):
        exc = ServiceUnavailable("Connection failed.")
        response = custom_exception_handler(exc, {})

        assert response is not None
        assert "status" in response.data
        assert response.data["status"] == 503

    def test_preserves_detail_field(self):
        exc = ServiceUnavailable("Connection failed.")
        response = custom_exception_handler(exc, {})

        assert response is not None
        assert "detail" in response.data
        assert response.data["detail"] == "Connection failed."

    def test_handles_validation_error(self):
        exc = ValidationError("Invalid input.")
        response = custom_exception_handler(exc, {})

        assert response is not None
        assert "error_code" in response.data
        assert "status" in response.data
        assert response.data["status"] == 400

    def test_handles_bad_gateway(self):
        exc = BadGateway()
        response = custom_exception_handler(exc, {})

        assert response is not None
        assert response.data["status"] == 502
        assert response.data["error_code"] == ErrorCodes.BAD_GATEWAY

    def test_error_code_uppercase(self):
        exc = ValidationError("Invalid input.")
        response = custom_exception_handler(exc, {})

        # Error code should be uppercase
        assert response.data["error_code"] == response.data["error_code"].upper()
