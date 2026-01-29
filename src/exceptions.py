"""Module with custom Django exceptions."""

from rest_framework.exceptions import APIException


class ErrorCodes:
    """Constants for error codes used in API error responses."""

    CONNECTION_UNAVAILABLE = "CONNECTION_UNAVAILABLE"
    OBJECT_UNAVAILABLE = "OBJECT_UNAVAILABLE"
    QUERY_ERROR = "QUERY_ERROR"
    SCHEMA_NOT_IMPLEMENTED = "SCHEMA_NOT_IMPLEMENTED"
    BAD_GATEWAY = "BAD_GATEWAY"


class BadGateway(APIException):
    """Class to raise a exception of a Bad Gateway."""

    status_code = 502
    default_detail = "Service temporarily unavailable, try again later."
    default_code = ErrorCodes.BAD_GATEWAY


class ServiceUnavailable(APIException):
    """Exception for server-side errors (database/connector unavailability)."""

    status_code = 503
    default_detail = "Service temporarily unavailable."
    default_code = "service_unavailable"

    def __init__(self, detail=None, code=None):
        """Initialize with optional detail message and error code."""
        super().__init__(detail=detail, code=code)
