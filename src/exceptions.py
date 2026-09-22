"""Module with custom Django exceptions and the project's problem-document vocabulary."""

import re

from rest_framework.exceptions import APIException

#: Base URI for the ``type`` member of the problem documents returned by the API.
#: The URIs are stable identifiers, they are not required to be dereferenceable.
PROBLEM_TYPE_BASE = "https://opendata.aragon.es/problems/"

#: Media type of the error documents. See RFC 9457.
PROBLEM_CONTENT_TYPE = "application/problem+json"


class ErrorCodes:
    """Constants for error codes used in API error responses.

    ``error_code`` is a deliberate project-specific extension of RFC 9457: it gives
    clients a short, stable token to branch on without having to parse ``type``.
    """

    CONNECTION_UNAVAILABLE = "CONNECTION_UNAVAILABLE"
    OBJECT_UNAVAILABLE = "OBJECT_UNAVAILABLE"
    QUERY_ERROR = "QUERY_ERROR"
    SCHEMA_NOT_IMPLEMENTED = "SCHEMA_NOT_IMPLEMENTED"
    BAD_GATEWAY = "BAD_GATEWAY"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    RESOURCE_UNAVAILABLE = "RESOURCE_UNAVAILABLE"
    MIME_TYPE_NOT_ALLOWED = "MIME_TYPE_NOT_ALLOWED"
    TOO_MANY_ROWS = "TOO_MANY_ROWS"
    INVALID_FORMAT = "INVALID_FORMAT"
    INVALID_JSON = "INVALID_JSON"
    INVALID_FIELD = "INVALID_FIELD"
    INVALID_SORT = "INVALID_SORT"
    INVALID_FILTER = "INVALID_FILTER"
    INVALID_COLUMNS = "INVALID_COLUMNS"
    INVALID_DELIMITER = "INVALID_DELIMITER"
    NOT_A_NUMBER = "NOT_A_NUMBER"
    REQUIRED = "REQUIRED"


#: Human readable ``detail`` used when the problem carries per-field ``errors``.
VALIDATION_ERROR_DETAIL = "The request contains invalid fields."

#: Fallback ``error_code`` per HTTP status, used when an exception carries no usable code.
_STATUS_ERROR_CODES = {
    400: "BAD_REQUEST",
    401: "NOT_AUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    406: "NOT_ACCEPTABLE",
    415: "UNSUPPORTED_MEDIA_TYPE",
    429: "THROTTLED",
    500: "INTERNAL_SERVER_ERROR",
    502: "BAD_GATEWAY",
    503: "SERVICE_UNAVAILABLE",
    504: "GATEWAY_TIMEOUT",
}

_DEFAULT_ERROR_CODE = "ERROR"


def normalize_error_code(code, status_code=None):
    """Normalize an arbitrary DRF error code into a stable ``UPPER_SNAKE_CASE`` token.

    Codes that are empty or purely numeric (a common mistake: passing an HTTP status as
    the second positional argument of ``ValidationError``) fall back to the code derived
    from ``status_code``.

    @param code: raw code, usually taken from ``ErrorDetail.code`` or ``default_code``.
    @param status_code: HTTP status used to pick a fallback code.
    @return: normalized error code.
    """
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", str(code or "")).strip("_").upper()
    if not normalized or normalized[0].isdigit():
        return _STATUS_ERROR_CODES.get(status_code, _DEFAULT_ERROR_CODE)
    return normalized


def problem_type(error_code):
    """Build the stable ``type`` URI matching an ``error_code``."""
    return PROBLEM_TYPE_BASE + error_code.lower().replace("_", "-")


def problem_title(error_code):
    """Build the concise ``title`` matching an ``error_code``."""
    return error_code.replace("_", " ").capitalize()


class BadGateway(APIException):
    """Class to raise a exception of a Bad Gateway."""

    status_code = 502
    default_detail = "Service temporarily unavailable, try again later."
    default_code = ErrorCodes.BAD_GATEWAY


class ServiceUnavailable(APIException):
    """Exception for server-side errors (database/connector unavailability)."""

    status_code = 503
    default_detail = "Service temporarily unavailable."
    default_code = ErrorCodes.SERVICE_UNAVAILABLE

    def __init__(self, detail=None, code=None):
        """Initialize with optional detail message and error code."""
        super().__init__(detail=detail, code=code)
