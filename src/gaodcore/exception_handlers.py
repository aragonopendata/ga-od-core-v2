"""Custom exception handler producing RFC 9457-style problem documents."""

import logging

from rest_framework.exceptions import ValidationError
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.settings import api_settings
from rest_framework.views import exception_handler

from custom_renderers import ProblemJSONRenderer
from exceptions import (
    PROBLEM_CONTENT_TYPE,
    VALIDATION_ERROR_DETAIL,
    ErrorCodes,
    normalize_error_code,
    problem_title,
    problem_type,
)

logger = logging.getLogger(__name__)

_NON_FIELD_ERRORS_KEY = api_settings.NON_FIELD_ERRORS_KEY


def _error_item(value, status_code):
    """Convert a single DRF ``ErrorDetail`` into an ``errors`` entry.

    @param value: an ``ErrorDetail`` (or any scalar) coming from ``exc.detail``.
    @param status_code: HTTP status, used when the entry carries no usable code.
    @return: dictionary with the human readable ``message`` and its ``code``.
    """
    return {
        "message": str(value),
        "code": normalize_error_code(getattr(value, "code", None), status_code),
    }


def _convert_errors(detail, status_code):
    """Recursively convert a DRF error detail tree into ``errors`` members.

    Dictionaries (nested serializers) keep their shape, lists (``many=True``
    serializers or several messages for one field) keep their order, and every leaf
    keeps both its message and its validation code. Nothing is discarded.

    @param detail: ``ErrorDetail``, list or dict as produced by DRF.
    @return: the same structure with every leaf replaced by a message/code pair.
    """
    if isinstance(detail, dict):
        return {
            str(key): _convert_errors(value, status_code)
            for key, value in detail.items()
        }
    if isinstance(detail, (list, tuple)):
        return [
            _convert_errors(item, status_code)
            if isinstance(item, (dict, list, tuple))
            else _error_item(item, status_code)
            for item in detail
        ]
    return [_error_item(detail, status_code)]


def _build_errors(detail, status_code):
    """Build the top level ``errors`` member.

    Messages that are not attached to a field are grouped under DRF's
    ``NON_FIELD_ERRORS_KEY`` so ``errors`` is always a JSON object.
    """
    if isinstance(detail, dict):
        return _convert_errors(detail, status_code)
    return {_NON_FIELD_ERRORS_KEY: _convert_errors(detail, status_code)}


def _get_raw_detail(exc, response):
    """Get the exception detail, falling back to what DRF already put in the response.

    Exceptions translated by DRF (``Http404``, Django's ``PermissionDenied``) have no
    ``detail`` attribute of their own.
    """
    detail = getattr(exc, "detail", None)
    if detail is not None:
        return detail
    data = response.data
    if isinstance(data, dict) and "detail" in data:
        return data["detail"]
    return data


def _build_problem(exc, response):
    """Build the problem document for an exception.

    The top level ``error_code`` is never picked arbitrarily from ``exc.get_codes()``:
    a structured detail always means a validation problem, and a scalar detail uses its
    own code (or the exception's ``default_code``).
    """
    status_code = response.status_code
    raw_detail = _get_raw_detail(exc, response)
    is_structured = isinstance(raw_detail, (dict, list, tuple))

    if isinstance(exc, ValidationError):
        error_code = ErrorCodes.VALIDATION_ERROR
        detail = VALIDATION_ERROR_DETAIL
    elif is_structured:
        error_code = normalize_error_code(
            getattr(exc, "default_code", None), status_code
        )
        detail = str(getattr(exc, "default_detail", None) or problem_title(error_code))
    else:
        error_code = normalize_error_code(
            getattr(raw_detail, "code", None) or getattr(exc, "default_code", None),
            status_code,
        )
        detail = str(raw_detail)

    problem = {
        "type": problem_type(error_code),
        "title": problem_title(error_code),
        "status": status_code,
        "detail": detail,
        "error_code": error_code,
    }
    if is_structured:
        # Field messages live in their own member, so a field literally named "status",
        # "detail", "error_code" or "errors" cannot overwrite the envelope.
        problem["errors"] = _build_errors(raw_detail, status_code)
    return problem


def _force_problem_renderer(context, response):
    """Make the response render as ``application/problem+json``.

    Replacing ``response.data`` is not enough: ``APIView.finalize_response`` copies the
    renderer negotiated for the request onto the response, so a request that asked for
    CSV/XML/XLSX would serialize the error envelope as a table or a spreadsheet. The
    renderer has to be replaced on the request too.

    The browsable API is left untouched so the HTML debugging interface keeps working.
    """
    request = context.get("request") if context else None
    if isinstance(getattr(request, "accepted_renderer", None), BrowsableAPIRenderer):
        return

    renderer = ProblemJSONRenderer()
    if request is not None:
        request.accepted_renderer = renderer
        request.accepted_media_type = PROBLEM_CONTENT_TYPE
    response.accepted_renderer = renderer
    response.accepted_media_type = PROBLEM_CONTENT_TYPE
    response.content_type = PROBLEM_CONTENT_TYPE


def _log_server_error(exc, context, problem):
    """Log 5xx problems with request context for easier debugging."""
    request = context.get("request") if context else None
    resource_id = None
    path = "unknown"
    if request is not None:
        resource_id = request.query_params.get("resource_id")
        path = request.path

    logger.error(
        "%s: %s - resource_id=%s, error_code=%s, detail=%s",
        problem["status"],
        path,
        resource_id,
        problem["error_code"],
        exc.detail if hasattr(exc, "detail") else str(exc),
    )


def custom_exception_handler(exc, context):
    """Return a consistent RFC 9457-style problem document for every API error.

    The body is::

        {
            "type": "https://opendata.aragon.es/problems/connection-unavailable",
            "title": "Connection unavailable",
            "status": 503,
            "detail": "Connection is not available.",
            "error_code": "CONNECTION_UNAVAILABLE"
        }

    ``error_code`` is a documented project-specific extension of RFC 9457. Validation
    problems additionally carry an ``errors`` member keyed by field name, each entry
    holding every message with its DRF validation code.
    """
    response = exception_handler(exc, context)

    if response is None:
        return None

    problem = _build_problem(exc, response)
    response.data = problem
    _force_problem_renderer(context, response)

    if response.status_code >= 500:
        _log_server_error(exc, context, problem)

    return response
