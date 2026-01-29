"""Custom exception handlers for consistent error response format."""

from rest_framework.views import exception_handler


def custom_exception_handler(exc, context):
    """
    Custom exception handler that adds error_code and status to all error responses.

    Returns a consistent JSON structure:
    {
        "detail": "Human-readable error message",
        "error_code": "MACHINE_READABLE_CODE",
        "status": 503
    }
    """
    response = exception_handler(exc, context)

    if response is not None:
        # Get error code using DRF's get_codes() method
        # This returns the code that was passed to the exception constructor
        error_code = exc.get_codes() if hasattr(exc, "get_codes") else "error"

        # Handle nested/list error codes - get the first one
        if isinstance(error_code, list):
            error_code = error_code[0] if error_code else "error"
        elif isinstance(error_code, dict):
            # For nested errors, get the first value
            error_code = list(error_code.values())[0] if error_code else "error"
            if isinstance(error_code, list):
                error_code = error_code[0] if error_code else "error"

        # Convert error code to uppercase for consistency
        if isinstance(error_code, str):
            error_code = error_code.upper()

        # Handle different response data formats
        # ValidationError can return a list, so we need to convert to dict
        if isinstance(response.data, list):
            response.data = {"detail": response.data}

        response.data["error_code"] = error_code
        response.data["status"] = response.status_code

    return response
