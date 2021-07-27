"""Module with custom Django exceptions."""

from rest_framework.exceptions import APIException


class BadGateway(APIException):
    """Class to raise a exception of a Bad Gateway."""
    status_code = 502
    default_detail = 'Service temporarily unavailable, try again later.'
    default_code = 'bad_gateway'
