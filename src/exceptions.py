from rest_framework.exceptions import APIException


class BadGateway(APIException):
    status_code = 502
    default_detail = 'Service temporarily unavailable, try again later.'
    default_code = 'bad_gateway'
