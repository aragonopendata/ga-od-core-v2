from typing import List

from rest_framework.exceptions import NotAcceptable, ValidationError
from rest_framework.negotiation import DefaultContentNegotiation
from rest_framework.renderers import BaseRenderer
from rest_framework.request import Request

from exceptions import ErrorCodes


def get_allowed_formats(renderers: List[BaseRenderer]):
    return [renderer.format for renderer in renderers]


class HtmlFallbackContentNegotiation(DefaultContentNegotiation):
    """Default negotiation that answers a text/html-only request with the first renderer instead of a 406.

    The Browsable API is not offered on the public data views, so a browser asking only for text/html (with no
    explicit format) gets the default renderer. Explicit formats and any other unsatisfiable Accept still fail."""

    def select_renderer(self, request: Request, renderers: List[BaseRenderer], format_suffix=None):
        try:
            return super().select_renderer(request, renderers, format_suffix=format_suffix)
        except NotAcceptable:
            if format_suffix is None and 'text/html' in request.META.get('HTTP_ACCEPT', ''):
                return renderers[0], renderers[0].media_type
            raise


class LegacyContentNegotiation(HtmlFallbackContentNegotiation):
    def select_renderer(self, request: Request, renderers: List[BaseRenderer], format_suffix=None):
        """
        Given a request and a list of renderers, return a two-tuple of:
        (renderer, media type).
        """
        allowed_formats = get_allowed_formats(renderers)
        force_format = request.query_params.get('formato')
        if force_format is not None and force_format not in allowed_formats:
            raise ValidationError(f'Formato: "{force_format}" is not allowed. Allowed values: {allowed_formats}',
                                  ErrorCodes.INVALID_FORMAT)

        return super().select_renderer(request, renderers, format_suffix=force_format or format_suffix)
