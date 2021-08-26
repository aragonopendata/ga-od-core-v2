"""Custom Content Negotiation. This is used to know what response-type is required by a user."""

from typing import List

from rest_framework.exceptions import ValidationError
from rest_framework.negotiation import DefaultContentNegotiation
from rest_framework.renderers import BaseRenderer
from rest_framework.request import Request


def get_allowed_formats(renderers: List[BaseRenderer]) -> List[str]:
    """From a list of instances of BaseRender this return content-types or formats that can be rendered.

    :param renderers: List of instances of BaseRender.

    :return: Content-types or formats that can be rendered.
    """
    return [renderer.format for renderer in renderers]


class LegacyContentNegotiation(DefaultContentNegotiation):
    def select_renderer(self, request: Request, renderers: List[BaseRenderer], format_suffix=None):
        """
        Given a request and a list of renderers, return a two-tuple of:
        (renderer, media type).
        """
        allowed_formats = get_allowed_formats(renderers)
        force_format = request.query_params.get('formato')
        if force_format is not None and force_format not in allowed_formats:
            raise ValidationError(f'Formato: "{force_format}" is not allowed. Allowed values: {allowed_formats}', 400)

        return super().select_renderer(request, renderers, format_suffix=force_format or format_suffix)
