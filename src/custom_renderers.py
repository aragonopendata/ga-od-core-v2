"""
Custom renderers for backward compatibility.
"""
from drf_excel.renderers import XLSXRenderer
from rest_framework.renderers import JSONRenderer
from rest_framework_csv.renderers import CSVRenderer

from exceptions import PROBLEM_CONTENT_TYPE


class BackwardCompatibleXLSXRenderer(XLSXRenderer):
    """
    XLSX renderer that supports both the official MIME type and the legacy simplified one.
    """
    # Use the legacy simplified MIME type for backward compatibility
    media_type = "application/xlsx"
    format = "xlsx"

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Render the data into XLSX format.
        """
        # Handle both MIME types
        if accepted_media_type in ["application/xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
            return super().render(data, accepted_media_type, renderer_context)
        return super().render(data, accepted_media_type, renderer_context)


class SCSVRenderer(CSVRenderer):
    """
    Semicolon Separated Values renderer.
    Same as CSV but uses semicolon (;) as delimiter instead of comma.
    """
    media_type = "text/scsv"
    format = "scsv"


class ProblemJSONRenderer(JSONRenderer):
    """Renderer for RFC 9457-style error documents.

    Errors must never go through the data renderers (CSV, SCSV, XLSX, YAML, XML): an
    error envelope is not tabular data. The exception handler swaps the negotiated
    renderer for this one so the body and the ``Content-Type`` always agree.
    """

    media_type = PROBLEM_CONTENT_TYPE
    format = "problem+json"
