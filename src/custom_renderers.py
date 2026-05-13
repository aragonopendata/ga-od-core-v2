"""
Custom renderers for backward compatibility.
"""
from drf_excel.renderers import XLSXRenderer
from rest_framework_csv.renderers import CSVRenderer


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
