import contextlib
import csv
import itertools
import json
import logging
import resource
import tempfile
import warnings
from time import monotonic
from json.decoder import JSONDecodeError
from typing import Optional, Dict, Any, List, Callable
from urllib.parse import urlparse

import xlsxwriter
from django.conf import settings
from django.http import FileResponse, HttpResponse, StreamingHttpResponse
from drf_excel.mixins import XLSXFileMixin
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
from rest_framework.exceptions import ValidationError

import download_limits
from exceptions import ServiceUnavailable, ErrorCodes, DownloadInProgress, TooManyDownloads
from rest_framework.renderers import BrowsableAPIRenderer, JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.utils.serializer_helpers import ReturnList

from connectors import (
    get_resource_data,
    get_resource_rows,
    get_resource_columns,
    NoObjectError,
    DriverConnectionError,
    NotImplementedSchemaError,
    OrderBy,
    FieldNoExistsError,
    SortFieldNoExistsError,
    MimeTypeError,
    TooManyRowsErrorExcel,
    get_resource_data_feature,
    get_GeoJson_resource,
    update_resource_size,
    open_resource,
    _HTTP_SCHEMAS,
)
from download_stream import (
    CsvEncoder,
    JsonEncoder,
    StreamedDownload,
    StreamedDownloadResponse,
    csv_blocks,
    current_rss_kb,
    json_blocks,
)
from log_sanitizer import mask_uri
from phase_timing import PhaseTimer, phase
from gaodcore.negotations import HtmlFallbackContentNegotiation, LegacyContentNegotiation
from gaodcore_manager.models import ResourceConfig
from utils import get_return_list, modify_header
from views import APIViewMixin

logger = logging.getLogger(__name__)
timing_logger = logging.getLogger("gaodcore.download_timing")

_RESOURCE_MAX_ROWS_EXCEL = 1048576
XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
CSV_CONTENT_TYPE = "text/csv"


# Same renderers and order as the global DRF setting, minus the Browsable API: a browser asking for text/html would
# otherwise get the whole dataset rendered as HTML (CPU spikes / OOM). Explicit formats are unaffected.
PUBLIC_DATA_RENDERER_CLASSES = tuple(
    renderer for renderer in api_settings.DEFAULT_RENDERER_CLASSES if renderer is not BrowsableAPIRenderer
)


def _get_data_public_error(func: Callable, *args, **kwargs) -> List[Dict[str, Any]]:
    try:
        return func(*args, **kwargs)
    except FieldNoExistsError as err:
        raise ValidationError(str(err), ErrorCodes.INVALID_FIELD) from err
    except SortFieldNoExistsError as err:
        raise ValidationError(str(err), ErrorCodes.INVALID_SORT) from err
    except NoObjectError as err:
        raise ServiceUnavailable(
            "Object is not available.", code=ErrorCodes.OBJECT_UNAVAILABLE
        ) from err
    except DriverConnectionError as err:
        raise ServiceUnavailable(
            "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        ) from err
    except NotImplementedSchemaError as err:
        raise ServiceUnavailable(
            "Unexpected error: schema is not implemented.",
            code=ErrorCodes.SCHEMA_NOT_IMPLEMENTED,
        ) from err
    except MimeTypeError as err:
        raise ServiceUnavailable(
            "Unexpected error: mimetype of input file is not implemented.",
            code=ErrorCodes.SCHEMA_NOT_IMPLEMENTED,
        ) from err


def _get_resource(resource_id: int):
    try:
        return ResourceConfig.objects.select_related().get(
            id=resource_id, enabled=True, connector_config__enabled=True
        )
    except ResourceConfig.DoesNotExist as err:
        logger.warning(
            "Resource %s does not exist or is not available: %s", resource_id, err
        )
        raise ValidationError(
            "Resource not exists or is not available",
            ErrorCodes.RESOURCE_UNAVAILABLE,
        ) from err


def _write_xlsx_rows(worksheet, data) -> tuple:
    """Write the header and then the rows in strict row order, as ``constant_memory`` requires.

    With ``constant_memory`` xlsxwriter flushes a row when a later one is started and silently drops anything written to
    an earlier row, so the header must be complete before the first data row. Returns the number of hyperlinks Excel
    refused (kept as text) and the first one as ``(excel_row, excel_column, column_name)``.
    """
    skipped_hyperlinks = 0
    first_skipped_hyperlink = None
    rows = iter(data)
    first = next(rows, None)
    if first is None:
        return skipped_hyperlinks, first_skipped_hyperlink
    for col, key in enumerate(first.keys()):
        worksheet.write(0, col, key)
    for row, item in enumerate(itertools.chain((first,), rows)):
        for col, (key, value) in enumerate(item.items()):
            # Write None values as blank cells, not NaN
            if value is None:
                worksheet.write_blank(row + 1, col, None)
                write_result = 0
            else:
                write_result = worksheet.write(row + 1, col, value)
            if write_result == -4:
                worksheet.write_string(row + 1, col, str(value))
                skipped_hyperlinks += 1
                if first_skipped_hyperlink is None:
                    first_skipped_hyperlink = (row + 2, col + 1, key)
    return skipped_hyperlinks, first_skipped_hyperlink


def get_response_xlsx(
    data: ReturnList, resource_config: Optional[ResourceConfig] = None
) -> FileResponse:
    """Render resource data as XLSX and report hyperlinks skipped by Excel limits.

    The workbook is written row by row (``constant_memory``) to a temporary file that is served as a streaming response
    and deleted when the response is closed, or right away if building the workbook fails.
    """

    output = tempfile.NamedTemporaryFile(suffix=".xlsx")  # deleted when closed
    try:
        # xlsxwriter spills each worksheet to its own temp file in constant_memory mode; keep them in a directory that
        # is always removed, even when writing fails before workbook.close().
        with tempfile.TemporaryDirectory() as tmpdir:
            workbook = xlsxwriter.Workbook(
                output, {"constant_memory": True, "tmpdir": tmpdir}
            )
            worksheet = workbook.add_worksheet()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"Ignoring URL '.*' since it exceeds Excel's limit of 65,530 URLS per worksheet\.",
                    category=UserWarning,
                    module=r"xlsxwriter\.worksheet",
                )
                skipped_hyperlinks, first_skipped_hyperlink = _write_xlsx_rows(
                    worksheet, data
                )
            # Close the workbook before sending the data.
            workbook.close()
        output.flush()
        output.seek(0)
    except BaseException:
        output.close()
        raise

    if first_skipped_hyperlink is not None:
        excel_row, excel_column, column_name = first_skipped_hyperlink
        logger.warning(
            "XLSX hyperlink limit reached; URLs remain as text: "
            "resource_id=%s resource_name=%r object_location=%r schema=%r "
            "skipped_hyperlinks=%s first_excel_row=%s first_excel_column=%s "
            "column_name=%r",
            getattr(resource_config, "id", None),
            getattr(resource_config, "name", None),
            getattr(resource_config, "object_location", None),
            getattr(resource_config, "object_location_schema", None),
            skipped_hyperlinks,
            excel_row,
            excel_column,
            column_name,
        )

    response = FileResponse(
        output,
        content_type=XLSX_CONTENT_TYPE,
    )
    # FileResponse would announce the temporary file name; the caller decides the download name.
    del response["Content-Disposition"]
    return response


def get_response_csv(data: ReturnList, delimiter: str = ",") -> HttpResponse:
    """Get resource csv with order column names."""
    """output CSV (Comma Separated Values) dynamically using Django views"""

    response = HttpResponse(content_type=CSV_CONTENT_TYPE)
    writer = csv.writer(response, delimiter=delimiter)

    for row, item in enumerate(data):
        csv_header = []
        csv_data = []
        for col, (key, value) in enumerate(item.items()):
            if row == 0:
                csv_header.append(key)
            csv_data.append(value)
        if row == 0:
            writer.writerow(csv_header)
        writer.writerow(csv_data)
    return response


def response_size(response) -> int:
    """Size in bytes of a response body without materializing a streaming one; ``-1`` when it is unknown.

    A streaming response (``FileResponse``) announces its size in ``Content-Length``; a response that is not yet
    rendered has no body to measure.
    """
    if response is None:
        return -1
    if getattr(response, "streaming", False):
        # Streaming responses have no ``.content``; FileResponse announces the real size up front.
        length = response.get("Content-Length", "")
        return int(length) if length.isdigit() else -1
    if not getattr(response, "is_rendered", True):
        return -1
    container = getattr(response, "_container", None)
    if container is None:
        return len(response.content)
    # ``.content`` joins every chunk into a full copy of the body; summing the chunk lengths measures without copying.
    return sum(map(len, container))


def _save_size_when_ready(response, resource_config, registries: int) -> None:
    """Store the real size of ``response`` once its body exists: now if it is ready, else right after it renders."""

    def save(ready_response):
        update_resource_size(resource_config, registries, response_size(ready_response))

    if getattr(response, "is_rendered", True):
        save(response)
    else:
        response.add_post_render_callback(save)


class DownloadView(APIViewMixin):
    """This view allow get public serialized data from internal databases or APIs of Gobierno de Aragón. If JSON
    response type is selected and the view has a shape field the response will be in GEOJSON format"""

    _PREVIEW_LIMIT = 1000
    # Set with ``as_view(is_preview=True)`` in urls.py; independent of the URL prefix and the query string.
    is_preview = False

    content_negotiation_class = LegacyContentNegotiation
    renderer_classes = PUBLIC_DATA_RENDERER_CLASSES

    def dispatch(self, request, *args, **kwargs):
        """Run the request and log one ``download_timing`` line with the time spent in each phase."""
        timer = PhaseTimer()
        self._timer = timer
        if not getattr(settings, "DOWNLOAD_TIMING_LOG", True):
            return super().dispatch(request, *args, **kwargs)
        timer.attrs["endpoint"] = "download" if self.is_download_endpoint(request) else "preview"
        raw_id = request.GET.get("resource_id") or request.GET.get("view_id") or ""
        if raw_id.isdigit():
            timer.attrs["resource_id"] = raw_id
        try:
            response = super().dispatch(request, *args, **kwargs)
        except BaseException:
            self._log_timing(timer, None)
            raise
        if isinstance(response, StreamedDownloadResponse):
            # Its ``download_timing`` line is written when the server closes the response, at the end of the sending.
            pass
        elif getattr(response, "is_rendered", True):
            self._log_timing(timer, response)
        else:
            # DRF responses are rendered lazily by Django after the view returns; time the render and log after it.
            original_render = response.render

            def timed_render():
                started = monotonic()
                try:
                    return original_render()
                finally:
                    timer.add("render", monotonic() - started)
                    self._log_timing(timer, response)

            response.render = timed_render
        return response

    @staticmethod
    def _log_timing(timer: PhaseTimer, response, download: Optional[StreamedDownload] = None) -> None:
        """Emit the single-line phase timing log. Never raises, so it cannot alter the response or the error.

        Fields (seconds, 3 decimals): t_connect = engine creation + reflection, summed over every engine built in the
        request (``connects`` counts them); t_query = the SQL/API query; t_normalize = value normalization and
        serialization (including get_return_list and modify_header); t_render = renderer / XLSX / CSV build.
        ``bytes=-1`` means unknown (streaming response or no response). ``maxrss_kb`` is the worker peak RSS.

        A streamed download (``download``) is logged when its sending ends: ``rows`` and ``bytes`` are what was
        delivered to the server, the phase times are the sum over its blocks, ``t_total`` runs until the end of the
        sending, and three fields come last: ``result`` (``complete``, ``early_close`` or ``error``), ``t_first``
        (request start to first block ready) and ``rss_kb`` (resident memory now).
        """
        if timer.emitted:
            return
        timer.emitted = True
        try:
            if download is not None:
                status, size = 200, download.bytes
                timer.attrs["rows"] = download.rows
            elif response is None:
                status, size = "-", -1
            else:
                status = response.status_code
                size = response_size(response)
            attrs = timer.attrs
            timing_logger.info(
                "download_timing resource_id=%s endpoint=%s format=%s status=%s rows=%s connects=%s "
                "t_connect=%.3f t_query=%.3f t_normalize=%.3f t_render=%.3f t_total=%.3f bytes=%s maxrss_kb=%s%s",
                attrs.get("resource_id", "-"),
                attrs.get("endpoint", "-"),
                attrs.get("format", "-"),
                status,
                attrs.get("rows", -1),
                timer.counts.get("connect", 0),
                timer.times.get("connect", 0.0),
                timer.times.get("query", 0.0),
                timer.times.get("normalize", 0.0),
                timer.times.get("render", 0.0),
                monotonic() - timer.started_at,
                size,
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                (f" limited={attrs['limited']}" if "limited" in attrs else "")
                + (
                    f" result={download.result} t_first={download.t_first:.3f} rss_kb={current_rss_kb()}"
                    if download is not None
                    else ""
                ),
            )
        except Exception:  # noqa: BLE001 - diagnostics must never break the request
            timing_logger.warning("download_timing could not be logged", exc_info=True)

    def _open_shared_resource(
        self, stack, resource_config, resource_id, timer, is_xlsx: bool
    ):
        """Open the resource (engine + reflected table) on ``stack`` and map connection errors to HTTP errors."""
        context = " during Excel validation" if is_xlsx else ""
        try:
            return stack.enter_context(
                open_resource(
                    uri=resource_config.connector_config.uri,
                    object_location=resource_config.object_location,
                    object_location_schema=resource_config.object_location_schema,
                    timer=timer,
                )
            )
        except DriverConnectionError as err:
            logger.warning("Connection is not available%s: %s", context, err)
            logger.warning(
                "Resource: %s - Location: %s - Schema: %s",
                resource_id,
                resource_config.object_location,
                resource_config.object_location_schema,
            )
            raise ServiceUnavailable(
                "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
            ) from err
        except NoObjectError as err:
            logger.warning("Object is not available%s: %s", context, err)
            logger.warning(
                "Resource: %s - Location: %s - Schema: %s",
                resource_id,
                resource_config.object_location,
                resource_config.object_location_schema,
            )
            raise ServiceUnavailable(
                "Object is not available.", code=ErrorCodes.OBJECT_UNAVAILABLE
            ) from err

    @extend_schema(
        tags=["default"],
        parameters=[
            OpenApiParameter(
                "resource_id",
                description="Id of resource to be searched against.",
                type=OpenApiTypes.NUMBER,
            ),
            OpenApiParameter(
                "view_id",
                description="Alias of resource_id. Backward compatibility.",
                type=OpenApiTypes.NUMBER,
            ),
            OpenApiParameter(
                "filters",
                description="Matching conditions to select, e.g {'key1': 'a', 'key2': 'b'}.",
                type={"type": "object"},
            ),
            OpenApiParameter(
                "offset",
                description="Offset this number of rows.",
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                "limit",
                description="Limit this number of rows.",
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                "fields",
                description="Fields to return. Default: all fields in original order.",
                type={"type": "array", "items": {"type": "string"}},
            ),
            OpenApiParameter(
                "like",
                description="Matching conditions to select, e.g {'key1': 'a', 'key2': 'b'}.",
                type={"type": "object"},
            ),
            OpenApiParameter(
                "columns",
                description="Alias of fields.",
                type={"type": "array", "items": {"type": "string"}},
            ),
            OpenApiParameter(
                "sort",
                description="Comma separated field names with ordering e.g: 'fieldname1, fieldname2 desc'.",
                type={"type": "array", "items": {"type": "string"}},
            ),
            OpenApiParameter(
                "formato",
                description='Backward compatibility of "Accept" header or extension.',
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "nameRes",
                description="Force name of file to download.",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "name",
                description="Force name of file to download.",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "delimiter",
                description=(
                    "Delimiter for CSV format. Must be exactly one character "
                    "(and not '\\r' or '\\n'), otherwise the request is rejected "
                    "with a 400 error. Use ';' for semicolon-separated values. "
                    "Default is ',' (or ';' for the scsv format). Ignored for "
                    "non-CSV formats."
                ),
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                "_page",
                description="Deprecated. Number of the page.",
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                "_pageSize",
                description="Deprecated. Number of results in each page.",
                type=OpenApiTypes.INT,
            ),
        ],
        responses={
            200: {
                "description": "Successful response with data in the requested format",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "array",
                            "items": {"type": "object", "additionalProperties": True},
                        },
                        "example": [
                            {"id": 1, "name": "Example", "value": 100},
                            {"id": 2, "name": "Sample", "value": 200},
                        ],
                    },
                    "application/xml": {
                        "schema": {"type": "string"},
                        "example": '<?xml version="1.0" encoding="utf-8"?>\n<root><list-item><id>1</id><name>Example</name></list-item></root>',
                    },
                    "text/csv": {
                        "schema": {"type": "string"},
                        "example": "id,name,value\n1,Example,100\n2,Sample,200",
                    },
                    "application/yaml": {
                        "schema": {"type": "string"},
                        "example": "- id: 1\n  name: Example\n  value: 100\n- id: 2\n  name: Sample\n  value: 200",
                    },
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
                        "schema": {"type": "string", "format": "binary"}
                    },
                    "text/scsv": {
                        "schema": {"type": "string"},
                        "example": "id;name;value\n1;Example;100\n2;Sample;200",
                    },
                },
            },
            429: {
                "description": (
                    "An identical large download (same resource and parameters, whatever the file name) is already "
                    "being generated. Retry after the number of seconds in the Retry-After header. Only large "
                    "downloads are affected; HEAD and preview never are."
                ),
                "headers": {
                    "Retry-After": {
                        "description": "Seconds to wait before retrying.",
                        "schema": {"type": "integer"},
                    }
                },
                "content": {"application/problem+json": {"schema": {"type": "object"}}},
            },
            503: {
                "description": (
                    "The resource or connection is not available, or (error_code TOO_MANY_DOWNLOADS) too many large "
                    "downloads are running for this resource or in total. In the latter case retry after the number "
                    "of seconds in the Retry-After header."
                ),
                "headers": {
                    "Retry-After": {
                        "description": "Seconds to wait before retrying (too many large downloads only).",
                        "schema": {"type": "integer"},
                    }
                },
                "content": {"application/problem+json": {"schema": {"type": "object"}}},
            },
        },
    )
    def get(self, request: Request, **_kwargs) -> Response:
        """Este metodo permite acceder a los datos publicos de las bases de datos o APIs del Gobierno de Aragón.
         Si se selecciona el formato JSON y alguno de los campos que devuelve la función es tipo "shape" la respuesta
         sera en formato GEOJSON. Los parametros "fields" y "columns" no funcionan cuando se solicitan los datos en
         formato GEOJSON.

        This method allows get serialized public data from databases or APIs of Gobierno de Aragón. If JSON format is
        chosen and any field type returned by the function is "shape", the answer format will be GEOJSON. If data is
        in Geojson format, "fields" and "columns" parameters will not work."""
        timer = self._timer
        resource_id = self._get_resource_id(request)
        timer.attrs["resource_id"] = resource_id
        offset = self._get_offset(request)
        limit = self._get_limit(request)
        fields = self._get_fields(request)
        filters = self._get_filters(request)
        columns = self._get_columns(request)

        like = self._get_like(request)
        sort = self._get_sort(request)
        format = self._get_format(request)
        delimiter = self._get_delimiter(request, format)
        timer.attrs["format"] = format

        resource_config = _get_resource(resource_id=resource_id)
        logger.info("Downloading resource: %s", resource_config)

        # Large downloads (not previews) take a slot and a lock on their identity before any work is done on them.
        try:
            permit = (
                download_limits.acquire(
                    resource_id,
                    limit,
                    download_limits.normalized_url(resource_id, request.query_params, format),
                )
                if self.is_download_endpoint(request)
                else None
            )
        except (DownloadInProgress, TooManyDownloads) as err:
            timer.attrs["limited"] = err.limited
            logger.warning("Large download limited (%s): resource %s", err.limited, resource_id)
            raise
        try:
            response = self._build_download_response(
                request, resource_config, resource_id, format, delimiter, limit, offset, fields, filters, columns,
                like, sort,
            )
        except BaseException:
            if permit is not None:
                permit.release()
            raise
        if permit is not None:
            # Held until the server has sent the response, which is when its memory is still in use.
            permit.attach(response)
        return response

    def _build_download_response(
        self, request, resource_config, resource_id, format, delimiter, limit, offset, fields, filters, columns,
        like, sort,
    ):
        """Query the resource and build the response of ``get``."""
        timer = self._timer
        # The stored size is the size of the whole resource (what scripts/update_resources_size.py refreshes and
        # what download limits read): a filtered or partial download must not overwrite it.
        whole_resource = not (filters or like or fields or offset) and limit is None
        with contextlib.ExitStack() as stack:
            # One engine and one reflected table per request, shared by every step below
            # and disposed once when the stack closes, also on errors.
            shared = self._open_shared_resource(
                stack, resource_config, resource_id, timer, format == "xlsx"
            )
            if format == "xlsx":
                logger.info(
                    "Downloading resource in xlsx format: %s", resource_config
                )
                # One query, bounded to max + 1 rows: seeing one row over the maximum is enough to reject
                # the request without reading the whole resource. A user limit <= max never gets rejected.
                limit = _RESOURCE_MAX_ROWS_EXCEL + 1 if limit is None else min(
                    limit, _RESOURCE_MAX_ROWS_EXCEL + 1
                )

            reourceGeojon = get_GeoJson_resource(
                uri=resource_config.connector_config.uri,
                object_location=resource_config.object_location,
                object_location_schema=resource_config.object_location_schema,
                timer=timer,
                resource=shared,
            )
            featureCollection = bool(reourceGeojon and format == "json")
            download = None
            streamed_json = (
                format == "json" and not featureCollection and self._renders_plain_json(request)
            )
            if (
                (format in ("csv", "scsv") or streamed_json)
                and self.is_download_endpoint(request)
                and settings.DOWNLOAD_STREAMING
            ):
                logger.info("Downloading resource %s in %s format, streaming.", resource_id, format)
                disposition = self._content_disposition(request, resource_config)
                if streamed_json:
                    renderer = request.accepted_renderer
                    content_type = (
                        f"{renderer.media_type}; charset={renderer.charset}" if renderer.charset else renderer.media_type
                    )
                    encoder, make_blocks = JsonEncoder(renderer, columns, timer), json_blocks
                else:
                    content_type = CSV_CONTENT_TYPE
                    encoder, make_blocks = CsvEncoder(columns, delimiter, timer), csv_blocks
                download = self._open_stream(
                    stack, resource_config, resource_id, limit, offset, fields, filters, like, sort, shared,
                    whole_resource, encoder, make_blocks,
                )
            elif featureCollection:
                logger.info(
                    "Downloading resource %s in geojson format. FeatureCollection",
                    resource_id,
                )
                data = _get_data_public_error(
                    get_resource_data_feature,
                    uri=resource_config.connector_config.uri,
                    object_location=resource_config.object_location,
                    object_location_schema=resource_config.object_location_schema,
                    filters=filters,
                    like=like,
                    limit=limit,
                    offset=offset,
                    fields=fields,
                    sort=sort,
                    timer=timer,
                    resource=shared,
                )
            else:
                logger.info("Downloading resource %s in json format.", resource_id)
                data = _get_data_public_error(
                    get_resource_data,
                    uri=resource_config.connector_config.uri,
                    object_location=resource_config.object_location,
                    object_location_schema=resource_config.object_location_schema,
                    filters=filters,
                    like=like,
                    limit=limit,
                    offset=offset,
                    fields=fields,
                    sort=sort,
                    timer=timer,
                    resource=shared,
                )

        if download is not None:
            try:
                response = StreamedDownloadResponse(download, content_type=content_type)
                response["content-disposition"] = disposition
            except BaseException:
                download.close()
                raise
            return response

        if format == "xlsx":
            # Reject before normalizing, serializing or rendering a file that will not be returned.
            # MSSQL ignores limit/offset in SQL for now, so the row count is what decides there.
            data = list(data)
            if len(data) > _RESOURCE_MAX_ROWS_EXCEL:
                raise ValidationError(
                    "An xlsx cannot be generated with so many lines, please request it in another format",
                    ErrorCodes.TOO_MANY_ROWS,
                ) from TooManyRowsErrorExcel
            with phase(timer, "normalize"):
                data = get_return_list(data, format_is_xlsx=True)
            timer.attrs["rows"] = len(data)
            registries = len(data)
            with phase(timer, "normalize"):
                modified = modify_header(data, columns, format_is_xlsx=True)
            with phase(timer, "render"):
                response = get_response_xlsx(modified, resource_config)
            if whole_resource:
                _save_size_when_ready(response, resource_config, registries)
        elif format in ("csv", "scsv"):
            with phase(timer, "normalize"):
                data = get_return_list(data, format_is_xlsx=False)
            timer.attrs["rows"] = len(data)
            registries = len(data)
            with phase(timer, "normalize"):
                modified = modify_header(data, columns)
            with phase(timer, "render"):
                response = get_response_csv(modified, delimiter=delimiter)
            if whole_resource:
                _save_size_when_ready(response, resource_config, registries)
        elif featureCollection:
            timer.attrs["rows"] = len(data.get("features", ()))
            response = Response(data)
        else:
            with phase(timer, "normalize"):
                data = get_return_list(data, format_is_xlsx=False)
            timer.attrs["rows"] = len(data)
            registries = len(data)

            with phase(timer, "normalize"):
                response = Response(modify_header(data, columns))
            if whole_resource:
                _save_size_when_ready(response, resource_config, registries)

        if self.is_download_endpoint(request) or format == "xlsx":
            response["content-disposition"] = self._content_disposition(request, resource_config)

        return response

    @staticmethod
    def _renders_plain_json(request: Request) -> bool:
        """Whether the negotiated renderer is ``JSONRenderer`` and its output does not depend on the whole data.

        Only then the body can be built row by row: with ``indent`` in the ``Accept`` (or any other renderer, e.g. a
        subclass or the XML/YAML ones) it goes through the renderer as before.
        """
        renderer = request.accepted_renderer
        return type(renderer) is JSONRenderer and renderer.get_indent(request.accepted_media_type, {}) is None

    def _open_stream(
        self, stack, resource_config, resource_id, limit, offset, fields, filters, like, sort,
        shared, whole_resource, encoder, make_blocks,
    ) -> StreamedDownload:
        """Run the query and prepare the first block (CSV or JSON); the rest is produced while the response is sent.

        Everything that can fail on the request's own data (connection, object, fields, sort, query, columns,
        normalization and serialization of the first block) fails here, before a response exists, and maps to the
        usual HTTP error. On success the open resources move from ``stack`` to the returned ``StreamedDownload``, which
        closes them when the server closes the response.
        """
        timer = self._timer
        block_rows = settings.DOWNLOAD_BLOCK_ROWS
        rows = _get_data_public_error(
            get_resource_rows,
            uri=resource_config.connector_config.uri,
            object_location=resource_config.object_location,
            object_location_schema=resource_config.object_location_schema,
            filters=filters,
            like=like,
            limit=limit,
            offset=offset,
            fields=fields,
            sort=sort,
            timer=timer,
            resource=shared,
            block_rows=block_rows,
        )
        stack.callback(rows.close)  # closes the cursor and then its connection, before the engine is disposed
        first_rows = _get_data_public_error(rows.fetch, block_rows, True)
        first, rest = make_blocks(rows, encoder, block_rows, first_rows)
        t_first = monotonic() - timer.started_at
        on_complete = None
        if whole_resource:
            def on_complete(registries, size):
                update_resource_size(resource_config, registries, size)
        on_finished = None
        if getattr(settings, "DOWNLOAD_TIMING_LOG", True):
            def on_finished(finished):
                self._log_timing(timer, None, finished)
        return StreamedDownload(
            stack.pop_all(),
            first,
            rest,
            resource_id=resource_id,
            masked_uri=mask_uri(resource_config.connector_config.uri),
            started_at=timer.started_at,
            t_first=t_first,
            on_complete=on_complete,
            on_finished=on_finished,
        )

    def head(self, request: Request, **_kwargs) -> StreamingHttpResponse:
        """Answer HEAD with the headers of the download, without querying the resource.

        Without this method Django serves HEAD with ``get``: it queries the source, builds the whole file and the
        server then discards the body. Link checkers HEAD every catalogue URL several times a day (more than 2,000
        HEAD requests a day in November 2026, 466 of them to a 300k-700k row resource), and each one cost a full
        download.

        The request is validated as in ``get`` (resource, format and parameters), so an unknown resource or a bad
        parameter returns the same error. For database sources it also connects and reflects the table, as ``get``
        does before querying (median 0.2 s), so an unreachable source or a missing table returns the same 503. It
        never runs the query: field and sort names, the XLSX row limit and slow queries are not detected. HTTP
        sources are not opened, because opening one downloads the whole remote file.

        The response has no ``Content-Length``, since the size is only known after building the file; it is a
        streaming response so that ``CommonMiddleware`` does not announce 0 bytes. ``ResourceSizeConfig`` is not
        updated.
        """
        timer = self._timer
        resource_id = self._get_resource_id(request)
        timer.attrs["resource_id"] = resource_id
        self._get_offset(request)
        self._get_limit(request)
        self._get_fields(request)
        self._get_filters(request)
        self._get_columns(request)
        self._get_like(request)
        self._get_sort(request)
        format = self._get_format(request)
        self._get_delimiter(request, format)
        timer.attrs["format"] = format

        resource_config = _get_resource(resource_id=resource_id)
        if urlparse(resource_config.connector_config.uri).scheme not in _HTTP_SCHEMAS:
            with contextlib.ExitStack() as stack:
                self._open_shared_resource(stack, resource_config, resource_id, timer, False)

        response = StreamingHttpResponse((), content_type=self._head_content_type(request, format))
        if self.is_download_endpoint(request) or format == "xlsx":
            response["content-disposition"] = self._content_disposition(request, resource_config)
        return response

    @staticmethod
    def _head_content_type(request: Request, format: str) -> str:
        """Content-Type that ``get`` would send for ``format``."""
        if format == "xlsx":
            return XLSX_CONTENT_TYPE
        if format in ("csv", "scsv"):
            return CSV_CONTENT_TYPE
        # As DRF's Response does when it renders.
        charset = request.accepted_renderer.charset
        media_type = request.accepted_media_type
        return f"{media_type}; charset={charset}" if charset else media_type

    @staticmethod
    def _content_disposition(request: Request, resource_config: ResourceConfig) -> str:
        filename = (
            request.query_params.get("name")
            or request.query_params.get("nameRes")
            or resource_config.name
        )
        return f'attachment; filename="{filename}.{request.accepted_renderer.format}"'

    def get_filename(self, request: Request, resource_config: ResourceConfig):
        """Note: this is import due that replace XLSX Render method that forcer his own filename"""
        return (
            request.query_params.get("name")
            or request.query_params.get("nameRes")
            or resource_config.name
        )

    def is_download_endpoint(self, request: Request):
        return not self.is_preview

    @staticmethod
    def _get_fields(request: Request):
        # Las columnas se pueden obtener tanto con el parametro columns como con el parametro fields.
        # El formato de este parametro puede ser fields=field1,field2 o fields=field1 & fields=fields2
        # (este es el que utiliza el swagger)
        fields = []
        fields_param = ""
        if request.query_params.get("fields"):
            fields_param = request.query_params.get("fields")
        if fields_param:
            if "," not in fields_param:
                fields = request.query_params.getlist("fields")
            else:
                for field in fields_param.split(","):
                    fields.append(field)
        return fields

    @staticmethod
    def _get_columns(request: Request):
        # Las columnas se pueden obtener tanto con el parametro columns como con el parametro fields.
        # El formato de este parametro puede ser fields=field1,field2 o fields=field1 & fields=fields2
        # (este es el que utiliza el swagger)
        columns = []
        columns_param = ""
        if request.query_params.get("columns"):
            columns_param = request.query_params.get("columns")
        if columns_param:
            if "," not in columns_param:
                columns = request.query_params.getlist("columns", [])
            else:
                for field in columns_param.split(","):
                    columns.append(field)
        return columns

    @staticmethod
    def _get_resource_id(request: Request) -> int:
        """Get resource_id from query string.

        @param request: Django response instance.
        @return: resource_id.
        """
        resource_id = request.query_params.get(
            "resource_id"
        ) or request.query_params.get("view_id")
        if not resource_id:
            raise ValidationError(
                "It is required to specify resource_id in the query string.",
                ErrorCodes.REQUIRED,
            )

        try:
            resource_id = int(resource_id)
        except ValueError as err:
            logger.info("Resource_id is not a number. : %s", err)
            raise ValidationError(
                "Resource_id is not a number.", ErrorCodes.NOT_A_NUMBER
            ) from err

        return resource_id

    @staticmethod
    def _get_int_field(request: Request, field) -> int:
        """Get interger field from query string.

        @param request: Django response instance.
        @return: SQL offset value.
        """
        value = request.query_params.get(field)
        if value:
            try:
                value = int(request.query_params.get(field))
            except ValueError as err:
                raise ValidationError(
                    f"Value of {field} is not a number.", ErrorCodes.NOT_A_NUMBER
                ) from err

        return value

    def _get_offset(self, request: Request) -> int:
        """Get offset from query string.

        @param request: Django response instance.
        @return: SQL offset value.
        """

        offset = self._get_int_field(request, "offset")
        page = self._get_int_field(request, "_page")
        page_size = self._get_int_field(request, "_pageSize")

        if not page and not page_size:
            return offset

        try:
            if page > 0 and page_size:
                offset = (page - 1) * page_size
        except TypeError:
            offset = 0

        return offset

    def _get_limit(self, request: Request) -> Optional[int]:
        """Get limit from query string. If preview, limit number of rows.

        @param request: Django response instance.
        @return: SQL limit value.
        """
        limit = request.query_params.get("limit") or None
        page_size = self._get_int_field(request, "_pageSize")

        if limit:
            try:
                limit = int(limit)
            except ValueError as err:
                raise ValidationError(
                    "Value of limit is not a number.", ErrorCodes.NOT_A_NUMBER
                ) from err
        elif page_size:
            try:
                limit = int(page_size)
            except ValueError as err:
                raise ValidationError(
                    "Value of _pageSize is not a number.", ErrorCodes.NOT_A_NUMBER
                ) from err

        if self.is_preview and (limit is None or limit <= 0 or limit > self._PREVIEW_LIMIT):
            # A preview is always bounded: no limit, zero, negative or too large end up at the cap.
            limit = self._PREVIEW_LIMIT

        return limit

    @staticmethod
    def _get_filters(request: Request) -> Dict[str, Any]:
        """Get filters from query string.

        @param request: Django response instance.
        @return: filters. SQL where parameters. Format {"column": value, ...}.
        """
        try:
            filters = json.loads(request.query_params.get("filters", "{}"))
        except JSONDecodeError as err:
            raise ValidationError("Invalid JSON.", ErrorCodes.INVALID_JSON) from err

        if not isinstance(filters, dict):
            raise ValidationError(
                "Invalid format: eg. {“key1”: “a”, “key2”: “b”}",
                ErrorCodes.INVALID_FILTER,
            )

        for _, value in filters.items():
            if (
                type(value) not in (str, int, float, bool, dict, list, None)
                and value is not None
            ):
                raise ValidationError(
                    f"Value {value} is not a String, Integer, Float, Bool, Dict, List, Null or None",
                    ErrorCodes.INVALID_FILTER,
                )
        return filters

    @staticmethod
    def _get_like(request: Request) -> Dict[str, Any]:
        """Get filters_like from query string.

        @param request: Django response instance.
        @return: filters_like. SQL where parameters. Format {"column": value, ...}.
        """
        try:
            like = json.loads(request.query_params.get("like", "{}"))

        except JSONDecodeError as err:
            raise ValidationError("Invalid JSON.", ErrorCodes.INVALID_JSON) from err

        if not isinstance(like, dict):
            raise ValidationError(
                "Invalid format: eg. {“key1”: “a”, “key2”: “b”}",
                ErrorCodes.INVALID_FILTER,
            )
        for _, value in like.items():
            if type(value) not in (str, int, float, bool, None) and value is not None:
                raise ValidationError(
                    f"Value {value} is not a String, Integer, Float, Bool, Null or None",
                    ErrorCodes.INVALID_FILTER,
                )
        return like

    @staticmethod
    def _get_sort(request: Request) -> List[OrderBy]:
        """Get sort options from query string.

        @param request: Django response instance.
        @return: List of OrderBy. This are used in SQL sentences.
        """
        sort = []
        for item in request.query_params.get("sort", "").strip().split(","):
            item: str
            if not item:
                continue
            clause = item.strip().split(" ")
            if len(clause) == 1:
                sort.append(OrderBy(field=clause[0], ascending=True))
            elif len(clause) == 2:
                if clause[1].lower() == "asc":
                    sort.append(OrderBy(field=clause[0], ascending=True))
                elif clause[1].lower() == "desc":
                    sort.append(OrderBy(field=clause[0], ascending=False))
                else:
                    raise ValidationError(
                        f"Sort value {item} is not allowed. Ej: fieldname1 asc, fieldname2 desc.",
                        ErrorCodes.INVALID_SORT,
                    )
            else:
                raise ValidationError(
                    f"Sort value {item} is not allowed. Too many arguments.",
                    ErrorCodes.INVALID_SORT,
                )

        return sort

    @staticmethod
    def _get_format(request: Request) -> str:
        """Get Response_type from query string.

        @param request: Django response instance.
        @return:Response_content_type.
        """
        try:
            format = request.accepted_renderer.format
        except ValueError as err:
            raise ValidationError(
                "Invalid Response_contnt_type.", ErrorCodes.INVALID_FORMAT
            ) from err

        return format

    @staticmethod
    def _get_delimiter(request: Request, format: str) -> Optional[str]:
        """Get and validate the CSV delimiter from query string.

        Only applies to the "csv" and "scsv" formats; for any other format the
        parameter is ignored (not validated) to keep today's behavior of silently
        accepting it. Validated early, before any data is fetched, so an invalid
        value fails fast instead of costing a full query first.

        @param request: Django response instance.
        @param format: resolved response format (e.g. "csv", "scsv", "json", "xlsx").
        @return: delimiter to use, or None when the format is not csv/scsv.
        """
        if format not in ("csv", "scsv"):
            return None

        default_delimiter = ";" if format == "scsv" else ","
        delimiter = request.query_params.get("delimiter", None)

        if not delimiter:
            return default_delimiter

        if len(delimiter) != 1 or delimiter in ("\r", "\n"):
            raise ValidationError(
                "Invalid delimiter: it must be a single character (and not a "
                "line break).",
                ErrorCodes.INVALID_DELIMITER,
            )

        return delimiter


class ShowColumnsView(XLSXFileMixin, APIViewMixin):
    """This view allows to get datatype of each column from a resource. If the view has a shape file download and
    preview endpoints' response will be in geojson format if JSON response type is selected-"""

    content_negotiation_class = HtmlFallbackContentNegotiation
    renderer_classes = PUBLIC_DATA_RENDERER_CLASSES

    @staticmethod
    @extend_schema(
        tags=["default"],
        parameters=[
            OpenApiParameter(
                "resource_id",
                description="",
                type=OpenApiTypes.NUMBER,
            ),
            OpenApiParameter(
                "view_id",
                description="Alias of resource_id. Backward compatibility.",
                type=OpenApiTypes.NUMBER,
            ),
        ],
    )
    def get(request: Request, **_kwargs) -> Response:
        """
        Devuelve todos los campos de un recurso/vista. Si la vista tiene algun campo tipo "shape" en las funciones
         preview y download la respuestas JSON se devolveran en formato GEOJSON.

        This view allows to get datatype of each column from a resource. If the view has any "shape" field in the
         preview and download functions, the JSON responses will be returned in GEOJSON format."""
        resource_id = request.query_params.get(
            "resource_id"
        ) or request.query_params.get("view_id")
        resource_config = _get_resource(resource_id=resource_id)
        data = _get_data_public_error(
            get_resource_columns,
            uri=resource_config.connector_config.uri,
            object_location=resource_config.object_location,
            object_location_schema=resource_config.object_location_schema,
        )

        return Response(get_return_list(data, format_is_xlsx=False))


class ResourcesView(XLSXFileMixin, APIViewMixin):  # pylint: disable=too-few-public-methods
    """
    Devuelve el listado de todas las vistas que se pueden consultar.
    This view allow to get a list of public resources."""

    content_negotiation_class = HtmlFallbackContentNegotiation
    renderer_classes = PUBLIC_DATA_RENDERER_CLASSES

    @staticmethod
    @extend_schema(tags=["default"])
    def get(_: Request, **_kwargs) -> Response:
        """
        Devuelve el listado de todas las vistas que se pueden consultar.
        This view allow to get a list of public resources."""
        resources = (
            {
                "id": resource.id,
                "name": resource.name,
                "available": resource.enabled and resource.connector_config.enabled,
            }
            for resource in ResourceConfig.objects.order_by("id")
            .prefetch_related("connector_config")
            .all()
        )
        return Response(get_return_list(resources, format_is_xlsx=False))
