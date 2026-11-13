"""Streaming of a download: the owner of everything a response needs while it is being sent, and the CSV, JSON, GeoJSON
and XML encoders.

A download used to be built whole in memory before the first byte left. Here the response is an iterator of blocks
(``StreamedDownload``): it is handed to ``StreamingHttpResponse``, which Django closes (``response.close()``) when the
server has finished with it, whether it sent everything or the client went away. That ``close()`` is what releases the
connection to the source, so the owner of the resources is the iterator and not the request that created it.
"""

import contextlib
import csv
import io
import logging
import os
import threading
import weakref
from time import monotonic
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from django.http import StreamingHttpResponse
from rest_framework.renderers import LONG_SEPARATORS, SHORT_SEPARATORS

import xml_output
from connectors import RowStream
from phase_timing import PhaseTimer, phase
from utils import get_return_list, modify_header

logger = logging.getLogger(__name__)

# How a streamed download ended (the ``result`` field of the ``download_timing`` line).
COMPLETE = "complete"  # the last block was requested and there was none
ERROR = "error"  # generating a block failed
EARLY_CLOSE = "early_close"  # closed with neither: with sync workers, nearly always a failed write to the client

# A block: its bytes and the number of rows it holds.
Block = Tuple[bytes, int]

_PAGE_SIZE_KB = os.sysconf("SC_PAGE_SIZE") // 1024 if hasattr(os, "sysconf") else 4


def current_rss_kb() -> int:
    """Resident memory of this process right now, in KiB; ``-1`` where ``/proc`` is not available."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            return int(statm.read().split()[1]) * _PAGE_SIZE_KB
    except (OSError, ValueError, IndexError):
        return -1


def _close_quietly(stack: contextlib.ExitStack) -> None:
    try:
        stack.close()
    except Exception:  # noqa: BLE001 - closing must never raise
        logger.warning("Could not close the resources of a streamed download", exc_info=True)


class StreamedDownload:
    """Iterator of the byte blocks of a download that owns what feeds it.

    ``stack`` closes, in reverse order of registration, the cursor, the connection and the engine of the source. It is
    closed when the blocks run out, when one fails and, as the last resort, in ``close()``.

    * ``first`` is the already prepared first block (so its errors are still reported as HTTP errors) and ``blocks``
      yields the rest, ending when the data does. Neither may yield an empty block that is not the very first one
      (an empty chunk would end a chunked response).
    * ``on_complete(rows, bytes)`` runs once, when the last block has been delivered and before the resources are
      released (it stores the size of the resource). Its failures are logged and ignored.
    * ``on_finished(download)`` runs once, in ``close()``, with the final ``result``, ``rows`` and ``bytes``
      (it writes the ``download_timing`` line). Its failures are logged and ignored.

    ``rows`` and ``bytes`` count what was delivered to the server, not what the client received.
    """

    def __init__(
        self,
        stack: contextlib.ExitStack,
        first: Block,
        blocks: Iterator[Block],
        *,
        resource_id: Any,
        masked_uri: str,
        started_at: Optional[float] = None,
        t_first: float = 0.0,
        on_complete: Optional[Callable[[int, int], None]] = None,
        on_finished: Optional[Callable[["StreamedDownload"], None]] = None,
    ):
        self._stack = stack
        self._pending: Optional[Block] = first
        self._blocks = blocks
        self.resource_id = resource_id
        self.masked_uri = masked_uri
        self.started_at = monotonic() if started_at is None else started_at
        self.t_first = t_first
        self._on_complete = on_complete
        self._on_finished = on_finished
        self.result: Optional[str] = None
        self.rows = 0
        self.bytes = 0
        self._closed = False
        self._lock = threading.Lock()
        # Safety net when the response is neither iterated nor closed (e.g. a middleware failed after the view).
        # It is not deterministic: it runs when the object is collected.
        self._finalizer = weakref.finalize(self, _close_quietly, stack)
        self._finalizer.atexit = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        while True:
            if self._closed or self.result is not None:
                raise StopIteration
            try:
                if self._pending is not None:
                    block, self._pending = self._pending, None
                else:
                    block = next(self._blocks)
            except StopIteration:
                block = None
            except BaseException:
                self._fail()
                raise
            if block is None:  # outside the handler, so what _complete logs is not chained to the StopIteration
                self._complete()
                raise StopIteration
            data, rows = block
            self.rows += rows
            if not data:
                continue
            self.bytes += len(data)
            return data

    def _complete(self) -> None:
        self.result = COMPLETE
        if self._on_complete is not None:
            try:
                self._on_complete(self.rows, self.bytes)
            except Exception:  # noqa: BLE001 - the download did complete
                logger.warning("Could not store the size of resource_id=%s", self.resource_id, exc_info=True)
        self._release()

    def _fail(self) -> None:
        self.result = ERROR
        logger.error(
            "Download failed while streaming: resource_id=%s uri=%s rows_sent=%s bytes_sent=%s elapsed=%.1fs",
            self.resource_id,
            self.masked_uri,
            self.rows,
            self.bytes,
            monotonic() - self.started_at,
            exc_info=True,
        )
        self._release()

    def _release(self) -> None:
        blocks = self._blocks
        if hasattr(blocks, "close"):
            try:
                blocks.close()
            except Exception:  # noqa: BLE001
                logger.warning("Could not close the blocks of a streamed download", exc_info=True)
        _close_quietly(self._stack)

    def close(self) -> None:
        """Called by the server when it is done with the response. Idempotent; never raises."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if self.result is None:
            self.result = EARLY_CLOSE
        self._release()
        self._finalizer.detach()
        if self._on_finished is not None:
            try:
                self._on_finished(self)
            except Exception:  # noqa: BLE001 - diagnostics must never break the response
                logger.warning("download_timing could not be logged", exc_info=True)


class StreamedDownloadResponse(StreamingHttpResponse):
    """Response whose body is a ``StreamedDownload`` (closed by Django through ``response.close()``)."""


class CsvEncoder:
    """Turns blocks of normalized rows into CSV bytes identical to what one pass over all the rows gives.

    The header (the keys of the first row, renamed by ``columns_name``) is written with the first block of rows; with no
    rows there is no header at all.
    """

    def __init__(self, columns_name: List[str], delimiter: str, timer: Optional[PhaseTimer] = None):
        self._columns_name = columns_name
        self._delimiter = delimiter
        self._timer = timer
        self._header_written = False

    def encode(self, rows: List[Dict[str, Any]]) -> Block:
        if not rows:
            return b"", 0
        with phase(self._timer, "normalize"):
            data = get_return_list(rows, format_is_xlsx=False)
            data = modify_header(data, self._columns_name)
        with phase(self._timer, "render"):
            buffer = io.StringIO()
            writer = csv.writer(buffer, delimiter=self._delimiter)
            if not self._header_written:
                writer.writerow(list(data[0].keys()))
            writer.writerows(item.values() for item in data)
            encoded = buffer.getvalue().encode("utf-8")
        self._header_written = True
        return encoded, len(data)


class JsonEncoder:
    """Turns blocks of normalized rows into the bytes ``JSONRenderer`` gives for all the rows at once (no ``indent``).

    ``[`` comes with the first block, the rows are separated by the renderer's item separator and ``]`` is the last
    block (``close``), so a body that is cut never ends in a valid JSON document. Every row goes through the same
    ``json.dumps`` configuration as the renderer, built once from its attributes.
    """

    def __init__(self, renderer, columns_name: List[str], timer: Optional[PhaseTimer] = None):
        separators = SHORT_SEPARATORS if renderer.compact else LONG_SEPARATORS
        self._item_separator = separators[0]
        self._dumps = renderer.encoder_class(
            ensure_ascii=renderer.ensure_ascii, allow_nan=not renderer.strict, separators=separators
        ).encode
        self._columns_name = columns_name
        self._timer = timer
        self._started = False

    def encode(self, rows: List[Dict[str, Any]]) -> Block:
        if not rows:
            return (b"" if self._started else b"["), 0
        with phase(self._timer, "normalize"):
            data = get_return_list(rows, format_is_xlsx=False)
            data = modify_header(data, self._columns_name)
        with phase(self._timer, "render"):
            dumps = self._dumps
            body = self._item_separator.join([dumps(item) for item in data])
            body = body.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
            if self._started:
                body = self._item_separator + body
            else:
                body = "[" + body
                self._started = True
        return body.encode(), len(data)

    @staticmethod
    def close() -> Block:
        return b"]", 0


class GeoJsonEncoder:
    """Turns blocks of ``(properties, geometry)`` features into the bytes ``JSONRenderer`` gives for the whole
    FeatureCollection (compact separators, no ``indent``).

    The geometry is copied as ``ST_AsGeoJSON`` returned it, without parsing it and serializing it again: in production
    (PostGIS 2.1) that text is byte for byte what the renderer wrote for it. ``null`` stands for a NULL geometry. The
    properties are already normalized (``connectors.feature_properties``) and go through the renderer's ``json.dumps``
    configuration. ``{"type":"FeatureCollection","features":[`` comes with the first block and ``]}`` is the last one
    (``close``), so a body that is cut never ends in a valid JSON document. Use it with ``json_blocks``.
    """

    _START = '{"type":"FeatureCollection","features":['

    def __init__(self, renderer, timer: Optional[PhaseTimer] = None):
        if not renderer.compact:
            raise ValueError("GeoJSON is only streamed with the compact JSON renderer")
        self._dumps = renderer.encoder_class(
            ensure_ascii=renderer.ensure_ascii, allow_nan=not renderer.strict, separators=SHORT_SEPARATORS
        ).encode
        self._timer = timer
        self._started = False

    def encode(self, features: List[Tuple[Dict[str, Any], Optional[str]]]) -> Block:
        if not features:
            return (b"" if self._started else self._START.encode()), 0
        with phase(self._timer, "render"):
            dumps = self._dumps
            body = ",".join(
                [
                    '{"type":"Feature","geometry":' + (geometry if geometry is not None else "null")
                    + ',"properties":' + dumps(properties) + "}"
                    for properties, geometry in features
                ]
            )
            body = body.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
            if self._started:
                body = "," + body
            else:
                body = self._START + body
                self._started = True
        return body.encode(), len(features)

    @staticmethod
    def close() -> Block:
        return b"]}", 0


def json_blocks(stream: RowStream, encoder: JsonEncoder, block_rows: int, first_rows: List[Dict[str, Any]]):
    """Prepare the first block now and return ``(first, rest)`` like ``csv_blocks``; ``]`` is the last block of ``rest``.

    The ``]`` is only produced when the rows run out without error: a failing or abandoned generator never reaches it.
    """
    first = encoder.encode(first_rows)

    def rest() -> Iterator[Block]:
        if first_rows:
            while True:
                rows = stream.fetch(block_rows)
                if not rows:
                    break
                yield encoder.encode(rows)
        yield encoder.close()

    return first, rest()


class XmlEncoder:
    """Turns blocks of normalized rows into the bytes ``GaodXMLRenderer`` gives for all the rows at once.

    The declaration and the opening ``<root>`` come with the first block (also with no rows at all) and ``</root>`` is
    the last block (``close``), so a body that is cut never ends in a well-formed document. Escaping and names are
    those of ``xml_output``: nothing is duplicated here.
    """

    def __init__(self, columns_name: List[str], timer: Optional[PhaseTimer] = None):
        self._columns_name = columns_name
        self._timer = timer
        self._started = False

    def encode(self, rows: List[Dict[str, Any]]) -> Block:
        if not rows:
            return (b"" if self._started else self._start().encode()), 0
        with phase(self._timer, "normalize"):
            data = get_return_list(rows, format_is_xlsx=False)
            data = modify_header(data, self._columns_name)
        with phase(self._timer, "render"):
            body = xml_output.encode_items(data)
            if not self._started:
                body = self._start() + body
            return body.encode("utf-8"), len(data)

    def _start(self) -> str:
        self._started = True
        return xml_output.XML_DECLARATION + xml_output.ROOT_OPEN

    @staticmethod
    def close() -> Block:
        return xml_output.ROOT_CLOSE.encode(), 0


def xml_blocks(stream: RowStream, encoder: XmlEncoder, block_rows: int, first_rows: List[Dict[str, Any]]):
    """Prepare the first block now and return ``(first, rest)`` like ``json_blocks``; ``</root>`` is the last of ``rest``.

    ``</root>`` is only produced when the rows run out without error: a failing or abandoned generator never reaches it.
    """
    first = encoder.encode(first_rows)

    def rest() -> Iterator[Block]:
        if first_rows:
            while True:
                rows = stream.fetch(block_rows)
                if not rows:
                    break
                yield encoder.encode(rows)
        yield encoder.close()

    return first, rest()


def csv_blocks(stream: RowStream, encoder: CsvEncoder, block_rows: int, first_rows: List[Dict[str, Any]]):
    """Prepare the first block now and return ``(first, rest)``: the first block's bytes and a generator of the others.

    The first block is encoded here, so a failure of the query, the normalization or the serialization surfaces
    before the response exists.
    """
    first = encoder.encode(first_rows)

    def rest() -> Iterator[Block]:
        if not first_rows:
            return
        while True:
            rows = stream.fetch(block_rows)
            if not rows:
                return
            yield encoder.encode(rows)

    return first, rest()
