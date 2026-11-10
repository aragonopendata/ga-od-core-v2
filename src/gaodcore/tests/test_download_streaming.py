"""CSV, SCSV, JSON and XML downloads are streamed: the owner of the resources (``StreamedDownload``), the result it reports in the
``download_timing`` line and the errors before and after the first block.

The data comes from SQLite files read through the real connector code. The blocks are made small (``DOWNLOAD_BLOCK_ROWS``)
so that a download has several. ``client`` reads a streamed response to the end; ``streaming_client`` hands it back
unread so a test can cut it half way, as a server does when the client goes away.
"""

import gc
import logging
import math
from contextlib import ExitStack
from unittest.mock import patch

import pytest
import sqlalchemy.exc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.cursor import CursorResult

import connectors
import download_stream
import xml_output
from download_stream import COMPLETE, EARLY_CLOSE, ERROR, StreamedDownload
from gaodcore import views as gaodcore_views
from rest_framework_xml.renderers import XMLRenderer as OldXMLRenderer
from utils import get_return_list, modify_header
from xml_output import GaodXMLRenderer
from gaodcore_manager.models import ConnectorConfig, ResourceConfig, ResourceSizeConfig

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"
ROWS = 10
BLOCK_ROWS = 3


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _capture(logger):
    handler = _ListHandler()
    logger.addHandler(handler)
    return handler


@pytest.fixture
def timing_lines():
    handler = _capture(gaodcore_views.timing_logger)
    yield lambda: [r.getMessage() for r in handler.records if r.getMessage().startswith("download_timing ")]
    gaodcore_views.timing_logger.removeHandler(handler)


@pytest.fixture
def error_records():
    handler = _capture(download_stream.logger)
    yield lambda: [r for r in handler.records if r.levelno >= logging.ERROR]
    download_stream.logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def _small_blocks(settings):
    settings.DOWNLOAD_BLOCK_ROWS = BLOCK_ROWS
    settings.DOWNLOAD_STREAMING = True


def _make_resource(tmp_path, name, statements, location="things"):
    uri = f"sqlite:///{tmp_path / (name + '.db')}"
    engine = create_engine(uri)
    with engine.begin() as conn:
        for statement, params in statements:
            conn.execute(text(statement), params)
    engine.dispose()
    connector = ConnectorConfig.objects.create(name=f"connector_{name}", uri=uri, enabled=True)
    return ResourceConfig.objects.create(
        name=name, connector_config=connector, object_location=location, enabled=True
    )


@pytest.fixture
def resource(tmp_path):
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT, price REAL)", {})]
    statements += [
        ("INSERT INTO things VALUES (:i, :n, :p)", {"i": i, "n": f"note, {i}", "p": i * 1.5}) for i in range(1, ROWS + 1)
    ]
    return _make_resource(tmp_path, "things", statements)


def get(client, resource, fmt="csv", **params):
    return client.get(DOWNLOAD, {"resource_id": resource.id, "format": fmt, "sort": "id", **params})


@pytest.fixture
def tracker():
    """What is open on the source: connections not yet closed and engines not yet disposed, plus the order of closing."""

    class Tracker:
        def __init__(self):
            self.connections = set()
            self.engines = set()
            self.events = []

        def open_connections(self):
            return len(self.connections)

        def undisposed_engines(self):
            return len(self.engines)

        def last(self, event):
            return max(i for i, e in enumerate(self.events) if e == event)

    t = Tracker()
    real_get_engine = connectors._get_engine
    real_connect, real_conn_close = Engine.connect, Connection.close
    real_dispose, real_result_close = Engine.dispose, CursorResult.close

    def get_engine(*args, **kwargs):
        engine = real_get_engine(*args, **kwargs)
        t.engines.add(id(engine))
        return engine

    def connect(self, *args, **kwargs):
        conn = real_connect(self, *args, **kwargs)
        t.connections.add(id(conn))
        return conn

    def conn_close(self):
        t.connections.discard(id(self))
        t.events.append("connection")
        return real_conn_close(self)

    def dispose(self, *args, **kwargs):
        t.engines.discard(id(self))
        t.events.append("engine")
        return real_dispose(self, *args, **kwargs)

    def result_close(self):
        t.events.append("result")
        return real_result_close(self)

    with (
        patch.object(connectors, "_get_engine", get_engine),
        patch.object(Engine, "connect", connect),
        patch.object(Connection, "close", conn_close),
        patch.object(Engine, "dispose", dispose),
        patch.object(CursorResult, "close", result_close),
    ):
        yield t


pytestmark = pytest.mark.django_db


# --- StreamedDownload alone ----------------------------------------------------------------------------------------

class _Source:
    """Stack, blocks and callbacks that record what happens to them."""

    def __init__(self, blocks):
        self.events = []
        self.stack = ExitStack()
        self.stack.callback(self.events.append, "closed")
        self.generator = self._generate(blocks)
        self.completed = []
        self.finished = []

    def _generate(self, blocks):
        try:
            for block in blocks:
                if isinstance(block, Exception):
                    raise block
                yield block
        finally:
            self.events.append("generator closed")

    def download(self, first=(b"a", 1), **kwargs):
        return StreamedDownload(
            self.stack,
            first,
            self.generator,
            resource_id=7,
            masked_uri="postgresql://user:***@host/db",
            on_complete=lambda rows, size: self.completed.append((rows, size)),
            on_finished=self.finished.append,
            **kwargs,
        )


def test_exhausted_download_is_complete_and_closes_everything_once():
    source = _Source([(b"bb", 2), (b"ccc", 3)])
    download = source.download()

    assert list(download) == [b"a", b"bb", b"ccc"]
    assert (download.result, download.rows, download.bytes) == (COMPLETE, 6, 6)
    assert source.completed == [(6, 6)]
    assert source.events.count("closed") == 1  # closed as soon as the last block is delivered
    assert source.finished == []  # the line is written by close(), not before

    download.close()
    download.close()

    assert source.finished == [download] and download.result == COMPLETE
    assert source.events.count("closed") == 1


def test_close_midway_is_early_close_without_size():
    source = _Source([(b"bb", 2), (b"ccc", 3)])
    download = source.download()
    assert next(download) == b"a"

    download.close()

    assert download.result == EARLY_CLOSE
    assert (download.rows, download.bytes) == (1, 1)
    assert source.completed == []
    assert source.events == ["closed"]
    assert source.generator.gi_frame is None  # the generator of the remaining blocks was closed too
    assert list(download) == []  # nothing more is produced after close
    assert source.finished == [download]


def test_close_never_iterated_is_early_close():
    source = _Source([(b"bb", 2)])
    download = source.download()

    download.close()

    assert (download.result, download.rows, download.bytes) == (EARLY_CLOSE, 0, 0)
    assert source.events.count("closed") == 1


def test_failing_block_is_reraised_logged_and_closes_everything(error_records):
    boom = RuntimeError("block 3 failed")
    source = _Source([(b"bb", 2), boom])
    download = source.download()

    assert next(download) == b"a"
    assert next(download) == b"bb"
    with pytest.raises(RuntimeError, match="block 3 failed"):
        next(download)

    assert download.result == ERROR
    assert source.completed == []
    assert source.events.count("closed") == 1
    (record,) = error_records()
    message = record.getMessage()
    assert "resource_id=7" in message and "postgresql://user:***@host/db" in message
    assert "rows_sent=3" in message and "bytes_sent=3" in message
    with pytest.raises(StopIteration):
        next(download)  # a failed download does not go on

    download.close()
    assert download.result == ERROR  # close() does not turn it into early_close
    assert source.finished == [download]


def test_empty_blocks_are_never_delivered():
    source = _Source([(b"", 0), (b"bb", 2), (b"", 0)])

    assert list(source.download(first=(b"", 0))) == [b"bb"]


def test_failing_size_update_keeps_the_download_complete(caplog):
    source = _Source([])
    download = StreamedDownload(
        source.stack, (b"a", 1), source.generator, resource_id=7, masked_uri="x", on_complete=_raise
    )

    with caplog.at_level(logging.WARNING, logger="download_stream"):
        assert list(download) == [b"a"]

    assert download.result == COMPLETE
    assert "Could not store the size of resource_id=7" in caplog.text


def _raise(*_args):
    raise RuntimeError("database down")


def test_close_never_raises(caplog):
    source = _Source([(b"bb", 2)])
    source.stack.callback(_raise)
    download = source.download()

    with caplog.at_level(logging.WARNING, logger="download_stream"):
        download.close()
        download.close()

    assert "Could not close the resources" in caplog.text
    assert download.result == EARLY_CLOSE

    unlogged = _Source([])
    broken = StreamedDownload(
        unlogged.stack, (b"a", 1), unlogged.generator, resource_id=7, masked_uri="x", on_finished=_raise
    )
    broken.close()  # the line failing to be written must not break the response


def test_download_never_iterated_nor_closed_is_closed_by_the_finalizer():
    source = _Source([(b"bb", 2)])
    download = source.download()
    assert source.events == []

    del download
    gc.collect()

    assert source.events == ["closed"]


# --- The endpoint --------------------------------------------------------------------------------------------------

STREAM_CASES = [
    pytest.param("csv", {}, id="csv"),
    pytest.param("scsv", {}, id="scsv"),
    pytest.param("csv", {"delimiter": "|"}, id="delimiter"),
    pytest.param("csv", {"columns": "i,n,p"}, id="columns"),
    pytest.param("csv", {"fields": "note,id", "columns": "n,i", "sort": "id desc"}, id="fields_columns_sort"),
    pytest.param("csv", {"limit": "4", "offset": "2"}, id="limit_offset"),
    pytest.param("csv", {"filters": '{"id": 7}'}, id="filter_one_row"),
    pytest.param("csv", {"filters": '{"id": 99}'}, id="filter_no_rows"),
    pytest.param("scsv", {"like": '{"note": "note, 1"}'}, id="like"),
]


@pytest.mark.parametrize("fmt,params", STREAM_CASES)
def test_body_is_the_same_streamed_or_built_whole(client, settings, resource, fmt, params):
    streamed = get(client, resource, fmt, **params)
    settings.DOWNLOAD_STREAMING = False
    whole = get(client, resource, fmt, **params)

    assert streamed.status_code == whole.status_code == 200
    assert streamed.content == whole.content
    assert streamed["Content-Type"] == whole["Content-Type"] == "text/csv"
    assert streamed["Content-Disposition"] == whole["Content-Disposition"]


def test_download_is_streamed_in_blocks_and_nothing_is_read_ahead(streaming_client, resource):
    response = get(streaming_client, resource)
    assert response.streaming and response.status_code == 200

    with patch.object(connectors.RowStream, "fetch", autospec=True, side_effect=connectors.RowStream.fetch) as fetch:
        chunks = iter(response.streaming_content)
        first = next(chunks)
        assert first.startswith(b"id,note,price\r\n")
        assert fetch.call_count == 0  # the first block was read before the response existed
        rest = list(chunks)

    assert len(rest) == math.ceil(ROWS / BLOCK_ROWS) - 1
    assert fetch.call_count == len(rest) + 1  # the last call finds no more rows
    assert b"".join([first, *rest]).count(b"\r\n") == ROWS + 1


def test_the_other_formats_and_the_preview_are_not_streamed(client, settings, resource):
    assert not get(client, resource, "csv", **{}).streaming
    assert not isinstance(get(client, resource, "yaml"), download_stream.StreamedDownloadResponse)
    preview = client.get(PREVIEW, {"resource_id": resource.id, "format": "csv"})
    assert preview.status_code == 200 and not isinstance(preview, download_stream.StreamedDownloadResponse)
    settings.DOWNLOAD_STREAMING = False
    assert not isinstance(get(client, resource, "csv"), download_stream.StreamedDownloadResponse)


def test_empty_result_has_no_header_and_completes(client, tmp_path, timing_lines):
    empty = _make_resource(tmp_path, "empty", [("CREATE TABLE things (id INTEGER PRIMARY KEY)", {})])

    response = get(client, empty)

    assert response.status_code == 200 and response.content == b""
    assert " rows=0 " in timing_lines()[0] and " result=complete " in timing_lines()[0]


def test_timing_line_is_written_when_the_sending_ends(streaming_client, resource, timing_lines):
    response = get(streaming_client, resource)
    assert timing_lines() == []  # not when the response is created

    body = b"".join(response.streaming_content)

    (line,) = timing_lines()
    assert f" rows={ROWS} " in line and f" bytes={len(body)} " in line and " status=200 " in line
    assert " format=csv " in line and " endpoint=download " in line
    # New fields come last, after the ones existing parsers know.
    assert line.index(" maxrss_kb=") < line.index(" result=complete ") < line.index(" t_first=") < line.index(" rss_kb=")
    assert line.split(" rss_kb=")[1].isdigit() and int(line.split(" rss_kb=")[1]) > 0
    assert float(line.split(" t_first=")[1].split()[0]) <= float(line.split(" t_total=")[1].split()[0])


def test_size_of_the_resource_is_stored_with_the_rows_and_bytes_delivered(client, resource):
    response = get(client, resource)

    stored = ResourceSizeConfig.objects.get(resource_id=resource)
    assert (stored.registries, stored.size) == (ROWS, len(response.content))


def test_partial_download_does_not_store_the_size(client, resource):
    get(client, resource, limit="3")
    get(client, resource, fields="id")

    assert not ResourceSizeConfig.objects.exists()


def test_client_going_away_is_early_close_and_releases_the_source(streaming_client, resource, timing_lines, tracker):
    response = get(streaming_client, resource)
    chunks = iter(response.streaming_content)
    next(chunks)  # one block sent, the server then fails to write and closes the response
    assert tracker.open_connections() == 1 and tracker.undisposed_engines() == 1

    response.close()

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0
    (line,) = timing_lines()
    assert " result=early_close " in line and " status=200 " in line
    assert f" rows={BLOCK_ROWS} " in line
    assert not ResourceSizeConfig.objects.exists()
    response.close()
    assert len(timing_lines()) == 1


def test_resources_are_closed_in_order_cursor_connection_engine(client, resource, tracker):
    get(client, resource)

    assert tracker.last("result") < tracker.last("connection") < tracker.last("engine")
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_failure_after_the_first_block_aborts_without_a_marker(streaming_client, resource, timing_lines, error_records,
                                                               tracker):
    real_fetch = connectors.RowStream.fetch
    calls = []  # the first block was read when the response was created, so these are the 2nd block on

    def fetch(self, size, first=False):
        calls.append(first)
        if len(calls) == 3:  # the 4th block: three were delivered
            raise RuntimeError("source went away")
        return real_fetch(self, size, first)

    response = get(streaming_client, resource)
    received = []
    with patch.object(connectors.RowStream, "fetch", fetch):
        with pytest.raises(RuntimeError, match="source went away"):
            for chunk in response.streaming_content:
                received.append(chunk)
    response.close()

    body = b"".join(received)
    assert body.startswith(b"id,note,price\r\n") and body.count(b"\r\n") == 1 + BLOCK_ROWS * 3
    assert b"error" not in body.lower() and b"RuntimeError" not in body  # nothing but the rows
    (record,) = error_records()
    assert "resource_id=%s" % resource.id in record.getMessage() and "rows_sent=9" in record.getMessage()
    (line,) = timing_lines()
    assert " result=error " in line and " rows=9 " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_source_failing_after_the_first_block_is_not_mapped_to_an_http_error(streaming_client, resource):
    """Once the headers are sent the error cannot become a 5xx: it reaches the server, which aborts the response."""
    response = get(streaming_client, resource)
    assert response.status_code == 200

    def failing_fetchmany(self, size):
        raise sqlalchemy.exc.OperationalError("SELECT", {}, Exception("connection lost"))

    with patch.object(connectors._RowCursor, "fetchmany", failing_fetchmany):
        with pytest.raises(sqlalchemy.exc.OperationalError):
            b"".join(response.streaming_content)


# --- Failures before the first block: the same HTTP errors as always -----------------------------------------------

def _problem(response):
    body = response.json()
    return response.status_code, body.get("error_code")


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
@pytest.mark.parametrize(
    "case,expected",
    [
        pytest.param("missing_object", (503, "OBJECT_UNAVAILABLE"), id="missing_object"),
        pytest.param("bad_sort", (400, "VALIDATION_ERROR"), id="bad_sort"),
        pytest.param("bad_field", (400, "VALIDATION_ERROR"), id="bad_field"),
        pytest.param("bad_columns", (400, "VALIDATION_ERROR"), id="bad_columns"),
        pytest.param("query_fails", (503, "QUERY_ERROR"), id="query_fails"),
    ],
)
def test_failure_before_the_first_block_returns_the_usual_error(
    streaming_client, settings, resource, tracker, case, expected, streaming
):
    settings.DOWNLOAD_STREAMING = streaming
    params = {}
    if case == "missing_object":
        resource.object_location = "nothing_here"
        resource.save()
    elif case == "bad_sort":
        params["sort"] = "nope"
    elif case == "bad_field":
        params["fields"] = "nope"
    elif case == "bad_columns":
        params["columns"] = "only_one"
    if case == "query_fails":
        failure = sqlalchemy.exc.OperationalError("SELECT", {}, Exception("timeout"))
        with patch.object(connectors, "_build_query", side_effect=failure):
            response = get(streaming_client, resource, **params)
    else:
        response = get(streaming_client, resource, **params)

    assert _problem(response) == expected
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_failure_of_the_first_fetch_returns_the_usual_error(streaming_client, settings, resource, tracker, streaming):
    settings.DOWNLOAD_STREAMING = streaming
    if streaming:
        # The query is accepted and fails when the first rows are read.
        failure = sqlalchemy.exc.OperationalError("SELECT", {}, Exception("DPY-4011"))
        with patch.object(connectors._RowCursor, "fetchmany", side_effect=failure):
            response = get(streaming_client, resource)
    else:
        failure = sqlalchemy.exc.OperationalError("SELECT", {}, Exception("DPY-4011"))
        with patch.object(connectors, "_build_query", side_effect=failure):
            response = get(streaming_client, resource)

    assert _problem(response) == (503, "QUERY_ERROR")
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_failure_of_the_normalization_is_the_same_error(streaming_client, settings, resource, tracker, streaming):
    settings.DOWNLOAD_STREAMING = streaming

    def boom(_value):
        raise RuntimeError("cannot normalize")

    with patch.dict(connectors._NORMALIZERS, {int: boom}):
        with pytest.raises(RuntimeError, match="cannot normalize"):
            get(streaming_client, resource)

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.fixture
def blob_resource(tmp_path):
    """The 8th row holds bytes that are not UTF-8: they cannot be serialized."""
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, raw BLOB)", {})]
    statements += [
        ("INSERT INTO things VALUES (:i, :b)", {"i": i, "b": b"\xff\xfe" if i == 8 else b"ok"}) for i in range(1, 11)
    ]
    return _make_resource(tmp_path, "blobs", statements)


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_failure_of_the_serialization_in_the_first_block_is_the_same_error(
    streaming_client, settings, blob_resource, tracker, streaming
):
    settings.DOWNLOAD_STREAMING = streaming

    with pytest.raises(ValueError):
        get(streaming_client, blob_resource, filters='{"id": 8}')

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_failure_of_the_serialization_after_the_first_block_aborts(streaming_client, blob_resource, timing_lines,
                                                                   error_records, tracker):
    response = get(streaming_client, blob_resource)  # the first block (rows 1-3) is fine
    assert response.status_code == 200
    received = []

    with pytest.raises(ValueError):
        for chunk in response.streaming_content:
            received.append(chunk)

    assert b"".join(received).count(b"\r\n") == 1 + 6  # header and the two blocks before the 8th row
    assert len(error_records()) == 1
    assert " result=error " in timing_lines()[0]
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_response_never_iterated_nor_closed_is_closed_by_the_finalizer(streaming_client, resource, tracker):
    response = get(streaming_client, resource)
    assert tracker.open_connections() == 1

    response._resource_closers.clear()  # nothing will close it: the owner's finalizer is the only safety net
    streaming_client.cookies.clear()
    del response
    gc.collect()

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


# --- Skipping rows while reading (MSSQL without an orderable column) ----------------------------------------------

class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchmany(self, size):
        taken, self._rows = self._rows[:size], self._rows[size:]
        return taken

    def close(self):
        pass


class _FakeConnection:
    def close(self):
        pass


@pytest.mark.parametrize(
    "skip,size,expected",
    [
        pytest.param(0, 4, [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]], id="no_skip"),
        pytest.param(2, 4, [[2, 3], [4, 5, 6, 7], [8, 9]], id="skip_inside_first_block"),
        pytest.param(4, 4, [[4, 5, 6, 7], [8, 9]], id="skip_a_whole_block"),
        pytest.param(6, 4, [[6, 7], [8, 9]], id="skip_across_blocks"),
        pytest.param(20, 4, [], id="skip_everything"),
    ],
)
def test_offset_without_sortable_column_drops_rows_while_reading(skip, size, expected):
    cursor = connectors._RowCursor(_FakeConnection(), _FakeResult(range(10)), skip, None)

    blocks = []
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        blocks.append(rows)

    assert blocks == expected


# --- JSON: the bytes of JSONRenderer without ``indent``, produced block by block ------------------------------------

def _whole(client, settings, resource, fmt="json", **params):
    """The response as it is built without streaming (the renderer of DRF)."""
    settings.DOWNLOAD_STREAMING = False
    try:
        return get(client, resource, fmt, **params)
    finally:
        settings.DOWNLOAD_STREAMING = True


@pytest.fixture
def special_resource(tmp_path):
    """Values that need care: NaN/inf, Decimal, dates, bytes, U+2028/U+2029, quotes and non ASCII."""
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, txt TEXT, price REAL, d DATE, blob BLOB)", {})]
    texts = ["plain", "a\u2028b\u2029c", "ñandú \u20ac \U0001F600", 'quote " and \\ and \n', None, "", "x" * 50]
    statements += [
        (
            "INSERT INTO things VALUES (:i, :t, :p, :d, :b)",
            {"i": i, "t": t, "p": i / 3, "d": f"2020-01-{i:02d}", "b": b"ok"},
        )
        for i, t in enumerate(texts, 1)
    ]
    return _make_resource(tmp_path, "special", statements)


@pytest.mark.parametrize("rows", [0, 1, 2, 3, 4, 6, 7], ids=lambda n: f"{n}_rows")
def test_json_bytes_are_those_of_the_renderer(client, streaming_client, settings, tmp_path, rows):
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT, price REAL)", {})]
    statements += [("INSERT INTO things VALUES (:i, :n, :p)", {"i": i, "n": f"n\u2028{i}", "p": i * 1.5})
                   for i in range(1, rows + 1)]
    resource = _make_resource(tmp_path, f"json{rows}", statements)

    streamed = get(streaming_client, resource, "json")
    assert isinstance(streamed, download_stream.StreamedDownloadResponse)
    chunks = list(streamed.streaming_content)
    streamed.close()
    whole = _whole(client, settings, resource)

    assert b"".join(chunks) == whole.content
    assert chunks[-1] == b"]" and all(chunks)
    assert len(chunks) == 1 + max(1, math.ceil(rows / BLOCK_ROWS))  # the blocks of rows and the closing bracket
    assert streamed["Content-Type"] == whole["Content-Type"] == "application/json"
    assert streamed["Content-Disposition"] == whole["Content-Disposition"]
    assert "Content-Length" not in streamed.headers


def test_json_special_values_are_the_same_bytes(client, streaming_client, settings, special_resource):
    streamed = get(streaming_client, special_resource, "json")
    body = b"".join(streamed.streaming_content)
    streamed.close()
    assert body == _whole(client, settings, special_resource).content
    assert b"\\u2028" in body and b"\\u2029" in body


@pytest.mark.parametrize("literal", ["1e999", "-1e999"], ids=["inf", "minus_inf"])
def test_json_infinity_is_the_same_bytes_as_the_renderer(client, streaming_client, settings, tmp_path, literal):
    resource = _make_resource(
        tmp_path, "inf", [("CREATE TABLE things (id INTEGER PRIMARY KEY, v REAL)", {}),
                          (f"INSERT INTO things VALUES (1, {literal})", {})]
    )
    streamed = get(streaming_client, resource, "json")
    body = b"".join(streamed.streaming_content)
    streamed.close()
    assert body == _whole(client, settings, resource).content


def test_json_nan_is_the_same_bytes_as_the_renderer(client, streaming_client, settings, tmp_path):
    """NaN and infinity are normalized before the serialization, by code that both paths share."""
    resource = _make_resource(
        tmp_path, "nan", [("CREATE TABLE things (id INTEGER PRIMARY KEY, v REAL)", {})]
        + [("INSERT INTO things VALUES (:i, :v)", {"i": i, "v": float("nan") if i % 2 else 1.5}) for i in range(1, 6)]
    )
    streamed = get(streaming_client, resource, "json")
    body = b"".join(streamed.streaming_content)
    streamed.close()
    assert body == _whole(client, settings, resource).content


@pytest.mark.parametrize(
    "columns",
    ["a,b,c", "x,x,c", "id,id,id"],
    ids=["renamed", "duplicate_names", "same_name_everywhere"],
)
def test_json_columns_are_applied_per_block_like_the_whole(client, streaming_client, settings, resource, columns):
    streamed = get(streaming_client, resource, "json", columns=columns)
    body = b"".join(streamed.streaming_content)
    streamed.close()
    assert body == _whole(client, settings, resource, columns=columns).content


def test_json_wrong_number_of_columns_is_the_same_error(streaming_client, client, settings, resource, tracker):
    streamed = get(streaming_client, resource, "json", columns="a,b")
    assert not isinstance(streamed, download_stream.StreamedDownloadResponse)
    whole = _whole(client, settings, resource, columns="a,b")
    assert streamed.status_code == whole.status_code and streamed.content == whole.content
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize(
    "accept, streamed",
    [
        ("application/json", True),
        ("*/*", True),
        ("text/html", True),
        ("application/json; indent=0", True),
        ("application/json; indent=4", False),
        ("application/json; indent=2, text/html", False),
    ],
    ids=["json", "any", "html_only", "indent_zero", "indent_4", "indent_2_listed"],
)
def test_json_negotiation_streams_only_without_indent(client, streaming_client, settings, resource, accept, streamed):
    response = streaming_client.get(
        DOWNLOAD, {"resource_id": resource.id, "sort": "id"}, HTTP_ACCEPT=accept
    )
    assert isinstance(response, download_stream.StreamedDownloadResponse) is streamed
    body = b"".join(response.streaming_content) if streamed else response.content
    settings.DOWNLOAD_STREAMING = False
    whole = client.get(DOWNLOAD, {"resource_id": resource.id, "sort": "id"}, HTTP_ACCEPT=accept)
    assert body == whole.content and response["Content-Type"] == whole["Content-Type"]
    if not streamed:
        assert b"\n    " in body or b"\n  " in body  # indented by the renderer, as before


@pytest.mark.parametrize("how", ["formato", "format"], ids=["formato", "format"])
def test_json_selected_by_query_parameter_is_streamed(streaming_client, resource, how):
    response = streaming_client.get(DOWNLOAD, {"resource_id": resource.id, how: "json"})
    assert isinstance(response, download_stream.StreamedDownloadResponse)
    assert response["Content-Type"] == "application/json"
    response.close()


def test_json_preview_is_not_streamed(client, resource):
    response = client.get(PREVIEW, {"resource_id": resource.id, "format": "json"})
    assert not isinstance(response, download_stream.StreamedDownloadResponse) and response.status_code == 200


def test_json_geometry_resource_goes_the_old_way(streaming_client, resource):
    collection = {"type": "FeatureCollection", "features": []}
    with (
        patch.object(gaodcore_views, "get_GeoJson_resource", return_value=True),
        patch.object(gaodcore_views, "get_resource_data_feature", return_value=collection),
        patch.object(gaodcore_views, "get_resource_rows") as rows,
    ):
        response = get(streaming_client, resource, "json")
    assert not isinstance(response, download_stream.StreamedDownloadResponse)
    assert response.json() == collection
    rows.assert_not_called()


def test_json_timing_and_size_of_a_complete_download(streaming_client, resource, timing_lines):
    response = streaming_client.get(DOWNLOAD, {"resource_id": resource.id, "format": "json"})
    body = b"".join(response.streaming_content)
    response.close()
    (line,) = timing_lines()
    assert f" rows={ROWS} " in line and f" bytes={len(body)} " in line and " result=complete " in line
    assert ResourceSizeConfig.objects.get(resource_id=resource).size == len(body)


# --- JSON: errors ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_json_failure_of_the_first_fetch_is_the_same_error(streaming_client, settings, resource, tracker, streaming):
    settings.DOWNLOAD_STREAMING = streaming
    failure = sqlalchemy.exc.OperationalError("SELECT", {}, Exception("DPY-4011"))
    target = (connectors._RowCursor, "fetchmany") if streaming else (connectors, "_build_query")
    with patch.object(*target, side_effect=failure):
        response = get(streaming_client, resource, "json")

    assert _problem(response) == (503, "QUERY_ERROR")
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_json_failure_of_the_normalization_is_the_same_error(streaming_client, settings, resource, tracker, streaming):
    settings.DOWNLOAD_STREAMING = streaming

    def boom(_value):
        raise RuntimeError("cannot normalize")

    with patch.dict(connectors._NORMALIZERS, {int: boom}):
        with pytest.raises(RuntimeError, match="cannot normalize"):
            get(streaming_client, resource, "json")

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_json_failure_of_the_serialization_in_the_first_block_is_the_same_error(
    streaming_client, settings, blob_resource, tracker, streaming
):
    settings.DOWNLOAD_STREAMING = streaming

    with pytest.raises(ValueError):
        get(streaming_client, blob_resource, "json", filters='{"id": 8}')

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_json_failure_in_a_later_block_has_no_closing_bracket_nor_marker(
    streaming_client, resource, timing_lines, error_records, tracker
):
    real_fetch = connectors.RowStream.fetch
    calls = []

    def fetch(self, size, first=False):
        calls.append(first)
        if len(calls) == 3:
            raise RuntimeError("source went away")
        return real_fetch(self, size, first)

    response = get(streaming_client, resource, "json")
    received = []
    with patch.object(connectors.RowStream, "fetch", fetch):
        with pytest.raises(RuntimeError, match="source went away"):
            for chunk in response.streaming_content:
                received.append(chunk)
    response.close()

    body = b"".join(received)
    assert body.startswith(b"[{") and not body.endswith(b"]") and b"]" not in body
    assert body.count(b'"id"') == 9
    assert b"error" not in body.lower() and b"RuntimeError" not in body
    assert len(error_records()) == 1
    (line,) = timing_lines()
    assert " result=error " in line and " rows=9 " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_json_failure_of_the_serialization_after_the_first_block_aborts(
    streaming_client, blob_resource, timing_lines, tracker
):
    response = get(streaming_client, blob_resource, "json")
    received = []
    with pytest.raises(ValueError):
        for chunk in response.streaming_content:
            received.append(chunk)
    response.close()

    assert b"]" not in b"".join(received) and b"".join(received).count(b'"id"') == 6
    assert " result=error " in timing_lines()[0]
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_json_cut_half_way_is_early_close_without_bracket_or_size(
    streaming_client, resource, timing_lines, tracker
):
    response = get(streaming_client, resource, "json")
    chunks = iter(response.streaming_content)
    received = [next(chunks), next(chunks)]
    response.close()

    assert b"]" not in b"".join(received)
    (line,) = timing_lines()
    assert " result=early_close " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


# --- XML: the bytes of GaodXMLRenderer, produced block by block ------------------------------------------------------

def _rows_of(resource_rows, columns=None, fields=None, note=None):
    """The normalized rows a download of the test table has, as the renderers receive them."""
    rows = [{"id": i, "note": note or f"note, {i}", "price": connectors._normalize_number(i * 1.5)}
            for i in range(1, resource_rows + 1)]
    if fields:
        rows = [{name: row[name] for name in fields} for row in rows]
    data = get_return_list(rows, format_is_xlsx=False)
    return modify_header(data, columns or [])


def _xml_table(tmp_path, name, rows, note=None):
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT, price REAL)", {})]
    statements += [
        ("INSERT INTO things VALUES (:i, :n, :p)", {"i": i, "n": note or f"note, {i}", "p": i * 1.5})
        for i in range(1, rows + 1)
    ]
    return _make_resource(tmp_path, name, statements)


def _same(left: bytes, right: bytes) -> bool:
    """Byte equality that reports where two long bodies differ (pytest's diff of two big bytes takes minutes)."""
    if left == right:
        return True
    at = next((i for i, (a, b) in enumerate(zip(left, right)) if a != b), min(len(left), len(right)))
    print(f"bodies differ at byte {at}: {left[at - 40:at + 40]!r} != {right[at - 40:at + 40]!r}")
    return False


def _read(response):
    chunks = list(response.streaming_content)
    response.close()
    return chunks


@pytest.mark.parametrize("block_rows", [None, 7], ids=["default_blocks", "small_blocks"])
@pytest.mark.parametrize("rows", [0, 1, 999, 1000, 1001, 2500])
def test_xml_bytes_are_those_of_the_renderers(client, streaming_client, settings, tmp_path, rows, block_rows):
    if block_rows is None:
        settings.DOWNLOAD_BLOCK_ROWS = 1000
    else:
        settings.DOWNLOAD_BLOCK_ROWS = block_rows
    resource = _xml_table(tmp_path, f"xml{rows}", rows)

    streamed = get(streaming_client, resource, "xml")
    assert isinstance(streamed, download_stream.StreamedDownloadResponse)
    body = b"".join(_read(streamed))
    whole = _whole(client, settings, resource, "xml")

    data = _rows_of(rows)
    expected = xml_output.render_document(data).encode()
    assert _same(body, expected) and _same(whole.content, expected)
    assert _same(body, OldXMLRenderer().render(data).encode())
    assert body.startswith(b'<?xml version="1.0" encoding="utf-8"?>\n<root>') and body.endswith(b"</root>")
    assert streamed["Content-Type"] == whole["Content-Type"] == "application/xml; charset=utf-8"
    assert streamed["Content-Disposition"] == whole["Content-Disposition"]
    assert "Content-Length" not in streamed.headers


def test_xml_without_rows_is_the_empty_document(streaming_client, tmp_path):
    resource = _xml_table(tmp_path, "xml_none", 0)
    response = get(streaming_client, resource, "xml")
    chunks = _read(response)
    assert b"".join(chunks) == xml_output.render_document([]).encode() == (
        b'<?xml version="1.0" encoding="utf-8"?>\n<root></root>'
    )
    assert chunks == [b'<?xml version="1.0" encoding="utf-8"?>\n<root>', b"</root>"]


def test_xml_is_streamed_in_blocks_with_the_root_closed_last(streaming_client, resource):
    response = get(streaming_client, resource, "xml")
    chunks = _read(response)
    assert chunks[0].startswith(b'<?xml version="1.0" encoding="utf-8"?>\n<root><list-item>')
    assert chunks[-1] == b"</root>" and all(chunks)
    assert len(chunks) == math.ceil(ROWS / BLOCK_ROWS) + 1
    assert [c.count(b"<list-item>") for c in chunks[:-1]] == [3, 3, 3, 1]


@pytest.mark.parametrize(
    "params,columns,fields",
    [
        pytest.param({"columns": "a,b,c"}, ["a", "b", "c"], None, id="columns"),
        pytest.param({"fields": "note,id"}, None, ["note", "id"], id="fields"),
        pytest.param({"fields": "note,id", "columns": "n,i"}, ["n", "i"], ["note", "id"], id="fields_columns"),
    ],
)
def test_xml_columns_and_fields_are_those_of_the_renderers(
    client, streaming_client, settings, resource, params, columns, fields
):
    streamed = get(streaming_client, resource, "xml", **params)
    body = b"".join(_read(streamed))
    data = _rows_of(ROWS, columns, fields)
    assert body == OldXMLRenderer().render(data).encode() == _whole(client, settings, resource, "xml", **params).content


def test_xml_names_that_are_not_xml_names_and_carriage_returns_are_the_renderer_bytes(
    client, streaming_client, settings, tmp_path
):
    resource = _xml_table(tmp_path, "xml_odd", 7, note="a\rb <&> c")
    columns = "a b,1x,TOTAL$"
    streamed = get(streaming_client, resource, "xml", columns=columns)
    body = b"".join(_read(streamed))

    assert body == _whole(client, settings, resource, "xml", columns=columns).content
    assert body == xml_output.render_document(_rows_of(7, columns.split(","), note="a\rb <&> c")).encode()
    assert b'<field name="a b">1</field>' in body and b'<field name="1x">' in body
    assert b"a&#13;b &lt;&amp;&gt; c" in body and b"\r" not in body


def test_xml_is_streamed_only_for_download_with_the_gaod_renderer(streaming_client, settings, resource):
    response = get(streaming_client, resource, "xml")
    assert isinstance(response, download_stream.StreamedDownloadResponse)
    response.close()

    class Subclass(GaodXMLRenderer):
        pass

    renderers = tuple(Subclass if r is GaodXMLRenderer else r for r in gaodcore_views.PUBLIC_DATA_RENDERER_CLASSES)
    with patch.object(gaodcore_views.DownloadView, "renderer_classes", renderers):
        other = get(streaming_client, resource, "xml")
    assert not isinstance(other, download_stream.StreamedDownloadResponse)
    assert other.status_code == 200

    preview = streaming_client.get(PREVIEW, {"resource_id": resource.id, "format": "xml"})
    assert not isinstance(preview, download_stream.StreamedDownloadResponse) and preview.status_code == 200

    settings.DOWNLOAD_STREAMING = False
    assert not isinstance(get(streaming_client, resource, "xml"), download_stream.StreamedDownloadResponse)


def test_xml_preview_keeps_its_cap(client, settings, tmp_path):
    resource = _xml_table(tmp_path, "xml_preview", 1005)
    response = client.get(PREVIEW, {"resource_id": resource.id, "format": "xml"})
    assert response.content.count(b"<list-item>") == 1000


@pytest.mark.parametrize(
    "request_kwargs",
    [
        pytest.param({"data": {"format": "xml"}}, id="format"),
        pytest.param({"data": {"formato": "xml"}}, id="formato"),
        pytest.param({"data": {}, "HTTP_ACCEPT": "application/xml"}, id="accept"),
    ],
)
def test_xml_negotiation_streams(streaming_client, resource, request_kwargs):
    data = {"resource_id": resource.id, "sort": "id", **request_kwargs.pop("data")}
    response = streaming_client.get(DOWNLOAD, data, **request_kwargs)
    assert isinstance(response, download_stream.StreamedDownloadResponse)
    assert response["Content-Type"] == "application/xml; charset=utf-8"
    assert b"".join(_read(response)).endswith(b"</root>")


def test_xml_head_is_unchanged_and_keeps_the_content_type_discrepancy_with_indent(client, resource):
    """HEAD uses the accepted media type (with its parameters); GET sends the renderer's one, streamed or not."""
    accept = {"HTTP_ACCEPT": "application/xml; indent=4"}
    params = {"resource_id": resource.id}
    with patch.object(gaodcore_views, "get_resource_rows") as rows, patch.object(
        gaodcore_views, "update_resource_size"
    ) as size:
        head = client.head(DOWNLOAD, params, **accept)
        rows.assert_not_called()
        size.assert_not_called()
    get_ = client.get(DOWNLOAD, params, **accept)
    assert head.status_code == get_.status_code == 200
    assert head["Content-Type"] == "application/xml; indent=4; charset=utf-8"
    assert get_["Content-Type"] == "application/xml; charset=utf-8"


def test_xml_timing_and_size_of_a_complete_download(streaming_client, resource, timing_lines):
    response = streaming_client.get(DOWNLOAD, {"resource_id": resource.id, "format": "xml"})
    body = b"".join(response.streaming_content)
    response.close()
    (line,) = timing_lines()
    assert f" rows={ROWS} " in line and f" bytes={len(body)} " in line and " result=complete " in line
    assert " format=xml " in line
    stored = ResourceSizeConfig.objects.get(resource_id=resource)
    assert (stored.registries, stored.size) == (ROWS, len(body))


@pytest.mark.parametrize(
    "params",
    [{"limit": "4"}, {"fields": "id"}, {"filters": '{"id": 7}'}, {"offset": "1"}],
    ids=["limit", "fields", "filter", "offset"],
)
def test_xml_partial_download_does_not_store_the_size(client, resource, params):
    get(client, resource, "xml", **params)
    assert not ResourceSizeConfig.objects.exists()


def test_xml_cut_half_way_is_early_close_without_root_or_size(streaming_client, resource, timing_lines, tracker):
    response = get(streaming_client, resource, "xml")
    chunks = iter(response.streaming_content)
    received = [next(chunks), next(chunks)]
    response.close()

    assert b"</root>" not in b"".join(received)
    (line,) = timing_lines()
    assert " result=early_close " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_xml_cut_after_the_closing_tag_is_early_close_without_size(streaming_client, resource, timing_lines, tracker):
    """The ``</root>`` block is handed over, but the download only completes when the next block is requested."""
    response = get(streaming_client, resource, "xml")
    chunks = iter(response.streaming_content)
    received = []
    while not received or received[-1] != b"</root>":
        received.append(next(chunks))
    response.close()  # the client closed before the server asked for the next block

    assert b"".join(received).endswith(b"</root>")
    (line,) = timing_lines()
    assert " result=early_close " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_xml_failure_in_a_later_block_has_no_closing_root(
    streaming_client, resource, timing_lines, error_records, tracker
):
    real_fetch = connectors.RowStream.fetch
    calls = []

    def fetch(self, size, first=False):
        calls.append(first)
        if len(calls) == 3:
            raise RuntimeError("source went away")
        return real_fetch(self, size, first)

    response = get(streaming_client, resource, "xml")
    received = []
    with patch.object(connectors.RowStream, "fetch", fetch):
        with pytest.raises(RuntimeError, match="source went away"):
            for chunk in response.streaming_content:
                received.append(chunk)
    response.close()

    body = b"".join(received)
    assert body.startswith(b"<?xml") and b"</root>" not in body and body.count(b"<list-item>") == 9
    assert len(error_records()) == 1
    (line,) = timing_lines()
    assert " result=error " in line and " rows=9 " in line
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.fixture
def control_char_resource(tmp_path):
    """Row 5 (second block) holds a character XML 1.0 cannot carry."""
    statements = [("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)", {})]
    statements += [
        ("INSERT INTO things VALUES (:i, :n)", {"i": i, "n": "bad\ufffe" if i == 5 else "ok"}) for i in range(1, 11)
    ]
    return _make_resource(tmp_path, "ctrl", statements)


def test_xml_unrepresentable_character_in_a_later_block_aborts(
    streaming_client, control_char_resource, timing_lines, error_records, tracker
):
    response = get(streaming_client, control_char_resource, "xml")
    assert response.status_code == 200
    received = []
    with pytest.raises(xml_output.XmlUnrepresentableError):
        for chunk in response.streaming_content:
            received.append(chunk)
    response.close()

    body = b"".join(received)
    assert body.count(b"<list-item>") == 3 and b"</root>" not in body
    assert " result=error " in timing_lines()[0]
    assert len(error_records()) == 1
    assert not ResourceSizeConfig.objects.exists()
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_xml_unrepresentable_character_in_the_first_block_fails_as_without_streaming(
    streaming_client, settings, control_char_resource, tracker, streaming
):
    """Not mapped to an HTTP error in either path: the exception reaches the test client (a 500 on a server)."""
    settings.DOWNLOAD_STREAMING = streaming
    with pytest.raises(xml_output.XmlUnrepresentableError):
        get(streaming_client, control_char_resource, "xml", filters='{"id": 5}')
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
@pytest.mark.parametrize(
    "case,expected",
    [
        pytest.param("missing_object", (503, "OBJECT_UNAVAILABLE"), id="missing_object"),
        pytest.param("bad_sort", (400, "VALIDATION_ERROR"), id="bad_sort"),
        pytest.param("bad_field", (400, "VALIDATION_ERROR"), id="bad_field"),
        pytest.param("bad_columns", (400, "VALIDATION_ERROR"), id="bad_columns"),
        pytest.param("query_fails", (503, "QUERY_ERROR"), id="query_fails"),
        pytest.param("first_fetch_fails", (503, "QUERY_ERROR"), id="first_fetch_fails"),
    ],
)
def test_xml_failure_before_the_first_block_returns_the_usual_error(
    streaming_client, settings, resource, tracker, case, expected, streaming
):
    settings.DOWNLOAD_STREAMING = streaming
    params = {}
    failure = sqlalchemy.exc.OperationalError("SELECT", {}, Exception("timeout"))
    if case == "missing_object":
        resource.object_location = "nothing_here"
        resource.save()
    elif case == "bad_sort":
        params["sort"] = "nope"
    elif case == "bad_field":
        params["fields"] = "nope"
    elif case == "bad_columns":
        params["columns"] = "only_one"
    if case == "query_fails":
        with patch.object(connectors, "_build_query", side_effect=failure):
            response = get(streaming_client, resource, "xml", **params)
    elif case == "first_fetch_fails":
        target = (connectors._RowCursor, "fetchmany") if streaming else (connectors, "_build_query")
        with patch.object(*target, side_effect=failure):
            response = get(streaming_client, resource, "xml", **params)
    else:
        response = get(streaming_client, resource, "xml", **params)

    assert _problem(response) == expected
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


@pytest.mark.parametrize("streaming", [True, False], ids=["streamed", "built_whole"])
def test_xml_failure_of_the_normalization_is_the_same_error(streaming_client, settings, resource, tracker, streaming):
    settings.DOWNLOAD_STREAMING = streaming

    def boom(_value):
        raise RuntimeError("cannot normalize")

    with patch.dict(connectors._NORMALIZERS, {int: boom}):
        with pytest.raises(RuntimeError, match="cannot normalize"):
            get(streaming_client, resource, "xml")
    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0


def test_xml_response_never_iterated_and_closed_twice_releases_once(streaming_client, resource, tracker):
    response = get(streaming_client, resource, "xml")
    assert tracker.open_connections() == 1

    response.close()
    response.close()

    assert tracker.open_connections() == 0 and tracker.undisposed_engines() == 0
    assert tracker.events.count("engine") == 1


def test_xml_http_source_is_streamed(streaming_client, resource):
    """HTTP sources arrive as a ``_ListCursor``: same path, same bytes."""
    rows = [{"id": i, "note": f"n{i}"} for i in range(1, 6)]
    stream = connectors.RowStream(connectors._ListCursor([(r["id"], r["note"]) for r in rows]), ["id", "note"], None)
    with patch.object(gaodcore_views, "get_resource_rows", return_value=stream):
        response = get(streaming_client, resource, "xml")
        assert isinstance(response, download_stream.StreamedDownloadResponse)
        body = b"".join(_read(response))
    expected = modify_header(get_return_list(rows, format_is_xlsx=False), [])
    assert body == xml_output.render_document(expected).encode()
