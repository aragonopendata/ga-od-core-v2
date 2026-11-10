"""A failed connection/object produces exactly ONE warning with resource id, reason and masked URI."""

import contextlib
import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from connectors import DriverConnectionError, NoObjectError, _get_engine_from_api
from gaodcore import views as gaodcore_views
from gaodcore.tests.slow_http_server import raw_http_server, respond_after_delay, respond_status_then_stall

DOWNLOAD = "/GA_OD_Core/download"
PREVIEW = "/GA_OD_Core/preview"


def _config(uri, location=None, schema=None):
    return SimpleNamespace(
        id=2190,
        name="things",
        object_location=location,
        object_location_schema=schema,
        connector_config=SimpleNamespace(uri=uri),
    )


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def caplog():
    """Capture the app loggers directly: the project logging config stops them propagating to the root logger."""
    handler = _Records()
    loggers = [logging.getLogger(name) for name in ("gaodcore.views", "connectors")]
    for each in loggers:
        each.addHandler(handler)
    yield SimpleNamespace(records=handler.records, at_level=lambda level: contextlib.nullcontext())
    for each in loggers:
        each.removeHandler(handler)


def _unavailable_records(caplog):
    return [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.parametrize(
    "status_line, reason",
    [(b"HTTP/1.1 503 Service Unavailable\r\n", "HTTP status 503"), (b"HTTP/1.1 404 Not Found\r\n", "HTTP status 404")],
)
def test_http_error_status_carries_reason_and_does_not_log(caplog, status_line, reason):
    handler = respond_after_delay(status_line, b"Content-Length: 2\r\n", b"{}", delay_seconds=0)
    with raw_http_server(handler) as url, caplog.at_level(logging.DEBUG):
        with pytest.raises(DriverConnectionError) as info:
            _get_engine_from_api(url, timeout=2)
    assert str(info.value) == "The url could not be reached."
    assert info.value.reason == reason
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_timeout_carries_reason_and_does_not_log(caplog):
    handler = respond_status_then_stall(
        b"HTTP/1.1 200 OK\r\n", b"Content-Length: 100000\r\nContent-Type: application/json\r\n", delay_seconds=3
    )
    with raw_http_server(handler) as url, caplog.at_level(logging.DEBUG):
        with pytest.raises(DriverConnectionError) as info:
            _get_engine_from_api(url, timeout=1)
    assert info.value.reason == "timed out after 1s while reading response"
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint", [DOWNLOAD, PREVIEW])
@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
def test_failed_http_source_logs_one_warning_and_keeps_the_503_body(client, caplog, endpoint, fmt):
    handler = respond_after_delay(
        b"HTTP/1.1 503 Service Unavailable\r\n", b"Content-Length: 2\r\n", b"{}", delay_seconds=0
    )
    with raw_http_server(handler) as url:
        uri = f"{url}/data.json"
        with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)):
            with caplog.at_level(logging.DEBUG):
                response = client.get(endpoint, {"resource_id": 2190, "formato": fmt})

    assert response.status_code == 503
    assert response.json()["detail"] == "Connection is not available."

    records = _unavailable_records(caplog)
    assert len(records) == 1, [r.getMessage() for r in records]
    message = records[0].getMessage()
    assert "resource_id=2190" in message
    assert "reason=HTTP status 503" in message
    assert f"uri={uri}" in message
    assert records[0].levelno == logging.WARNING


@pytest.mark.django_db
def test_failed_http_source_timeout_logs_one_warning(client, caplog):
    handler = respond_status_then_stall(
        b"HTTP/1.1 200 OK\r\n", b"Content-Length: 100000\r\nContent-Type: application/json\r\n", delay_seconds=3
    )
    with raw_http_server(handler) as url:
        uri = f"{url}/data.json"
        with patch.object(gaodcore_views, "_get_resource", return_value=_config(uri)):
            with patch("connectors.DEFAULT_HTTP_TIMEOUT_SECONDS", 1):
                with caplog.at_level(logging.DEBUG):
                    response = client.get(DOWNLOAD, {"resource_id": 2190, "formato": "json"})
    assert response.status_code == 503
    assert response.json()["detail"] == "Connection is not available."
    records = _unavailable_records(caplog)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert "reason=timed out after 1s while reading response" in records[0].getMessage()
    assert f"uri={uri}" in records[0].getMessage()


@pytest.mark.parametrize("error_class", [DriverConnectionError, NoObjectError])
@pytest.mark.parametrize("context", ["", " during Excel validation"])
def test_log_unavailable_masks_credentials_and_adds_location(caplog, error_class, context):
    error = error_class("generic message")
    error.__cause__ = RuntimeError("ORA-12541: TNS:no listener")
    config = _config("oracle://scott:tiger@db.example:1521/ORCL", location="things", schema="HR")
    with caplog.at_level(logging.DEBUG):
        gaodcore_views.DownloadView._log_unavailable("Thing is not available", context, error, 7, config)
    (record,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    message = record.getMessage()
    assert "tiger" not in message
    assert message.startswith("Thing is not available" + context + ": resource_id=7")
    assert "reason=RuntimeError: ORA-12541: TNS:no listener" in message
    assert "uri=oracle://scott:" in message
    assert message.endswith("location=things schema=HR")
