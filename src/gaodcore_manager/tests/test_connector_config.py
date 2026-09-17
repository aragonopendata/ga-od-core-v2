import os
from urllib.parse import urlparse

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.fixtures import FixtureRequest

from django.test import Client

from conftest import compare_files, ConnectorData, validate_error
from connectors import DriverConnectionError, MimeTypeError
from gaodcore_manager import validators


@pytest.mark.django_db
def test_connector(
    auth_client: Client, full_example: ConnectorData, accept_download: str
):
    # TODO: Check connector not download
    download_response = auth_client.get(
        "/GA_OD_Core/download",
        {
            "resource_id": full_example.resources.table.id,
        },
        HTTP_ACCEPT=accept_download,
    )

    compare_files(
        os.path.join(os.path.dirname(__file__), "..", "..", "gaodcore", "tests"),
        f"download_{full_example.scheme}",
        accept_download,
        download_response.content,
    )


@pytest.mark.django_db
def test_connector_config_path_error(
    auth_client: Client, request: FixtureRequest, connector_uri: str, accept_error: str
):
    connector_uri = connector_uri.rsplit("/", 1)[0] + "/path-error"
    response = auth_client.post(
        "/GA_OD_Core_admin/manager/connector-config/",
        {"name": request.node.name, "uri": connector_uri},
        HTTP_ACCEPT=accept_error,
    )
    assert response.status_code == 503
    validate_error(
        response,
        "Connection is not available.",
        accept_error,
        error_code="CONNECTION_UNAVAILABLE",
    )


@pytest.mark.django_db
def test_connector_credentials_error(
    auth_client: Client,
    request: FixtureRequest,
    connector_uri: str,
    caplog: LogCaptureFixture,
    accept_error: str,
):
    parsed = urlparse(connector_uri)
    if parsed.scheme in ["http", "https"]:
        # TODO: currently pytest-httpserver is not easy to auth
        return
    parsed = parsed._replace(
        netloc="{}:{}@{}:{}".format(
            "invalid_username",
            "invalid_password",
            parsed.hostname,
            parsed.netloc.split(":")[-1],
        )
    )
    response = auth_client.post(
        "/GA_OD_Core_admin/manager/connector-config/",
        {"name": request.node.name, "uri": parsed.geturl()},
        HTTP_ACCEPT=accept_error,
    )
    assert response.status_code == 503
    if parsed.scheme == "postgresql":
        assert (
            'FATAL:  password authentication failed for user "invalid_username"'
            in caplog.text
        )
    elif parsed.scheme == "mysql":
        assert "Access denied for user 'invalid_username'" in caplog.text
    else:
        raise NotImplementedError
    validate_error(
        response,
        "Connection is not available.",
        accept_error,
        error_code="CONNECTION_UNAVAILABLE",
    )


@pytest.mark.django_db
def test_connector_config_schema_error(
    auth_client, request: FixtureRequest, accept_error: str
):
    uri = "test://postgres:postgres@localhost:1/guillotina"
    response = auth_client.post(
        "/GA_OD_Core_admin/manager/connector-config/",
        {"name": request.node.name, "uri": uri},
        HTTP_ACCEPT=accept_error,
    )
    assert response.status_code == 400
    validate_error(
        response,
        "Schema of the URI is not available.",
        accept_error,
        "uri",
        error_code="VALIDATION_ERROR",
        field_error_code="SCHEMA_NOT_IMPLEMENTED",
    )


@pytest.mark.django_db
def test_connector_config_mime_type_error(
    mocker, auth_client, request: FixtureRequest, accept_error: str
):
    """A JSON API whose content-type is not allowed reports MIME_TYPE_NOT_ALLOWED.

    The rejection happens inside ``validate_uri`` after the remote response has been
    read, so the check itself is faked here: what is pinned is the code that
    ``uri_validator`` attaches to the "uri" field.
    """
    mocker.patch.object(
        validators, "validate_uri", side_effect=MimeTypeError("text/html")
    )
    response = auth_client.post(
        "/GA_OD_Core_admin/manager/connector-config/",
        {"name": request.node.name, "uri": "https://example.invalid/data"},
        HTTP_ACCEPT=accept_error,
    )

    assert response.status_code == 400
    validate_error(
        response,
        "Mimetype of content-type is not allowed. Only allowed: JSON mimetypes.",
        accept_error,
        "uri",
        error_code="VALIDATION_ERROR",
        field_error_code="MIME_TYPE_NOT_ALLOWED",
    )


@pytest.mark.django_db
def test_connector_config_connection_unavailable_code(
    mocker, auth_client, request: FixtureRequest, accept_error: str
):
    """A connector that cannot be reached is a 503 carrying its code at the top level.

    ``ServiceUnavailable`` is not a ``ValidationError``, so the problem document has no
    "errors" member and CONNECTION_UNAVAILABLE is the top level ``error_code``.
    """
    mocker.patch.object(
        validators, "validate_uri", side_effect=DriverConnectionError("nope")
    )
    response = auth_client.post(
        "/GA_OD_Core_admin/manager/connector-config/",
        {"name": request.node.name, "uri": "postgresql://user:pass@localhost:1/db"},
        HTTP_ACCEPT=accept_error,
    )

    assert response.status_code == 503
    validate_error(
        response,
        "Connection is not available.",
        accept_error,
        error_code="CONNECTION_UNAVAILABLE",
    )
