import os
from multiprocessing.connection import Client
from urllib.parse import urlparse

import pytest

import connectors
from conftest import (
    create_connector_ga_od_core,
    ConnectorData,
    compare_files,
    validate_error,
)
from connectors import (
    DriverConnectionError,
    MimeTypeError,
    NoObjectError,
    NotImplementedSchemaError,
    TooManyRowsError,
)
from gaodcore_manager import validators
from gaodcore_manager.models import ConnectorConfig


@pytest.mark.django_db
def test_resource(
    auth_client: Client, full_example: ConnectorData, accept_download: str
):
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
def test_resource_view(auth_client, full_example: ConnectorData, request):
    if full_example.scheme in ["http", "https"]:
        # Not applicable
        return
    resource_response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "connector_config": full_example.id,
            "object_location": full_example.resources.view.object_location,
        },
    )

    assert resource_response.status_code == 201


@pytest.mark.django_db
def test_resource_config_error(auth_client, connector_uri, request, accept_error):
    connector_data = create_connector_ga_od_core(
        auth_client, request.node.name, connector_uri
    )
    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector_data.id,
            "object_location": "fail",
        },
        HTTP_ACCEPT=accept_error,
    )
    assert response.status_code == 400
    scheme = urlparse(connector_uri).scheme
    if scheme in ["postgresql", "mysql"]:
        validate_error(
            response,
            "Resource is not available. Table, view, function, etc... not exists.",
            accept_error,
            "non_field_errors",
        )
    elif scheme in ["http", "https"]:
        validate_error(
            response,
            "Object location or object location schema is not allowed in http and https resources",
            accept_error,
            "non_field_errors",
        )
    else:
        raise NotImplementedError


# TODO: Fix this test
@pytest.mark.xfail(reason="This test is failing. Fix it.")
@pytest.mark.django_db
def test_resource_too_many_rows_error(
    mocker, auth_client: Client, full_example, accept_error
):
    auth_client.delete(
        f"/admin/GA_OD_Core_admin/manager/resource-config/{full_example.resources.table.id}/"
    )
    mocker.patch.object(connectors, "_RESOURCE_MAX_ROWS", 1)
    conf = {
        "name": full_example.resources.table.name,
        "enabled": True,
        "connector_config": full_example.resources.table.connector_config,
    }

    if full_example.resources.table.object_location:
        conf["object_location"] = full_example.resources.table.object_location

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/", conf, HTTP_ACCEPT=accept_error
    )

    assert response.status_code == 400
    validate_error(
        response,
        "This resource have too many rows. For security reason this is not allowed.",
        accept_error,
        "non_field_errors",
    )


@pytest.mark.django_db
def test_resource_with_invalid_connector_error(
    auth_client: Client, accept_error, request
):
    connector = ConnectorConfig(name="test", uri="postgresql://test/test", enabled=True)
    connector.save()

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector.id,
            "object_location": "fail",
        },
        HTTP_ACCEPT=accept_error,
    )

    assert response.status_code == 503
    validate_error(
        response,
        "Connection is not available.",
        accept_error,
        error_code="CONNECTION_UNAVAILABLE",
    )


def _saved_connector(name: str, uri: str) -> ConnectorConfig:
    """Store a connector without going through its validation, so no server is needed."""
    connector = ConnectorConfig(name=name, uri=uri, enabled=True)
    connector.save()
    return connector


@pytest.mark.django_db
def test_resource_object_location_required_error(auth_client, accept_error, request):
    """A PostgreSQL resource without "object_location" is rejected with REQUIRED."""
    connector = _saved_connector(request.node.name, "postgresql://test/test")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {"name": request.node.name, "enabled": True, "connector_config": connector.id},
        HTTP_ACCEPT=accept_error,
    )

    assert response.status_code == 400
    validate_error(
        response,
        "Object location is not filled.",
        accept_error,
        "non_field_errors",
        error_code="VALIDATION_ERROR",
        field_error_code="REQUIRED",
    )


@pytest.mark.django_db
def test_resource_object_location_not_allowed_error(auth_client, accept_error, request):
    """"object_location" makes no sense for an API connector: INVALID_FIELD."""
    connector = _saved_connector(request.node.name, "https://example.invalid/data.json")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector.id,
            "object_location": "fail",
        },
        HTTP_ACCEPT=accept_error,
    )

    assert response.status_code == 400
    validate_error(
        response,
        "Object location or object location schema is not allowed in http and https resources",
        accept_error,
        "non_field_errors",
        error_code="VALIDATION_ERROR",
        field_error_code="INVALID_FIELD",
    )


# Every failure ``validate_resource`` can report, with the code ``resource_validator``
# must translate it into. Reaching them for real needs a live database, so the probe is
# faked and what is pinned is the translation, which is the public contract.
RESOURCE_VALIDATION_ERRORS = [
    (
        NotImplementedSchemaError("nope"),
        "Schema of the URI is not available.",
        "SCHEMA_NOT_IMPLEMENTED",
    ),
    (
        MimeTypeError("text/html"),
        "Mimetype of content-type is not allowed. Only allowed: JSON mimetypes.",
        "MIME_TYPE_NOT_ALLOWED",
    ),
    (
        TooManyRowsError("too many"),
        "This resource have too many rows. For security reason this is not allowed.",
        "TOO_MANY_ROWS",
    ),
    (
        NoObjectError("no object"),
        "Resource is not available. Table, view, function, etc... not exists.",
        "RESOURCE_UNAVAILABLE",
    ),
]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "error,message,expected_code",
    RESOURCE_VALIDATION_ERRORS,
    ids=[item[2] for item in RESOURCE_VALIDATION_ERRORS],
)
def test_resource_config_error_codes(
    mocker, auth_client, request, error, message, expected_code
):
    mocker.patch.object(validators, "validate_resource", side_effect=error)
    connector = _saved_connector(request.node.name, "postgresql://test/test")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector.id,
            "object_location": "fail",
        },
        HTTP_ACCEPT="application/json",
    )

    assert response.status_code == 400
    validate_error(
        response,
        message,
        "application/json",
        "non_field_errors",
        error_code="VALIDATION_ERROR",
        field_error_code=expected_code,
    )


@pytest.mark.django_db
def test_resource_config_connection_unavailable_code(mocker, auth_client, request):
    """An unreachable database is a 503 whose code lives at the top level, not in "errors"."""
    mocker.patch.object(
        validators, "validate_resource", side_effect=DriverConnectionError("nope")
    )
    connector = _saved_connector(request.node.name, "postgresql://test/test")

    response = auth_client.post(
        "/admin/GA_OD_Core_admin/manager/resource-config/",
        {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector.id,
            "object_location": "fail",
        },
        HTTP_ACCEPT="application/json",
    )

    assert response.status_code == 503
    validate_error(
        response,
        "Connection is not available.",
        "application/json",
        error_code="CONNECTION_UNAVAILABLE",
    )
