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
        "/GA_OD_Core_admin/manager/resource-config/",
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
        "/GA_OD_Core_admin/manager/resource-config/",
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
            response.content,
            "Resource is not available. Table, view, function, etc... not exists.",
            accept_error,
            "non_field_errors",
        )
    elif scheme in ["http", "https"]:
        validate_error(
            response.content,
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
        f"/GA_OD_Core_admin/manager/resource-config/{full_example.resources.table.id}/"
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
        "/GA_OD_Core_admin/manager/resource-config/", conf, HTTP_ACCEPT=accept_error
    )

    assert response.status_code == 400
    validate_error(
        response.content,
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
        "/GA_OD_Core_admin/manager/resource-config/",
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
        response.content,
        "Connection is not available.",
        accept_error,
        "non_field_errors",
    )
