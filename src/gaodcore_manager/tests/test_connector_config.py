from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from django.contrib.auth.models import User
from django.test.client import Client
from rest_framework.response import Response


@pytest.mark.django_db
def test_connector_postgresql(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
def test_connector_config_db_error(auth_client_fixture: Client, django_user_model: User, request: FixtureRequest, pg: Tuple[str]):
    uri = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/test"
    response = auth_client_fixture.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Connection is not available.']}


@pytest.mark.django_db
def test_connector_postgresql_credentials_error(auth_client_fixture: Client, request: FixtureRequest):
    uri = f"postgresql://error:error@localhost:1/guillotina"
    response = auth_client_fixture.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Connection is not available.']}


@pytest.mark.django_db
def test_connector_config_schema_error(auth_client_fixture: Client, request: FixtureRequest):
    uri = f"test://postgres:postgres@localhost:1/guillotina"
    response = auth_client_fixture.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Schema of the URI is not available.']}
