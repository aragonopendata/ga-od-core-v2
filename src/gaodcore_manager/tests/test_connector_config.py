from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from django.contrib.auth.models import User
from django.test.client import Client
from rest_framework.response import Response


@pytest.mark.django_db
def test_connector_postgresql_json(auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
def test_connector_postgresql_xml(auth_client_fixture: Client, create_full_example_postgresql_fixture):
    xml = """
        <?xml version="1.0" encoding="utf-8"?>
            <root>
                <field_a>121.0</field_a>
                <field_b>dasd</field_b>
                <field_c></field_c>
                <field_d>2011-12-25 12:45:00</field_d>
            </root>"""
    headers = {'Content-Type': 'application/xml'}  # set what your server accepts
    requests.post('http://httpbin.org/post', data=xml, headers=headers).text
    auth_client_fixture.post('/GA_OD_Core/download.xml')

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
