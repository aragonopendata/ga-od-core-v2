from multiprocessing.connection import Client
from typing import Tuple
from unittest import mock

import psycopg2
import pytest
from gunicorn.config import User
from requests import Response

import connectors
from conftest import auth_client, get_uri, create_connector_ga_od_core, create_table


@pytest.mark.django_db
def test_resource_config_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    uri = get_uri(*pg)
    connector_data = create_connector_ga_od_core(client, request.node.name, uri)
    response = client.post(
        '/GA_OD_Core_admin/manager/resource-config/', {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector_data.json()['id'],
            "object_location": 'fail'
        })
    assert response.status_code == 400
    assert response.json() == {'non_field_errors': ['Resource is not available.']}


@pytest.mark.django_db
def test_resource_too_many_rows(mock_resource_max_rows, create_full_example_fixture: Response):

    assert create_full_example_fixture.json() == {
        'non_field_errors': ['Resource is not available.']
    }


@pytest.mark.django_db
def test_resource_postgresql_table(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get('/GA_OD_Core/download', {'resource_id': create_full_example_fixture.json()['id']})
    assert download_response.json() == [{
        "id": 1,
        "name": "RX-78-2 Gundam",
        "size": 18,
        "max_acceleration": 0.93,
        "weight": "60.0",
        "description": "The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series",
        "discover_date": "0079-09-18",
        "destroyed_date": "0079-12-31T12:01:01",
        "destroyed": True
    }, {
        "id": 2,
        "name": "Half Gundam",
        "size": None,
        "max_acceleration": None,
        "weight": None,
        "description": None,
        "discover_date": None,
        "destroyed_date": None,
        "destroyed": None
    }]


@pytest.mark.django_db
def test_resource_postgresql_view(auth_client_fixture: Client, django_user_model: User, pg: Tuple[str], request):
    uri = get_uri(*pg)
    connector_data = create_connector_ga_od_core(auth_client_fixture, request.node.name, uri)
    table_name = request.node.name + '_table'
    create_table(uri, table_name)

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()
    cursor.execute(f"""CREATE view {request.node.name} as SELECT * FROM {table_name};""")
    connection.commit()

    resource_response = auth_client_fixture.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": request.node.name,
        "connector_config": connector_data.json()['id'],
        "object_location": request.node.name
    })

    assert resource_response.status_code == 201


def test_resource_api(client, request, django_user_model):
    client = auth_client(client=client, django_user_model=django_user_model)
    connector_data = create_connector_ga_od_core(client, request.node.name,
                                                 'https://people.sc.fsu.edu/~jburkardt/data/csv/crash_catalonia.csv')
    resource_response = client.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": request.node.name,
        "connector_config": connector_data.json()['id'],
        "enabled": True
    })

    assert resource_response.status_code == 201

    response = client.get(f'/GA_OD_Core/download.json', {'resource_id': resource_response.json()['id']})
    assert response.status_code == 200