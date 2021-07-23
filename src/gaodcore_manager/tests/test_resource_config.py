from multiprocessing.connection import Client

import psycopg2
import pytest
from gunicorn.config import User

import connectors
from conftest import create_connector_ga_od_core, create_table, ConnectorData


@pytest.mark.django_db
def test_resource_config_error(auth_client, connector_uri, request):
    connector_data = create_connector_ga_od_core(auth_client, request.node.name, connector_uri)
    response = auth_client.post(
        '/GA_OD_Core_admin/manager/resource-config/', {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector_data.id,
            "object_location": 'fail'
        })
    assert response.status_code == 400
    assert response.json() == {'non_field_errors':
                                   ['Resource is not available. Table, view, function, etc... not exists.']}


@pytest.mark.django_db
def test_resource_too_many_rows(mocker, auth_client: Client, full_example):
    auth_client.delete(f'/GA_OD_Core_admin/manager/resource-config/{full_example.resources.table.id}/')
    mocker.patch.object(connectors, '_RESOURCE_MAX_ROWS', 1)
    response = auth_client.post(
        '/GA_OD_Core_admin/manager/resource-config/', {
            "name": full_example.resources.table.name,
            "enabled": True,
            "connector_config": full_example.resources.table.connector_config,
            "object_location": full_example.resources.table.object_location
        })

    assert response.json() == {
        'non_field_errors': ['This resource have too many rows. For security reason this is not allowed.']
    }


@pytest.mark.django_db
def test_resource_postgresql_view(auth_client, connector_uri, request):
    connector_data = create_connector_ga_od_core(auth_client, request.node.name, connector_uri)
    table_name = request.node.name + '_table'
    create_table(connector_uri, table_name)

    connection = psycopg2.connect(connector_uri)
    cursor = connection.cursor()
    cursor.execute(f"""CREATE view "{request.node.name}" as SELECT * FROM "{table_name}";""")
    connection.commit()

    resource_response = auth_client.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": request.node.name,
        "connector_config": connector_data.id,
        "object_location": request.node.name
    })

    assert resource_response.status_code == 201


# def test_resource_api(auth_client, request, django_user_model):
#     connector_data = create_connector_ga_od_core(auth_client, request.node.name,
#                                                  'https://people.sc.fsu.edu/~jburkardt/data/csv/crash_catalonia.csv')
#     resource_response = auth_client.post('/GA_OD_Core_admin/manager/resource-config/', {
#         "name": request.node.name,
#         "connector_config": connector_data.id,
#         "enabled": True
#     })
#
#     assert resource_response.status_code == 201
#
#     response = auth_client.get(f'/GA_OD_Core/download.json', {'resource_id': resource_response.json()['id']})
#     assert response.status_code == 200
