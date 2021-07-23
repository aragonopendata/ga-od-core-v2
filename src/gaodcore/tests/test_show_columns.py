import pytest

from conftest import validate_error
from django.test import Client


@pytest.mark.django_db
def test_show_columns(client, create_full_example_postgresql_fixture):
    view_data = create_full_example_postgresql_fixture.json()
    download_response = client.get(f'/GA_OD_Core/show_columns.json', {'resource_id': view_data['id']})
    assert download_response.json() == [{
        'COLUMN_NAME': 'id',
        'DATA_TYPE': 'INTEGER'
    }, {
        'COLUMN_NAME': 'name',
        'DATA_TYPE': 'VARCHAR'
    }, {
        'COLUMN_NAME': 'size',
        'DATA_TYPE': 'BIGINT'
    }, {
        'COLUMN_NAME': 'max_acceleration',
        'DATA_TYPE': 'DOUBLE_PRECISION'
    }, {
        'COLUMN_NAME': 'weight',
        'DATA_TYPE': 'NUMERIC'
    }, {
        'COLUMN_NAME': 'description',
        'DATA_TYPE': 'TEXT'
    }, {
        'COLUMN_NAME': 'discover_date',
        'DATA_TYPE': 'DATE'
    }, {
        'COLUMN_NAME': 'destroyed_date',
        'DATA_TYPE': 'TIMESTAMP'
    }, {
        'COLUMN_NAME': 'destroyed',
        'DATA_TYPE': 'BOOLEAN'
    }]


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
def test_show_columns_resource_not_exists(accept: str, client: Client):
    download_response = client.get(f'/GA_OD_Core/show_columns', {'resource_id': -1}, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content, "Resource not exists or is not available", accept)
