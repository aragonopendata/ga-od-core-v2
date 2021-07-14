import pytest

from conftest import auth_client, create_full_example


@pytest.mark.django_db
def test_show_columns(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, pg, request)
    view_data = view_response.json()
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
