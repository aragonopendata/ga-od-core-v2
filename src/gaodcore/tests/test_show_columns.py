"""Test show_columns endpoint."""

from django.test import Client
import pytest

from conftest import validate_error


@pytest.mark.django_db
def test_show_columns(client: Client, full_example):
    """Test if show columns endpoint is working as expected."""
    download_response = client.get('/GA_OD_Core/show_columns.json', {'resource_id': full_example.resources.table.id})
    data = download_response.json()
    if full_example.scheme == 'postgresql':
        assert data == [{
            'COLUMN_NAME': 'id',
            'DATA_TYPE': 'INTEGER'
        }, {
            'COLUMN_NAME': 'name',
            'DATA_TYPE': 'VARCHAR(500)'
        }, {
            'COLUMN_NAME': 'size',
            'DATA_TYPE': 'BIGINT'
        }, {
            'COLUMN_NAME': 'max_acceleration',
            'DATA_TYPE': 'DOUBLE_PRECISION'
        }, {
            'COLUMN_NAME': 'weight',
            'DATA_TYPE': 'DOUBLE_PRECISION'
        }, {
            'COLUMN_NAME': 'description',
            'DATA_TYPE': 'VARCHAR(500)'
        }, {
            'COLUMN_NAME': 'discover_date',
            'DATA_TYPE': 'DATE'
        }, {
            'COLUMN_NAME': 'destroyed_date',
            'DATA_TYPE': 'TIMESTAMP'
        }, {
            'COLUMN_NAME': 'destroyed',
            'DATA_TYPE': 'BOOLEAN'
        }, {
            'COLUMN_NAME': 'empty',
            'DATA_TYPE': 'VARCHAR(500)'
        }]
    elif full_example.scheme == 'mysql':
        assert data == [{
            'COLUMN_NAME': 'id',
            'DATA_TYPE': 'INTEGER'
        }, {
            'COLUMN_NAME': 'name',
            'DATA_TYPE': 'VARCHAR(500)'
        }, {
            'COLUMN_NAME': 'size',
            'DATA_TYPE': 'BIGINT'
        }, {
            'COLUMN_NAME': 'max_acceleration',
            'DATA_TYPE': 'FLOAT'
        }, {
            'COLUMN_NAME': 'weight',
            'DATA_TYPE': 'FLOAT'
        }, {
            'COLUMN_NAME': 'description',
            'DATA_TYPE': 'VARCHAR(500)'
        }, {
            'COLUMN_NAME': 'discover_date',
            'DATA_TYPE': 'DATE'
        }, {
            'COLUMN_NAME': 'destroyed_date',
            'DATA_TYPE': 'DATETIME'
        }, {
            'COLUMN_NAME': 'destroyed',
            'DATA_TYPE': 'TINYINT'
        }, {
            'COLUMN_NAME': 'empty',
            'DATA_TYPE': 'VARCHAR(500)'
        }]
    elif full_example.scheme in ['http', 'https']:
        assert data == [{
            'COLUMN_NAME': 'id',
            'DATA_TYPE': 'INTEGER'
        }, {
            'COLUMN_NAME': 'name',
            'DATA_TYPE': 'TEXT'
        }, {
            'COLUMN_NAME': 'size',
            'DATA_TYPE': 'INTEGER'
        }, {
            'COLUMN_NAME': 'max_acceleration',
            'DATA_TYPE': 'REAL'
        }, {
            'COLUMN_NAME': 'weight',
            'DATA_TYPE': 'REAL'
        }, {
            'COLUMN_NAME': 'description',
            'DATA_TYPE': 'TEXT'
        }, {
            'COLUMN_NAME': 'discover_date',
            'DATA_TYPE': 'TEXT'
        }, {
            'COLUMN_NAME': 'destroyed_date',
            'DATA_TYPE': 'TEXT'
        }, {
            'COLUMN_NAME': 'destroyed',
            'DATA_TYPE': 'BOOLEAN'
        }, {
            'COLUMN_NAME': 'empty',
            'DATA_TYPE': 'TEXT'
        }]
    else:
        raise NotImplementedError


@pytest.mark.django_db
def test_show_columns_resource_not_exists(accept_error, client: Client):

    download_response = client.get('/GA_OD_Core/show_columns', {'resource_id': -1}, HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, "Resource not exists or is not available", accept_error)
