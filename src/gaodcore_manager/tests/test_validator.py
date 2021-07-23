import os

import pytest

import connectors
from conftest import ConnectorData


@pytest.mark.django_db
def test_validator(auth_client, full_example):
    download_response = auth_client.get(f'/GA_OD_Core_admin/manager/validator.json', {
        'object_location': full_example.resources.table.object_location,
        'uri': full_example.uri
    })
    with open(os.path.join(os.path.dirname(__file__), f"../../gaodcore/tests/download.json"), r'rb') as f:
        data = f.read()
        assert data == download_response.content


@pytest.mark.django_db
def test_validator_object_location_error(auth_client, connector_uri):
    download_response = auth_client.get(f'/GA_OD_Core_admin/manager/validator.json', {
        'object_location': 'fail',
        'uri': connector_uri
    })
    assert download_response.json() == ['Resource is not available. Table, view, function, etc... not exists.']
    assert download_response.status_code == 400


@pytest.mark.django_db
def test_validator_object_database_error(auth_client, connector_uri):
    url = connector_uri.rsplit('/', 1)[0] + '/fail'
    download_response = auth_client.get(f'/GA_OD_Core_admin/manager/validator.json', {
        'object_location': 'fail',
        'uri': url
    })
    assert download_response.json() == ['Connection is not available.']


@pytest.mark.django_db
def test_validator_object_database_error(auth_client, connector_uri):
    url = connector_uri.rsplit('/', 1)[0] + '/fail'
    download_response = auth_client.get(f'/GA_OD_Core_admin/manager/validator.json', {
        'object_location': 'fail',
        'uri': url
    })
    assert download_response.json() == ['Connection is not available.']


@pytest.mark.django_db
def test_validator_too_many_rows(auth_client, full_example, mocker):
    mocker.patch.object(connectors, '_RESOURCE_MAX_ROWS', 1)
    download_response = auth_client.get(f'/GA_OD_Core_admin/manager/validator.json', {
        'object_location': full_example.resources.table.object_location,
        'uri': full_example.uri
    })
    assert download_response.json() == ['This resource have too many rows. For security reason this is not allowed.']
