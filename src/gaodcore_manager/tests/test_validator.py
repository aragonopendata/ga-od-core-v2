import os
from urllib.parse import urlparse

import pytest
from _pytest.logging import LogCaptureFixture
from django.test import Client
from pytest_httpserver import HTTPServer

import connectors
from conftest import ConnectorData, compare_files, validate_error, PROJECT_DIR


@pytest.mark.django_db
def test_validator(auth_client: Client, full_example: ConnectorData, accept_download: str):
    conf = {'uri': full_example.uri}

    if full_example.resources.table.object_location:
        conf['object_location'] = full_example.resources.table.object_location

    download_response = auth_client.get('/GA_OD_Core_admin/manager/validator', conf, HTTP_ACCEPT=accept_download)
    compare_files(os.path.join(os.path.dirname(__file__), '..', '..', 'gaodcore', 'tests'),
                  f'download_{full_example.scheme}', accept_download, download_response.content)


@pytest.mark.django_db
def test_validator_invalid_uri_error(auth_client, accept_error):
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        'object_location': 'fail',
        'uri': 'postgresql://test/adsf'
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    validate_error(response.content, 'Connection is not available.', accept_error)


@pytest.mark.django_db
def test_validator_invalid_schema_error(auth_client, accept_error):
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        'object_location': 'fail',
        'uri': 'test://test/adsf'
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    validate_error(response.content, 'Schema of the URI is not available.', accept_error)


@pytest.mark.django_db
def test_validator_invalid_location_error(auth_client, connector_uri, accept_error):
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        'object_location': 'fail',
        'uri': connector_uri
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    scheme = urlparse(connector_uri).scheme
    if scheme in ['postgresql', 'mysql']:
        validate_error(response.content, 'Resource is not available. Table, view, function, etc... not exists.',
                       accept_error)
    elif scheme in ['http', 'https']:
        validate_error(response.content,
                       'Object location or object location schema is not allowed in http and https resources',
                       accept_error)
    else:
        raise NotImplementedError


@pytest.mark.django_db
def test_validator_invalid_location_schema_error(auth_client, connector_uri, accept_error):
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        'object_location_schema': 'test',
        'object_location': 'fail',
        'uri': connector_uri
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    scheme = urlparse(connector_uri).scheme
    if scheme in ['postgresql']:
        validate_error(response.content, 'Resource is not available. Table, view, function, etc... not exists.',
                       accept_error)
    elif scheme in ['mysql']:
        validate_error(response.content, 'Object location schema is not allowed in mysql resources', accept_error)
    elif scheme in ['http', 'https']:
        validate_error(response.content,
                       'Object location or object location schema is not allowed in http and https resources',
                       accept_error)
    else:
        raise NotImplementedError


@pytest.mark.django_db
def test_validator_malformed_uri_error(auth_client, accept_error):
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        'object_location': 'fail',
        'uri': 'uri'
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    validate_error(response.content, 'Schema of the URI is not available.', accept_error)


@pytest.mark.django_db
def test_validator_config_path_error(auth_client: Client, connector_uri: str, accept_error: str):
    connector_uri = connector_uri.rsplit('/', 1)[0] + '/path-error'

    data = {'uri': connector_uri}
    if urlparse(connector_uri).scheme in ['postgresql', 'mysql']:
        data['object_location'] = 'fail'

    download_response = auth_client.get('/GA_OD_Core_admin/manager/validator', data, HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Connection is not available.', accept_error)

# TODO: Fix this test
@pytest.mark.xfail(reason="This test is failing. Fix it.")
@pytest.mark.django_db
def test_validator_too_many_rows(auth_client, full_example, mocker, accept_error):
    mocker.patch.object(connectors, '_RESOURCE_MAX_ROWS', 1)
    conf = {'uri': full_example.uri}
    if full_example.resources.table.object_location:
        conf['object_location'] = full_example.resources.table.object_location

    download_response = auth_client.get('/GA_OD_Core_admin/manager/validator', conf, HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'This resource have too many rows. For security reason this is not allowed.', accept_error)

# TODO: Fix this test
@pytest.mark.xfail(reason="Need to rethink this test. ")
@pytest.mark.django_db
def test_validator_credentials_error(auth_client: Client, connector_uri: str, caplog: LogCaptureFixture,
                                     accept_error: str):
    parsed = urlparse(connector_uri)
    if parsed.scheme in ['http', 'https']:
        # TODO: currently pytest-httpserver is not easy to auth
        return
    parsed = parsed._replace(netloc="{}:{}@{}:{}".format('invalid_username', 'invalid_password', parsed.hostname,
                                                         parsed.netloc.split(':')[-1]))
    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {
        "object_location": 'fail',
        "uri": parsed.geturl()
    },
                               HTTP_ACCEPT=accept_error)
    assert response.status_code == 400
    if parsed.scheme == 'postgresql':
        assert 'FATAL:  password authentication failed for user "invalid_username"' in caplog.text
    elif parsed.scheme == 'mysql':
        assert "Access denied for user 'invalid_username'" in caplog.text
    else:
        raise NotImplementedError
    validate_error(response.content, 'Connection is not available.', accept_error)


@pytest.mark.django_db
def test_api_content_type_error(auth_client, httpserver: HTTPServer, accept_error):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download_postgresql.xlsx')
    url = '/test'
    with open(path, 'rb') as file:
        httpserver.expect_request(url).respond_with_data(file.read(), content_type='application/xlsx')

    response = auth_client.get('/GA_OD_Core_admin/manager/validator', {'uri': httpserver.url_for(url)},
                               HTTP_ACCEPT=accept_error)
    validate_error(response.content, 'Mimetype of content-type is not allowed. Only allowed: JSON mimetypes.',
                   accept_error)
