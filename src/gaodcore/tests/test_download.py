import json
import os

import pytest
from django.test.client import Client

from conftest import validate_error, compare_files


@pytest.fixture(params=["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def endpoint(request):
    return request.param


@pytest.mark.django_db
def test_download_view_id(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'view_id': full_example.resources.table.id, "fields": ["id"]})
    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_field(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, "fields": ["id"]})

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_field_columns(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, "columns": ["id"]})

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_fields(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, "fields": ["id", "name"]})

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_non_existent_field_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        "fields": ["non_existent_field"]
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Field: non_existent_field not exists.', accept_error)


@pytest.mark.django_db
def test_download_name(endpoint: str, client: Client, full_example):
    download_response = client.get("/GA_OD_Core/download", {
        'resource_id': full_example.resources.table.id,
        "nameRes": 'download_name'
    })
    assert 'attachment; filename="download_name.json"' == download_response['content-disposition']


@pytest.mark.django_db
def test_download_name_res(endpoint: str, client: Client, full_example):
    download_response = client.get("/GA_OD_Core/download", {
        'resource_id': full_example.resources.table.id,
        "nameRes": 'download_name_res'
    })
    assert 'attachment; filename="download_name_res.json"' == download_response['content-disposition']


@pytest.mark.django_db
def test_download_format(endpoint: str, client: Client, full_example, accept_download: str):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'formato': accept_download.split('/')[1],
        'sort': ['id']
    })

    compare_files(os.path.dirname(__file__), f'download_{full_example.scheme}', accept_download,
                  download_response.content)


@pytest.mark.django_db
def test_download_format_error(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, 'formato': "dj"})
    assert download_response.status_code == 400
    # Note is normal that all return a JSON due that formato is incorrect. Format is replacement of accept.
    assert download_response.content == \
           b'["Formato: \\"dj\\" is not allowed. Allowed values: [\'json\', \'api\', \'yaml\', \'xml\', \'xlsx\', ' \
           b'\'csv\']"]'


@pytest.mark.django_db
def test_download_offset(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, 'offset': "1"})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        assert json.loads(file.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_offset_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'offset': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of offset is not a number.', accept_error)


@pytest.mark.django_db
def test_download_limit(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, 'limit': "1"})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), 'rb') as file:
        data = file.read()
        assert json.loads(data)[:1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_limit_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'limit': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of limit is not a number.', accept_error)


@pytest.mark.django_db
def test_download_pagination(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "1",
        '_page_size': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
def test_download_pagination_overflow(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination_page_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "a",
        '_page_size': "1"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page is not a number.', accept_error)


@pytest.mark.django_db
def test_download_pagination_page_size_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "1",
        '_page_size': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page_size is not a number.', accept_error)


@pytest.mark.django_db
def test_download_filters(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        assert json.loads(file.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_filters_json_error(endpoint: str, accept_error, client: Client):
    download_response = client.get(endpoint, {'resource_id': 1, 'filters': 'a'}, HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Invalid JSON.', accept_error)


@pytest.mark.django_db
def test_download_filters_value_error(endpoint: str, accept_error, client: Client):
    download_response = client.get(endpoint, {'resource_id': 1, 'filters': '{"a": []}'}, HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value [] is not a String, Integer, Float, Bool, Null or None',
                   accept_error)


@pytest.mark.django_db
def test_download_sort_asc(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, 'sort': 'name asc'})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x['name'])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_desc(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {'resource_id': full_example.resources.table.id, 'sort': 'name desc'})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x['name'], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_n(endpoint: str, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name asc, description'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: [x['name'], x['description']])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_non_existent_field_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name desc, description, acceleration asc'
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Sort field: acceleration not exists.', accept_error)


@pytest.mark.django_db
def test_download_sort_mode_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name none'
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'Sort value name none is not allowed. Ej: fieldname1 asc, fieldname2 desc.', accept_error)


@pytest.mark.django_db
def test_download_sort_too_many_arguments_error(endpoint: str, accept_error, client: Client, full_example):
    download_response = client.get(endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name none asd'
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Sort value name none asd is not allowed. Too many arguments.',
                   accept_error)


@pytest.mark.django_db
def test_download_resource_not_exists(endpoint: str, accept_error, client: Client):
    download_response = client.get(endpoint, {'resource_id': 96, "fields": ["id", "name"]}, HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Resource not exists or is not available', accept_error)


@pytest.mark.django_db
def test_download_extension(endpoint: str, accept_download: str, client: Client, full_example):
    download_response = client.get('/GA_OD_Core/download', {'resource_id': full_example.resources.table.id},
                                   HTTP_ACCEPT=accept_download)
    compare_files(os.path.dirname(__file__), f'download_{full_example.scheme}', accept_download,
                  download_response.content)
