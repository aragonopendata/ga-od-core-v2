"""Test download and preview endpoint."""

import json
import os

import pytest
from django.test.client import Client

from conftest import validate_error, compare_files


@pytest.mark.django_db
def test_download_view_id(download_endpoint, client: Client, full_example):
    """Check if view_id parameter is working as expected. This will return records of view_id."""
    download_response = client.get(download_endpoint, {'view_id': full_example.resources.table.id, "fields": ["id"]})
    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_field(download_endpoint, client: Client, full_example):
    """Check if columns parameter is working as expected. This will only show the column of resource that match with the
    field."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "fields": ["id"]
    })

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_field_columns(download_endpoint, client: Client, full_example):
    """Check if columns parameter is working as expected. This will only show columns of resource that match with any
    of the fields.
    """
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "columns": ["id"]
    })

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_fields(download_endpoint, client: Client, full_example):
    """Check if fields parameter is working as expected. This will only show columns of resource that match with any of
    the fields."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "fields": ["id", "name"]
    })

    response = download_response.json()
    response.sort(key=lambda item: item['id'])
    assert response == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
def test_download_non_existent_field_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when page size value is not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "fields": ["non_existent_field"]
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Field: non_existent_field not exists.', accept_error)


@pytest.mark.django_db
def test_download_name(download_endpoint, client: Client, full_example):
    """Check if name parameter is working as expected. Filename must be the value of name."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "nameRes": 'download_name'
    })
    assert download_response['content-disposition'] == 'attachment; filename="download_name.json"'


@pytest.mark.django_db
def test_download_name_res(download_endpoint, client: Client, full_example):
    """Check if name_res parameter is working as expected. Filename must be the value of name_res."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        "nameRes": 'download_name_res'
    })
    assert download_response['content-disposition'] == 'attachment; filename="download_name_res.json"'


@pytest.mark.django_db
def test_download_format(download_endpoint, client: Client, full_example, accept_download: str):
    """Check if formato parameter is working as expected. File downloaded must be in correct file format."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'formato': accept_download.split('/')[1],
        'sort': ['id']
    })

    compare_files(os.path.dirname(__file__), f'download_{full_example.scheme}', accept_download,
                  download_response.content)


@pytest.mark.django_db
def test_download_format_error(download_endpoint, client: Client, full_example):
    """Check message error when formato value is not valid."""
    download_response = client.get(download_endpoint, {'resource_id': full_example.resources.table.id, 'formato': "dj"})
    assert download_response.status_code == 400
    # Note is normal that GAODCore return a JSON due that formato is incorrect. Format is replacement of accept.
    assert download_response.content == \
           b'["Formato: \\"dj\\" is not allowed. Allowed values: [\'json\', \'api\', \'yaml\', \'xml\', \'xlsx\', ' \
           b'\'csv\']"]'


@pytest.mark.django_db
def test_download_offset(download_endpoint, client: Client, full_example):
    """Check if offset parameter is working as expected. The first number of records must be removed."""
    download_response = client.get(download_endpoint, {'resource_id': full_example.resources.table.id, 'offset': "1"})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        assert json.loads(file.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_offset_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when offset value is not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'offset': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of offset is not a number.', accept_error)


@pytest.mark.django_db
def test_download_limit(download_endpoint, client: Client, full_example):
    """Check if limit parameter is working as expected. Number of records must be limited."""
    download_response = client.get(download_endpoint, {'resource_id': full_example.resources.table.id, 'limit': "1"})

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), 'rb') as file:
        data = file.read()
        assert json.loads(data)[:1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_limit_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when limit value is not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'limit': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of limit is not a number.', accept_error)


@pytest.mark.django_db
def test_download_pagination(download_endpoint, client: Client, full_example):
    """Check if pagination parameter is working as expected. Pagination must show correct records."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "1",
        '_page_size': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
def test_download_pagination_overflow(download_endpoint, client: Client, full_example):
    """Check when there are not records to show. Pagination overflow size of number of records."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination_page_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when page value is not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "a",
        '_page_size': "1"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page is not a number.', accept_error)


@pytest.mark.django_db
def test_download_pagination_page_size_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when page size_value is not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        '_page': "1",
        '_page_size': "a"
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page_size is not a number.', accept_error)


@pytest.mark.django_db
def test_download_filters(download_endpoint, client: Client, full_example):
    """Check if filters parameter is working as expected. Filters must filter as expected."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        assert json.loads(file.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_filters_json_error(download_endpoint, accept_error, client: Client):
    """Check message error when invalid json is set as filter value."""
    download_response = client.get(download_endpoint, {'resource_id': 1, 'filters': 'a'}, HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Invalid JSON.', accept_error)


@pytest.mark.django_db
def test_download_filters_value_error(download_endpoint, accept_error, client: Client):
    """Check message error when filter values are not valid."""
    download_response = client.get(download_endpoint, {
        'resource_id': 1,
        'filters': '{"a": []}'
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value [] is not a String, Integer, Float, Bool, Null or None',
                   accept_error)


@pytest.mark.django_db
def test_download_sort_asc(download_endpoint, client: Client, full_example):
    """Check if sort parameter is working as expected. Sort must be asc."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name asc'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x['name'])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_desc(download_endpoint, client: Client, full_example):
    """Check if sort parameter is working as expected. Sort must be desc."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name desc'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: x['name'], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_n(download_endpoint, client: Client, full_example):
    """Check if multiple sort parameter are working as expected. Result must be correctly ordered."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name asc, description'
    })

    with open(os.path.join(os.path.dirname(__file__), "download_postgresql.json"), r'rb') as file:
        data = json.loads(file.read())
        data.sort(key=lambda x: [x['name'], x['description']])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_non_existent_field_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when field not exists in resource."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name desc, description, acceleration asc'
    },
                                   HTTP_ACCEPT=accept_error)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Sort field: acceleration not exists.', accept_error)


@pytest.mark.django_db
def test_download_sort_mode_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when sort mode argument is not correct."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name none'
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'Sort value name none is not allowed. Ej: fieldname1 asc, fieldname2 desc.', accept_error)


@pytest.mark.django_db
def test_download_sort_too_many_arguments_error(download_endpoint, accept_error, client: Client, full_example):
    """Check message error when there are too many arguments in sort parameter."""
    download_response = client.get(download_endpoint, {
        'resource_id': full_example.resources.table.id,
        'sort': 'name none asd'
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Sort value name none asd is not allowed. Too many arguments.',
                   accept_error)


@pytest.mark.django_db
def test_download_resource_not_exists(download_endpoint, accept_error, client: Client):
    """Check message error when resource not exists."""
    download_response = client.get(download_endpoint, {
        'resource_id': 96,
        "fields": ["id", "name"]
    },
                                   HTTP_ACCEPT=accept_error)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Resource not exists or is not available', accept_error)


@pytest.mark.django_db
def test_download_extension(download_endpoint, accept_download: str, client: Client, full_example):
    """Check if format extension is working as expected. This must download file in forrect file format."""
    download_response = client.get(download_endpoint, {'resource_id': full_example.resources.table.id},
                                   HTTP_ACCEPT=accept_download)
    compare_files(os.path.dirname(__file__), f'download_{full_example.scheme}', accept_download,
                  download_response.content)
