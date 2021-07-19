import json
import os

import pytest
from django.test.client import Client
from rest_framework.response import Response


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_field(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_fields(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_non_existent_field_error(endpoint: str, auth_client_fixture: Client,
                                           create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["non_existent_field"]
    })

    assert download_response.json() == ['Field: "\'non_existent_field\'" not exists.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_name(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "nameRes": 'download_name'
    })
    assert 'attachment; filename="download_name.json"' == download_response['content-disposition']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_name_res(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "nameRes": 'download_name_res'
    })
    assert 'attachment; filename="download_name_res.json"' == download_response['content-disposition']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_format(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'formato': "csv"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.csv"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_format_error(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'formato': "dj"
    })

    assert download_response.status_code == 400
    assert 'Formato: "dj" is not allowed' in download_response.json()[0]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_offset(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'offset': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_offset_error(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'offset': "a"
    })

    assert download_response.status_code == 400
    assert download_response.json() == ['Value of offset is not a number.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_limit(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'limit': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[:1] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_limit_error(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'limit': "a"
    })

    assert download_response.json() == ['Value of limit is not a number.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_pagination(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "1",
        '_page_size': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_pagination_overflow(endpoint: str, auth_client_fixture: Client,
                                      create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_pagination_page_error(endpoint: str, auth_client_fixture: Client,
                                        create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "a",
        '_page_size': "1"
    })

    assert 'Value of _page is not a number.' == json.loads(download_response.content)[0]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_pagination_page_size_error(endpoint: str, auth_client_fixture: Client,
                                             create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "1",
        '_page_size': "a"
    })

    assert 'Value of _page_size is not a number.' == json.loads(download_response.content)[0]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_filters(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_filters_json_error(endpoint: str, auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(endpoint, {'resource_id': 1, 'filters': 'a'})
    assert download_response.status_code == 400
    assert download_response.json() == ['Invalid JSON.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_filters_value_error(endpoint: str, auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(endpoint,
                                                {'resource_id': 1, 'filters': '{"a": []}'})
    assert download_response.status_code == 400
    assert download_response.json() == [f'Value [] is not a String, Integer, Float, Bool, Null or None']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_asc(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        data.sort(key=lambda x: x['name'])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_desc(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name desc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        data.sort(key=lambda x: x['name'], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_n(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name asc, description'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        data.sort(key=lambda x: [x['name'], x['description']])
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_non_existent_field_error(endpoint: str, auth_client_fixture: Client,
                                                create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    })

    assert download_response.json() == ['Sort field: "\'acceleration\' not exists.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_mode_error(endpoint: str, auth_client_fixture: Client,
                                  create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name none'
    })

    assert download_response.json() == ['Sort value "name none" is not allowed. Ej: “fieldname1 asc, fieldname2 desc”.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_sort_too_many_arguments_error(endpoint: str, auth_client_fixture: Client,
                                                create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name none asd'
    })

    assert download_response.json() == ['Sort value "name none asd" is not allowed. Too many arguments.']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download.json", "/GA_OD_Core/preview.json"])
def test_download_resource_not_exists(endpoint: str, client):
    download_response = client.get(endpoint, {'resource_id': 96, "fields": ["id", "name"]})
    assert download_response.status_code == 400
    assert download_response.json() == ['Resource not exists or is not available']


@pytest.mark.django_db
@pytest.mark.parametrize("extension,", ["json", "xml", "csv", "yaml"])
def test_download_extension(extension, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.{extension}',
                                                {'resource_id': create_full_example_postgresql_fixture.json()['id']})

    with open(os.path.join(os.path.dirname(__file__), f"data.{extension}"), r'rb') as f:
        assert f.read() == download_response.content
