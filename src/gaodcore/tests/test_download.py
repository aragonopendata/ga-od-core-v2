import json
import os
from typing import Tuple
from unittest import mock

import psycopg2
import pytest
from django.contrib.auth.models import User
from django.test.client import Client
from rest_framework.response import Response

import connectors
from conftest import auth_client, create_full_example, get_uri, create_connector_ga_od_core, create_table


@pytest.mark.django_db
def test_download_field(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "fields": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_fields(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
def test_download_non_existent_field_error(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "fields": ["non_existent_field"]
    })

    assert download_response.json() == ['Field: "\'non_existent_field\'" not exists.']


@pytest.mark.django_db
def test_download_name(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "nameRes": 'download_name'
    })
    assert 'attachment; filename="download_name.json"' == download_response['content-disposition']


@pytest.mark.django_db
def test_download_name_res(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        "nameRes": 'download_name_res'
    })
    assert 'attachment; filename="download_name_res.json"' == download_response['content-disposition']


@pytest.mark.django_db
def test_download_format(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.xml', {
        'resource_id': create_full_example_fixture.json()['id'],
        'formato': "csv"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.csv"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
def test_download_format_error(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.xml', {
        'resource_id': create_full_example_fixture.json()['id'],
        'formato': "dj"
    })

    assert download_response.status_code == 400
    assert 'Formato: "dj" is not allowed' in download_response.json()[0]


@pytest.mark.django_db
def test_download_offset(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'offset': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_offset_error(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'offset': "a"
    })

    assert download_response.status_code == 400
    assert download_response.json() == ['Value of offset is not a number.']


@pytest.mark.django_db
def test_download_limit(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'limit': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[:1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        '_page': "1",
        '_page_size': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
def test_download_pagination_overflow(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination_fail_page(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        '_page': "a",
        '_page_size': "1"
    })

    assert 'Value of _page is not a number.' == json.loads(download_response.content)[0]


@pytest.mark.django_db
def test_download_pagination_fail_page_size(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        '_page': "1",
        '_page_size': "a"
    })

    assert 'Value of _page_size is not a number.' == json.loads(download_response.content)[0]


@pytest.mark.django_db
def test_download_limit_error(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'limit': "a"
    })

    assert download_response.status_code == 400
    assert download_response.json() == ['Value of limit is not a number.']


@pytest.mark.django_db
def test_download_filters(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_filters_json_error(auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {'resource_id': 1, 'filters': 'a'})
    assert download_response.status_code == 400
    assert download_response.json() == ['Invalid JSON.']


@pytest.mark.django_db
def test_download_filters_value_error(auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json',
                                                {'resource_id': 1, 'filters': '{"a": []}'})
    assert download_response.status_code == 400
    assert download_response.json() == [f'Value [] is not a String, Integer, Float, Bool, Null or None']


@pytest.mark.django_db
def test_download_sort(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'sort': 'name desc, description, max_acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        data.sort(key=lambda x: x['name'], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_mode_errorauth_client_fixture(auth_client_fixture: Client,
                                                     create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'sort': 'name none'
    })

    assert download_response.json() == ['Sort value "name none" is not allowed. Ej: “fieldname1 asc, fieldname2 desc”.']


@pytest.mark.django_db
def test_download_sort_too_many_arguments_error(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.json', {
        'resource_id': create_full_example_fixture.json()['id'],
        'sort': 'name none asd'
    })

    assert download_response.json() == ['Sort value "name none asd" is not allowed. Too many arguments.']


@pytest.mark.django_db
def test_download_resource_not_exists(client):
    download_response = client.get(f'/GA_OD_Core/download.json', {'resource_id': 96, "fields": ["id", "name"]})
    assert download_response.status_code == 400
    assert download_response.json() == ['Resource not exists or is not available']


@pytest.mark.django_db
@pytest.mark.parametrize("extension,", ["json", "xml", "csv", "yaml"])
def test_download_extension(extension, auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download.{extension}',
                                                {'resource_id': create_full_example_fixture.json()['id']})

    with open(os.path.join(os.path.dirname(__file__), f"data.{extension}"), r'rb') as f:
        assert f.read() == download_response.content
