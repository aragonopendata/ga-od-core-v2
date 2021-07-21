import io
import json
import os

import numpy as np
import pandas
import pytest
from django.test.client import Client

from conftest import validate_error


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_view_id(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'view_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_field(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_field_columns(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "columns": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_fields(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
@pytest.mark.parametrize("accept", [
    'text/html',
    'application/json',
    'text/csv',
    'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_non_existent_field_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                           create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "fields": ["non_existent_field"]
    }, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Field: non_existent_field not exists.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", ])
def test_download_name(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "nameRes": 'download_name'
    })
    assert 'attachment; filename="download_name.json"' == download_response['content-disposition']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download"])
def test_download_name_res(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        "nameRes": 'download_name_res'
    })
    assert 'attachment; filename="download_name_res.json"' == download_response['content-disposition']


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_format(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'formato': "csv"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.csv"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_format_error(endpoint: str, auth_client_fixture: Client,
                               create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'formato': "dj"
    })
    assert download_response.status_code == 400
    # Note is normal that all return a JSON due that formato is incorrect. Format is replacement of accept.
    assert download_response.content == \
           b'["Formato: \\"dj\\" is not allowed. Allowed values: [\'json\', \'api\', \'yaml\', \'xml\', \'xlsx\', ' \
           b'\'csv\']"]'


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_offset(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'offset': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("accept,", [
    'text/html',
    'application/json',
    'text/csv',
    'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_offset_error(endpoint: str, accept: str, auth_client_fixture: Client,
                               create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'offset': "a"
    }, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of offset is not a number.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_limit(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'limit': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[:1] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", [
    'text/html',
    'application/json',
    'text/csv',
    'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_limit_error(endpoint: str, accept: str, auth_client_fixture: Client,
                              create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'limit': "a"
    }, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of limit is not a number.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
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
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_pagination_overflow(endpoint: str, auth_client_fixture: Client,
                                      create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_pagination_page_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                        create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "a",
        '_page_size': "1"
    }, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page is not a number.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_pagination_page_size_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                             create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        '_page': "1",
        '_page_size': "a"
    }, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value of _page_size is not a number.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_filters(endpoint: str, auth_client_fixture: Client, create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_filters_json_error(endpoint: str, accept: str, auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(endpoint, {'resource_id': 1, 'filters': 'a'}, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Invalid JSON.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_filters_value_error(endpoint: str, accept: str, auth_client_fixture, django_user_model):
    download_response = auth_client_fixture.get(endpoint,
                                                {'resource_id': 1, 'filters': '{"a": []}'}, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content, 'Value [] is not a String, Integer, Float, Bool, Null or None', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
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
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
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
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
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
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_sort_non_existent_field_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                                create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    }, HTTP_ACCEPT=accept)

    assert download_response.status_code == 400
    validate_error(download_response.content, 'Sort field: acceleration not exists.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_sort_mode_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                  create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name none'
    }, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'Sort value name none is not allowed. Ej: fieldname1 asc, fieldname2 desc.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_sort_too_many_arguments_error(endpoint: str, accept: str, auth_client_fixture: Client,
                                                create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(endpoint, {
        'resource_id': create_full_example_postgresql_fixture.json()['id'],
        'sort': 'name none asd'
    }, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'Sort value name none asd is not allowed. Too many arguments.', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', ])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_resource_not_exists(endpoint: str, accept: str, client: Client):
    download_response = client.get(endpoint, {'resource_id': 96, "fields": ["id", "name"]}, HTTP_ACCEPT=accept)
    assert download_response.status_code == 400
    validate_error(download_response.content,
                   'Resource not exists or is not available', accept)


@pytest.mark.django_db
@pytest.mark.parametrize("accept", ['text/html', 'application/json', 'text/csv', 'application/xml', 'application/yaml',
                                    'application/xlsx'])
@pytest.mark.parametrize("endpoint,", ["/GA_OD_Core/download", "/GA_OD_Core/preview"])
def test_download_extension(endpoint: str, accept: str, auth_client_fixture: Client,
                            create_full_example_postgresql_fixture):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/download',
                                                {'resource_id': create_full_example_postgresql_fixture.json()['id']},
                                                HTTP_ACCEPT=accept)

    if accept == 'text/html':
        assert download_response.content
    elif accept in ['application/json', 'text/csv', 'application/xml', 'application/yaml', 'application/xlsx']:
        with open(os.path.join(os.path.dirname(__file__), f"data.{accept.split('/')[1]}"), r'rb') as f:
            if accept == 'application/xlsx':
                test_df = pandas.read_excel(io.BytesIO(f.read())).replace(np.nan, None).to_dict(orient='records')
                result_df = pandas.read_excel(io.BytesIO(download_response.content)).replace(np.nan, None).to_dict(orient='records')
                assert test_df == result_df
            else:
                assert f.read() == download_response.content
