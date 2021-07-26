import json
import os

import psycopg2
import pytest

import connectors
from gaodcore.tests.helpers import get_uri, create_connector_ga_od_core, create_table, auth_client, \
    create_full_example


@pytest.mark.django_db
def test_download_fields(auth_client, pg, request):
    view_response = create_full_example(auth_client, *pg, request.node.originalname)
    download_response = auth_client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        "fields": ["id", "name"]
    })

    assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]


@pytest.mark.django_db
def test_download_nameres(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        "nameRes": 'test'
    })
    # TODO: django client not return headers


@pytest.mark.django_db
def test_download_field(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        "fields": ["id"]
    })

    assert download_response.json() == [{'id': 1}, {'id': 2}]


@pytest.mark.django_db
def test_download_non_existent_field_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        "fields": ["non_existent_field"]
    })

    assert download_response.json() == ['Field: "\'non_existent_field\'" not exists.']


@pytest.mark.django_db
def test_download_format(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.xml', {
        'resource_id': view_response.json()['id'],
        'formato': "csv"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.csv"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
def test_download_format_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.xml', {
        'resource_id': view_response.json()['id'],
        'formato': "dj"
    })

    assert download_response.status_code == 400
    assert 'Formato: "dj" is not allowed' in download_response.json()[0]


@pytest.mark.django_db
def test_download_offset(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'offset': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_offset_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'offset': "a"
    })

    assert download_response.status_code == 400
    assert download_response.json() == ['Value of offset is not a number.']


@pytest.mark.django_db
def test_download_limit(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'limit': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[:1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        '_page': "1",
        '_page_size': "1"
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        response_data = json.loads(download_response.content)
        assert data[1:2] == response_data


@pytest.mark.django_db
def test_download_pagination_overflow(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        '_page': "2",
        '_page_size': "1"
    })

    assert [] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_pagination_fail_page(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        '_page': "a",
        '_page_size': "1"
    })

    assert json.loads(download_response.content)[0] == 'Value of _page is not a number.'


@pytest.mark.django_db
def test_download_pagination_fail_page_size(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        '_page': "1",
        '_page_size': "a"
    })

    assert json.loads(download_response.content)[0] == 'Value of _page_size is not a number.'


@pytest.mark.django_db
def test_download_limit_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'limit': "a"
    })

    assert download_response.status_code == 400
    assert download_response.json() == ['Value of limit is not a number.']


@pytest.mark.django_db
def test_download_filters(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'filters': '{"description": null}'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_filters_json_error(client, django_user_model):
    client = auth_client(client=client, django_user_model=django_user_model)

    download_response = client.get(f'/GA_OD_Core/download.json', {'resource_id': 1, 'filters': 'a'})
    assert download_response.status_code == 400
    assert download_response.json() == ['Invalid JSON.']


@pytest.mark.django_db
def test_download_filters_value_error(client, django_user_model):
    client = auth_client(client=client, django_user_model=django_user_model)

    download_response = client.get(f'/GA_OD_Core/download.json', {'resource_id': 1, 'filters': '{"a": []}'})
    assert download_response.status_code == 400
    assert download_response.json() == [f'Value [] is not a String, Integer, Float, Bool, Null or None']


@pytest.mark.django_db
def test_download_sort(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1:2] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)

    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'sort': 'name desc, description, acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert json.loads(f.read())[1] == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'sort': 'name desc, description, max_acceleration asc'
    })

    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        data = json.loads(f.read())
        data.sort(key=lambda x: x['name'], reverse=True)
        assert data == json.loads(download_response.content)


@pytest.mark.django_db
def test_download_sort_mode_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'sort': 'name none'
    })

    assert download_response.json() == ['Sort value "name none" is not allowed. Ej: “fieldname1 asc, fieldname2 desc”.']


@pytest.mark.django_db
def test_download_sort_too_many_arguments_error(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/download.json', {
        'resource_id': view_response.json()['id'],
        'sort': 'name none asd'
    })

    assert download_response.json() == ['Sort value "name none asd" is not allowed. Too many arguments.']


@pytest.mark.django_db
def test_download_object_location_error(client):
    download_response = client.get(f'/GA_OD_Core/download.json', {'resource_id': 96, "fields": ["id", "name"]})
    assert download_response.status_code == 400
    assert download_response.json() == ['Resource not exists or is not available']


@pytest.mark.django_db
@pytest.mark.parametrize("extension,", ["json", "xml", "csv", "yaml"])
def test_download_extension(client, django_user_model, pg, request, extension):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, f"{request.node.originalname}_{extension}")
    download_response = client.get(f'/GA_OD_Core/download.{extension}', {'resource_id': view_response.json()['id']})

    with open(os.path.join(os.path.dirname(__file__), f"data.{extension}"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
def test_download_too_many_rows(client, django_user_model, pg, request, mocker):
    mocker.patch.object(connectors, '_RESOURCE_MAX_ROWS', 1)
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)

    assert view_response.json() == {
        'non_field_errors': ['This resource have too many rows. For security reason this is not allowed.']
    }


@pytest.mark.django_db
def test_download_postgresql_table(client, django_user_model, request, pg):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)
    download_response = client.get('/GA_OD_Core/download', {'resource_id': view_response.json()['id']})
    assert download_response.json() == [{
        "id": 1,
        "name": "RX-78-2 Gundam",
        "size": 18,
        "max_acceleration": 0.93,
        "weight": "60.0",
        "description": "The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series",
        "discover_date": "0079-09-18",
        "destroyed_date": "0079-12-31T12:01:01",
        "destroyed": True
    }, {
        "id": 2,
        "name": "Half Gundam",
        "size": None,
        "max_acceleration": None,
        "weight": None,
        "description": None,
        "discover_date": None,
        "destroyed_date": None,
        "destroyed": None
    }]


@pytest.mark.django_db
def test_download_postgresql_view(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    uri = get_uri(*pg)
    connector_data = create_connector_ga_od_core(client, request.node.name, uri)
    table_name = request.node.name + '_table'
    create_table(uri, table_name)

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()
    cursor.execute(f"""CREATE view {request.node.name} as SELECT * FROM {table_name};""")
    connection.commit()

    resource_response = client.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": request.node.name,
        "connector_config": connector_data.json()['id'],
        "object_location": request.node.name
    })

    assert resource_response.status_code == 201


def test_download_api(client, request, django_user_model):
    client = auth_client(client=client, django_user_model=django_user_model)
    connector_data = create_connector_ga_od_core(client, request.node.name, 'https://people.sc.fsu.edu/~jburkardt/data/csv/crash_catalonia.csv')
    resource_response = client.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": request.node.name,
        "connector_config": connector_data.json()['id'],
        "enabled": True
    })

    assert resource_response.status_code == 201

    response = client.get(f'/GA_OD_Core/download.json', {'resource_id': resource_response.json()['id']})
    assert response.status_code == 200
