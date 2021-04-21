import datetime
import json
import os

import psycopg2
import pytest
from django.db.models import Model
from django.test import Client
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Float, Numeric, Boolean, Date, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker


# TODO: CREATE test for not enabled connections and views
# TODO: Refactor


def get_auth_client(client: Client, django_user_model: Model):
    username = "user"
    password = "password"
    user = django_user_model.objects.create_user(username=username, password=password)
    client.force_login(user)

    return client


def get_uri(host: str, port: str) -> str:
    return f"postgresql://postgres:@{host}:{port}/guillotina"


def create_connector_ga_od_core(client: Client, test_name: str, uri: str):
    return client.post('/gaodcore/connector-config/', {"name": test_name,
                                                       "enabled": True,
                                                       "uri": uri})


def create_table(uri: str, test_name: str):
    engine = create_engine(uri, echo=True)

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class TestData(Base):
        __tablename__ = test_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String, unique=True)
        size = Column(BigInteger)
        max_acceleration = Column(Float)
        weight = Column(Numeric)
        description = Column(Text)
        discover_date = Column(Date)
        destroyed_date = Column(DateTime)
        destroyed = Column(Boolean)

    Base.metadata.create_all(engine)

    session = Session()

    session.add(TestData(
        name='RX-78-2 Gundam',
        size=18,
        max_acceleration=0.93,
        weight=60.0,
        description='The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series',
        discover_date=datetime.date(79, 9, 18),
        destroyed_date=datetime.datetime(79, 12, 31, 12, 1, 1),
        destroyed=True
    ))
    session.add(TestData(
        name='Half Gundam',
    ))

    session.commit()
    Session.close_all()


def create_view(client, test_name: str, connector_data):
    return client.post('/gaodcore/resource-config/', {
        "name": test_name,
        "enabled": True,
        "connector_config": connector_data.json()['id'],
        "object_location": test_name})


def create_full_example(client, host: str, port: str, test_name: str):
    uri = get_uri(host, port)
    connector_data = create_connector_ga_od_core(client, test_name, uri)
    create_table(uri, test_name)
    return create_view(client, test_name, connector_data)

#
# @pytest.mark.django_db
# def test_postgresql(client, django_user_model, request, pg):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.name)
#     download_response = client.get('/gaodcore/download', {'resource_id': view_response.json()['id']})
#     assert download_response.json() == [{
#         "id": 1,
#         "name": "RX-78-2 Gundam",
#         "size": 18,
#         "max_acceleration": 0.93,
#         "weight": "60.0",
#         "description": "The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series",
#         "discover_date": "0079-09-18",
#         "destroyed_date": "0079-12-31T12:01:01",
#         "destroyed": True}, {
#         "id": 2,
#         "name": "Half Gundam",
#         "size": None,
#         "max_acceleration": None,
#         "weight": None,
#         "description": None,
#         "discover_date": None,
#         "destroyed_date": None,
#         "destroyed": None}]
#
#
# @pytest.mark.django_db
# def test_postgresql_view(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     uri = get_uri(*pg)
#     connector_data = create_connector_ga_od_core(client, request.node.name, uri)
#     table_name = request.node.name + '_table'
#     create_table(uri, table_name)
#
#     connection = psycopg2.connect(uri)
#     cursor = connection.cursor()
#     cursor.execute(f"""CREATE view {request.node.name} as SELECT * FROM {table_name};""")
#     connection.commit()
#
#     view_response = client.post('/gaodcore/resource-config/', {
#         "name": request.node.name,
#         "connector_config": connector_data.json()['id'],
#         "object_location": request.node.name})
#
#     assert view_response.status_code == 201
#
#
# @pytest.mark.django_db
# def test_connector_no_db(client, django_user_model, request, pg):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     uri = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/test"
#     response = client.post('/gaodcore/connector-config/', {"name": request.node.name, "uri": uri})
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# def test_connector_connect_error(client, django_user_model, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     uri = f"postgresql://error:error@localhost:1/guillotina"
#     response = client.post('/gaodcore/connector-config/', {"name": request.node.name, "uri": uri})
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# def test_connector_schema_error(client, django_user_model, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     uri = f"test://postgres:postgres@localhost:1/guillotina"
#     response = client.post('/gaodcore/connector-config/', {"name": request.node.name, "uri": uri})
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# def test_table_not_exists(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     uri = get_uri(*pg)
#     connector_data = create_connector_ga_od_core(client, request.node.name, uri)
#     response = client.post('/gaodcore/resource-config/', {
#         "name": request.node.name,
#         "enabled": True,
#         "connector_config": connector_data.json()['id'],
#         "object_location": 'fail'})
#     assert response.status_code == 400
#
#
# @pytest.mark.django_db
# @pytest.mark.parametrize("extension,", ["json", "xml", "csv", "yaml", "xlsx"])
# def test_view_extension_download(client, django_user_model, pg, request, extension):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, f"{request.node.originalname}_{extension}")
#     download_response = client.get(f'/gaodcore/download.{extension}', {'resource_id': view_response.json()['id']})
#
#     with open(os.path.join(os.path.dirname(__file__), f"data.{extension}"), r'rb') as f:
#         assert f.read() == download_response.content
#
#
# @pytest.mark.django_db
# def test_download_fields(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#     download_response = client.get(f'/gaodcore/download.json', {'resource_id': view_response.json()['id'], "fields": [
#         "id", "name"]})
#
#     assert download_response.json() == [{'id': 1, 'name': 'RX-78-2 Gundam'}, {'id': 2, 'name': 'Half Gundam'}]
#
#
# @pytest.mark.django_db
# def test_download_field(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#     download_response = client.get(f'/gaodcore/download.json', {'resource_id': view_response.json()['id'], "fields": [
#         "id"]})
#
#     assert download_response.json() == [{'id': 1}, {'id': 2}]
#
#
# @pytest.mark.django_db
# def test_download_format(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#
#     download_response = client.get(f'/gaodcore/download.xml', {'resource_id': view_response.json()['id'], 'formato': "csv"})
#
#     with open(os.path.join(os.path.dirname(__file__), f"data.csv"), r'rb') as f:
#         assert f.read() == download_response.content
#
#
# @pytest.mark.django_db
# def test_download_invalid_format(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#
#     download_response = client.get(f'/gaodcore/download.xml', {'resource_id': view_response.json()['id'], 'formato': "dj"})
#
#     assert download_response.status_code == 400
#
#
#
# @pytest.mark.django_db
# def test_download_offset(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#
#     download_response = client.get(f'/gaodcore/download.json', {'resource_id': view_response.json()['id'], 'offset': "1"})
#
#     with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
#         assert json.loads(f.read())[1:] == json.loads(download_response.content)
#
# @pytest.mark.django_db
# def test_download_limit(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.originalname)
#
#     download_response = client.get(f'/gaodcore/download.json', {'resource_id': view_response.json()['id'], 'limit': "1"})
#
#     with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
#         assert json.loads(f.read())[0] == json.loads(download_response.content)
#
#


@pytest.mark.django_db
def test_validator(client, django_user_model, pg, request):
    client = get_auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)
    view_data = view_response.json()
    download_response = client.get(f'/gaodcore/validator.json',
                                   {'object_location': view_data['object_location'], 'uri': get_uri(*pg)})
    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
def test_validator_object_location_fail(client, django_user_model, pg, request):
    client = get_auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)
    download_response = client.get(f'/gaodcore/validator.json',
                                   {'object_location': 'fail', 'uri': get_uri(*pg)})
    with open(os.path.join(os.path.dirname(__file__), f"data.json"), r'rb') as f:
        assert f.read() == download_response.content
#
#
# @pytest.mark.django_db
# def test_show_columns(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     view_response = create_full_example(client, *pg, request.node.name)
#     view_data = view_response.json()
#     download_response = client.get(f'/gaodcore/show_columns.json',
#                                    {'resource_id': view_data['id']})
#     assert download_response.json() == [{'COLUMN_NAME': 'id', 'DATA_TYPE': 'INTEGER'},
#                                         {'COLUMN_NAME': 'name', 'DATA_TYPE': 'VARCHAR'},
#                                         {'COLUMN_NAME': 'size', 'DATA_TYPE': 'BIGINT'},
#                                         {'COLUMN_NAME': 'max_acceleration', 'DATA_TYPE': 'DOUBLE_PRECISION'},
#                                         {'COLUMN_NAME': 'weight', 'DATA_TYPE': 'NUMERIC'},
#                                         {'COLUMN_NAME': 'description', 'DATA_TYPE': 'TEXT'},
#                                         {'COLUMN_NAME': 'discover_date', 'DATA_TYPE': 'DATE'},
#                                         {'COLUMN_NAME': 'destroyed_date', 'DATA_TYPE': 'TIMESTAMP'},
#                                         {'COLUMN_NAME': 'destroyed', 'DATA_TYPE': 'BOOLEAN'}]
#
#
# @pytest.mark.django_db
# def test_views(client, django_user_model, pg, request):
#     client = get_auth_client(client=client, django_user_model=django_user_model)
#     create_full_example(client, *pg, request.node.name)
#     download_response = client.get(f'/gaodcore/views.json')
#     assert download_response.json() == {'1': 'test_views'}
