import datetime
import io
import json
import os
from csv import DictReader
from dataclasses import dataclass, field
from select import select
from typing import Optional, Tuple
from urllib.parse import urlparse

from django.views.generic import CreateView
from lxml import etree
import pandas
import pytest as pytest
import yaml
from _pytest.fixtures import FixtureRequest
from django.db.models import Model
from django.test import Client
from pytest_httpserver import HTTPServer
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Float, Boolean, Date, DateTime
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker
from pytest_docker_fixtures import images

DB_USERNAME = 'username'
DB_PASSWORD = 'password'
DB_NAME = 'gaodcore'

images.configure('mysql',
                 'mysql',
                 '8',
                 env={
                     'MYSQL_USER': DB_USERNAME,
                     'MYSQL_PASSWORD': DB_PASSWORD,
                     'MYSQL_DATABASE': DB_NAME
                 })

images.configure('postgresql',
                 'postgres',
                 '13',
                 env={
                     'POSTGRES_USER': DB_USERNAME,
                     'POSTGRES_PASSWORD': DB_PASSWORD,
                     'POSTGRES_DB': DB_NAME
                 })

pytest_plugins = ['pytest_docker_fixtures']

USERNAME = "user"
PASSWORD = "password"

PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')


@dataclass
class ResourceData:
    id: int
    name: str
    enabled: bool
    object_location: Optional[str]
    object_location_schema: Optional[str]
    created_at: str
    updated_at: str
    connector_config: int


@dataclass
class Resources:
    table: Optional[ResourceData] = None
    view: Optional[ResourceData] = None


@dataclass
class ConnectorData:
    id: int
    name: str
    uri: str
    enabled: bool
    created_at: str
    updated_at: str
    resources: Resources = field(default_factory=Resources)

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme


@pytest.fixture
def auth_client(client: Client, django_user_model: Model) -> Client:
    user = django_user_model.objects.create_user(username=USERNAME, password=PASSWORD)
    client.force_login(user)
    return client


def get_uri(host: str, port: str) -> str:
    return f"postgresql://postgres:@{host}:{port}/guillotina"


def create_connector_ga_od_core(client: Client, test_name: str, uri: str) -> ConnectorData:
    data = client.post('/GA_OD_Core_admin/manager/connector-config/', {
        "name": test_name,
        "enabled": True,
        "uri": uri
    }).json()
    return ConnectorData(**data)


def create_table_view(uri: str, test_name: str):
    engine = create_engine(uri, echo=True)

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class TestData(Base):
        __tablename__ = test_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(length=500), unique=True)
        size = Column(BigInteger)
        max_acceleration = Column(Float)
        weight = Column(Float)
        description = Column(String(length=500))
        discover_date = Column(Date)
        destroyed_date = Column(DateTime)
        destroyed = Column(Boolean)
        empty = Column(String(length=500))

    Base.metadata.create_all(engine)

    try:
        with engine.connect() as conn:
            conn.execute(f"CREATE VIEW {test_name}_view AS SELECT * FROM {test_name};")
    except (OperationalError, ProgrammingError):
        # In cases that is using parametrize in pytest, it try to duplicate views
        pass

    session = Session()

    try:
        session.bulk_save_objects([
            TestData(
                name='RX-78-2 Gundam',
                size=18,
                max_acceleration=0.93,
                weight=60.0,
                description='The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series',
                discover_date=datetime.date(79, 9, 18),
                destroyed_date=datetime.datetime(79, 12, 31, 12, 1, 1),
                destroyed=True),
            TestData(name='Half Gundam', )
        ])
        session.commit()

    except IntegrityError:
        # In cases that is using parametrize in pytest, it try to duplicate rows
        pass

    session.close()


def create_resource_table_view(client, test_name: str, table_name: Optional[str],
                               connector_data: ConnectorData) -> ResourceData:
    data = {
        "name": test_name,
        "enabled": True,
        "connector_config": connector_data.id,
    }
    if table_name:
        data["object_location"] = test_name

    data = client.post('/GA_OD_Core_admin/manager/resource-config/', data).json()

    return ResourceData(**data)


@pytest.fixture
def full_example(auth_client: Client, connector_uri: str, request) -> ConnectorData:
    connector_data = create_connector_ga_od_core(auth_client, request.node.originalname, connector_uri)
    parsed_url = urlparse(connector_uri)
    if parsed_url.scheme in ['postgresql', 'mysql']:
        table_name = request.node.originalname
        create_table_view(connector_uri, request.node.originalname)
        connector_data.resources.view = create_resource_table_view(auth_client, f"{request.node.originalname}_view",
                                                                   f"{request.node.originalname}_view", connector_data)
    elif parsed_url.scheme in ['http', 'https']:
        table_name = None
    else:
        raise NotImplementedError
    connector_data.resources.table = create_resource_table_view(auth_client, request.node.originalname, table_name,
                                                                connector_data)

    return connector_data


def validate_error(content_error: bytes, error_description: str, mime_type: str, error_field: Optional[str] = None):
    if mime_type == 'text/html':
        assert content_error
    elif mime_type == 'application/json':
        if error_field:
            error = {error_field: [error_description]}
        else:
            error = [error_description]
        assert json.loads(content_error) == error
    elif mime_type == 'text/csv':
        if ',' in error_description:
            error_description = f'"{error_description}"'
        if error_field:
            error_field = f'{error_field}.0'
        else:
            error_field = '""'
        assert content_error == f'{error_field}\r\n{error_description}\r\n'.encode()
    elif mime_type == 'application/xml':
        if error_field:
            error = f'<?xml version="1.0" encoding="utf-8"?>\n<root><{error_field}><list-item>{error_description}' \
                    f'</list-item></{error_field}></root>'
        else:
            error = f'<?xml version="1.0" encoding="utf-8"?>\n<root><list-item>{error_description}</list-item></root>'
        assert content_error == error.encode()
    else:
        raise NotImplementedError


def compare_files(directory: str, file_without_extension, mimetype: str, content: bytes):
    if mimetype == 'text/html':
        assert content
    elif mimetype in ['application/json', 'text/csv', 'application/xml', 'application/yaml', 'application/xlsx']:
        url = os.path.join(directory, f"{file_without_extension}.{mimetype.split('/')[1]}")
        if mimetype in ['application/xlsx', 'application/xml']:
            with open(url, 'rb') as f:
                if mimetype == 'application/xlsx':
                    test = pandas.read_excel(f)
                    test_data = test.where(pandas.notnull(test), None).to_dict(orient='records')
                    response = pandas.read_excel(io.BytesIO(content))
                    response_data = response.where(pandas.notnull(response), None).to_dict(orient='records')
                    assert test_data == response_data
                elif mimetype == 'application/xml':
                    parser = etree.XMLParser(remove_blank_text=True)
                    assert etree.tostring(etree.XML(f.read(),
                                                    parser=parser)) == etree.tostring(etree.XML(content, parser=parser))
                else:
                    raise NotImplementedError
        else:
            with open(url, 'r') as f:
                if mimetype == 'application/json':
                    assert json.loads(f.read()) == json.loads(content)
                elif mimetype == 'text/csv':
                    assert [row for row in DictReader(f)] == [row for row in DictReader(io.StringIO(content.decode()))]
                elif mimetype == 'application/yaml':
                    assert yaml.load(f.read(), Loader=yaml.SafeLoader) == yaml.load(content, Loader=yaml.SafeLoader)
                else:
                    raise NotImplementedError
    else:
        raise NotImplementedError


def connector_uri_api_get_url(httpserver: HTTPServer, request: FixtureRequest, filepath: str, content_type: str) -> str:
    url = '/' + request.node.originalname
    with open(filepath, 'rb') as f:
        httpserver.expect_request(url).respond_with_data(f.read(), content_type=content_type)
    return httpserver.url_for(url)


def get_uri(schema: str, _: str, port: int):
    return f"{schema}://{DB_USERNAME}:{DB_PASSWORD}@127.0.0.1:{port}/{DB_NAME}"


@pytest.fixture(params=['postgresql', 'mysql', 'api-json'])
def connector_uri(request, pg, mysql, httpserver: HTTPServer):
    if request.param in ['postgresql', 'mysql']:
        conf = mysql if request.param == 'mysql' else pg
        return get_uri(request.param, *conf)
    elif request.param == 'mysql':
        return f"mysql://{DB_USERNAME}:{DB_PASSWORD}@127.0.0.1:{mysql[1]}/{DB_NAME}"
    elif request.param == 'api-json':
        path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download_postgresql.json')
        return connector_uri_api_get_url(httpserver, request, path, 'application/json; charset=utf-8')
    else:
        raise NotImplementedError


@pytest.fixture(params=[
    'text/html',
    'application/json',
    'text/csv',
    'application/xml',
])
def accept_error(request):
    return request.param


@pytest.fixture(
    params=['text/html', 'application/json', 'text/csv', 'application/xml', 'application/yaml', 'application/xlsx'])
def accept_download(request):
    return request.param
