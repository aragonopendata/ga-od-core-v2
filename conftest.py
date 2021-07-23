import datetime
import json
import os

import pytest as pytest
from _pytest.fixtures import FixtureRequest
from django.db.models import Model
from django.test import Client
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Float, Numeric, Boolean, Date, DateTime, Text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, sessionmaker

pytest_plugins = ['pytest_docker_fixtures']

USERNAME = "user"
PASSWORD = "password"

PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')


def auth_client(client: Client, django_user_model: Model) -> Client:
    user = django_user_model.objects.create_user(username=USERNAME, password=PASSWORD)
    client.force_login(user)

    return client


@pytest.fixture
def auth_client_fixture(client: Client, django_user_model: Model) -> Client:
    return auth_client(client=client, django_user_model=django_user_model)


def get_uri(host: str, port: str) -> str:
    return f"postgresql://postgres:@{host}:{port}/guillotina"


def create_connector_ga_od_core(client: Client, test_name: str, uri: str):
    return client.post('/GA_OD_Core_admin/manager/connector-config/', {
        "name": test_name,
        "enabled": True,
        "uri": uri
    })


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

    try:
        session.bulk_save_objects([
            TestData(name='RX-78-2 Gundam',
                     size=18,
                     max_acceleration=0.93,
                     weight=60.0,
                     description='The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series',
                     discover_date=datetime.date(79, 9, 18),
                     destroyed_date=datetime.datetime(79, 12, 31, 12, 1, 1),
                     destroyed=True),
            TestData(name='Half Gundam', )])
        session.commit()
    except IntegrityError:
        # In cases that is using parametrize in pytest, it try to duplicate rows
        pass

    session.close()


def create_view(client, test_name: str, connector_data):
    return client.post('/GA_OD_Core_admin/manager/resource-config/', {
        "name": test_name,
        "enabled": True,
        "connector_config": connector_data.json()['id'],
        "object_location": test_name
    })


def create_full_example(auth_client, pg, request: FixtureRequest):
    uri = get_uri(*pg)
    connector_data = create_connector_ga_od_core(auth_client, request.node.originalname, uri)
    create_table(uri, request.node.originalname)
    return create_view(auth_client, request.node.originalname, connector_data)


@pytest.fixture
def create_full_example_postgresql_fixture(auth_client_fixture, pg, request):
    return create_full_example(auth_client_fixture, pg, request)


def validate_error(content_error: bytes, error_description: str, mime_type: str):
    if mime_type == 'text/html':
        assert content_error
    elif mime_type == 'application/json':
        assert content_error == json.dumps([error_description]).encode()
    elif mime_type == 'text/csv':
        if ',' in error_description:
            error_description = f'"{error_description}"'
        assert content_error == f'""\r\n{error_description}\r\n'.encode()
    elif mime_type == 'application/xml':
        error = f'<?xml version="1.0" encoding="utf-8"?>\n<root><list-item>{error_description}</list-item></root>'
        assert content_error == error.encode()
    else:
        raise NotImplementedError
