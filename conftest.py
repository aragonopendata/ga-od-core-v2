import datetime
import io
import json
import os
from csv import DictReader
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from lxml import etree
import pandas
import pytest as pytest
import yaml
from _pytest.fixtures import FixtureRequest
from pytest_httpserver import HTTPServer
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    text,
    BigInteger,
    Float,
    Boolean,
    Date,
    DateTime,
)
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker
from pytest_docker_fixtures import images

DB_USERNAME = "username"
DB_PASSWORD = "password"
DB_NAME = "gaodcore"

images.configure(
    "mysql",
    "mysql",
    "5.7",
    env={
        "MYSQL_USER": DB_USERNAME,
        "MYSQL_PASSWORD": DB_PASSWORD,
        "MYSQL_DATABASE": DB_NAME,
        "MYSQL_ROOT_PASSWORD": "",
        "MYSQL_ALLOW_EMPTY_PASSWORD": "yes",
    },
)

images.configure(
    "postgresql",
    "postgres",
    "13",
    env={
        "POSTGRES_USER": DB_USERNAME,
        "POSTGRES_PASSWORD": DB_PASSWORD,
        "POSTGRES_DB": DB_NAME,
    },
)

pytest_plugins = ["pytest_docker_fixtures"]

USERNAME = "user"
PASSWORD = "password"

PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")


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
def auth_client(client, django_user_model):
    user = django_user_model.objects.create_user(
        username=USERNAME, password=PASSWORD, is_staff=True
    )
    client.force_login(user)
    return client


NON_STAFF_USERNAME = "non_staff_user"


@pytest.fixture
def non_staff_client(client, django_user_model):
    user = django_user_model.objects.create_user(
        username=NON_STAFF_USERNAME, password=PASSWORD, is_staff=False
    )
    client.force_login(user)
    return client


def create_connector_ga_od_core(client, test_name: str, uri: str) -> ConnectorData:
    data = client.post('/admin/GA_OD_Core_admin/manager/connector-config/', {
        "name": test_name,
        "enabled": True,
        "uri": uri
    }).json()
    return ConnectorData(**{**data, "uri": uri})


def create_table_view(uri: str, test_name: str):
    engine = create_engine(uri, echo=False)

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
            conn.execute(text(f"CREATE VIEW {test_name}_view AS SELECT * FROM {test_name};"))
            conn.commit()
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

    data = client.post('/admin/GA_OD_Core_admin/manager/resource-config/', data).json()

    return ResourceData(**data)


@pytest.fixture
def full_example(auth_client, connector_uri: str, request) -> ConnectorData:
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


PROBLEM_CONTENT_TYPE = "application/problem+json"
PROBLEM_TYPE_BASE = "https://opendata.aragon.es/problems/"
NON_FIELD_ERRORS_KEY = "non_field_errors"
VALIDATION_ERROR_CODE = "VALIDATION_ERROR"
VALIDATION_ERROR_DETAIL = "The request contains invalid fields."


def problem_of(response) -> dict:
    """Assert that ``response`` carries a well formed problem document and return it.

    Errors are always served as ``application/problem+json``, whatever data format the
    client asked for, so the body is never CSV/SCSV/XML/YAML/XLSX. The only exception is
    the browsable API, which keeps rendering HTML.
    """
    assert response["Content-Type"] == PROBLEM_CONTENT_TYPE, response["Content-Type"]

    problem = json.loads(response.content)
    assert isinstance(problem, dict)

    # The envelope is complete and its status agrees with the real HTTP status.
    assert set(problem) >= {"type", "title", "status", "detail", "error_code"}
    assert problem["status"] == response.status_code
    assert isinstance(problem["detail"], str)

    # "type" and "title" are derived from the stable error_code.
    error_code = problem["error_code"]
    assert error_code == error_code.upper()
    assert problem["type"] == PROBLEM_TYPE_BASE + error_code.lower().replace("_", "-")
    assert problem["title"] == error_code.replace("_", " ").capitalize()

    if "errors" in problem:
        assert isinstance(problem["errors"], dict)

    return problem


def field_messages(problem: dict, error_field: Optional[str] = None) -> list:
    """Return the messages reported for ``error_field`` (or the non-field ones)."""
    return [
        item["message"]
        for item in problem["errors"][error_field or NON_FIELD_ERRORS_KEY]
    ]


def field_codes(problem: dict, error_field: Optional[str] = None) -> list:
    """Return the validation codes reported for ``error_field`` (or the non-field ones)."""
    return [
        item["code"] for item in problem["errors"][error_field or NON_FIELD_ERRORS_KEY]
    ]


def validate_error(response,
                   error_description: str,
                   mime_type: str,
                   error_field: Optional[str] = None,
                   error_code: Optional[str] = None,
                   field_error_code: Optional[str] = None):
    """Assert the shared error contract for an API error response.

    @param response: the Django test client response.
    @param error_description: the human readable message the caller expects.
    @param mime_type: the media type the request asked for.
    @param error_field: field the message belongs to, for validation errors.
    @param error_code: expected top level ``error_code``, when the caller pins it.
    @param field_error_code: expected semantic code of the entry inside ``errors``.
        For DRF ``ValidationError`` the top level code is always ``VALIDATION_ERROR``,
        so the meaningful code lives under ``errors[<field>][*].code``. It is checked
        against the entry carrying ``error_description``, which also guarantees the
        code did not regress to the generic ``INVALID`` or to a bare HTTP status.
    """
    if mime_type == 'text/html':
        # The browsable API keeps rendering HTML for humans.
        assert response['Content-Type'].startswith('text/html')
        assert response.content
        return

    problem = problem_of(response)

    if error_code:
        assert problem["error_code"] == error_code

    if field_error_code:
        # The semantic code belongs to the very entry holding the expected message.
        assert problem["error_code"] == VALIDATION_ERROR_CODE
        entries = problem["errors"][error_field or NON_FIELD_ERRORS_KEY]
        matching = [item for item in entries if item["message"] == error_description]
        assert matching, (error_description, entries)
        assert [item["code"] for item in matching] == [field_error_code] * len(matching)

    if error_field is not None:
        # Field errors live under "errors", never inside "detail".
        assert problem["error_code"] == VALIDATION_ERROR_CODE
        assert problem["detail"] == VALIDATION_ERROR_DETAIL
        assert error_description in field_messages(problem, error_field)
    elif problem["error_code"] == VALIDATION_ERROR_CODE:
        assert problem["detail"] == VALIDATION_ERROR_DETAIL
        assert error_description in field_messages(problem)
    else:
        # Non-validation problems carry the message in "detail" and have no "errors".
        assert problem["detail"] == error_description
        assert "errors" not in problem


def compare_files(directory: str, file_without_extension, mimetype: str, content: bytes):
    if mimetype == 'text/html':
        assert content
    elif mimetype in ['application/json', 'text/csv', 'application/xml', 'application/yaml', 'application/xlsx']:
        url = os.path.join(directory, f"{file_without_extension}.{mimetype.split('/')[1]}")
        if mimetype in ['application/xlsx', 'application/xml']:
            with open(url, 'rb') as f:
                if mimetype == 'application/xlsx':
                    test = pandas.read_excel(f, engine='openpyxl')
                    test_data = test.where(pandas.notnull(test), None).to_dict(orient='records')
                    response = pandas.read_excel(io.BytesIO(content), engine='openpyxl')
                    response_data = response.where(pandas.notnull(response), None).to_dict(orient='records')

                    # Custom comparison to handle NaN values properly
                    import math
                    def compare_xlsx_records(expected, actual):
                        if len(expected) != len(actual):
                            return False
                        for exp_record, act_record in zip(expected, actual):
                            if set(exp_record.keys()) != set(act_record.keys()):
                                return False
                            for key in exp_record.keys():
                                exp_val = exp_record[key]
                                act_val = act_record[key]
                                # Handle NaN comparison
                                if exp_val is None and act_val is None:
                                    continue
                                elif isinstance(exp_val, float) and isinstance(act_val, float):
                                    if math.isnan(exp_val) and math.isnan(act_val):
                                        continue
                                    elif exp_val != act_val:
                                        return False
                                elif exp_val != act_val:
                                    return False
                        return True

                    assert compare_xlsx_records(test_data, response_data), f"XLSX data mismatch:\nExpected: {test_data}\nActual: {response_data}"
                elif mimetype == 'application/xml':
                    parser = etree.XMLParser(remove_blank_text=True)
                    assert etree.tostring(
                        etree.XML(f.read(), parser=parser)
                    ) == etree.tostring(etree.XML(content, parser=parser))
                else:
                    raise NotImplementedError
        else:
            with open(url, "r") as f:
                if mimetype == "application/json":
                    assert json.loads(f.read()) == json.loads(content)
                elif mimetype == "text/csv":
                    assert [row for row in DictReader(f)] == [
                        row for row in DictReader(io.StringIO(content.decode()))
                    ]
                elif mimetype == "application/yaml":
                    assert yaml.load(f.read(), Loader=yaml.SafeLoader) == yaml.load(
                        content, Loader=yaml.SafeLoader
                    )
                else:
                    raise NotImplementedError
    else:
        raise NotImplementedError


def connector_uri_api_get_url(
    httpserver: HTTPServer, request: FixtureRequest, filepath: str, content_type: str
) -> str:
    url = "/" + request.node.originalname
    with open(filepath, "rb") as f:
        httpserver.expect_request(url).respond_with_data(
            f.read(), content_type=content_type
        )
    return httpserver.url_for(url)


def get_uri(schema: str, _: str, port: int):
    return f"{schema}://{DB_USERNAME}:{DB_PASSWORD}@127.0.0.1:{port}/{DB_NAME}"


@pytest.fixture(params=["postgresql", "mysql", "api-json"])
def connector_uri(request, pg, mysql, httpserver: HTTPServer):
    if request.param in ["postgresql", "mysql"]:
        conf = mysql if request.param == "mysql" else pg
        return get_uri(request.param, *conf)
    elif request.param == "mysql":
        return f"mysql://{DB_USERNAME}:{DB_PASSWORD}@127.0.0.1:{mysql[1]}/{DB_NAME}"
    elif request.param == "api-json":
        path = os.path.join(
            PROJECT_DIR, "gaodcore", "tests", "download_postgresql.json"
        )
        return connector_uri_api_get_url(
            httpserver, request, path, "application/json; charset=utf-8"
        )
    else:
        raise NotImplementedError


@pytest.fixture(
    params=[
        "text/html",
        "application/json",
        "text/csv",
        "text/scsv",
        "application/xml",
        "application/yaml",
        "application/xlsx",
    ]
)
def accept_error(request):
    """Every data format a client may ask for. Errors answer problem+json for all but HTML."""
    return request.param


@pytest.fixture(
    params=[
        "text/html",
        "application/json",
        "text/csv",
        "application/xml",
        "application/yaml",
        "application/xlsx",
    ]
)
def accept_download(request):
    return request.param
