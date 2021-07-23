import os

import pytest
from _pytest.fixtures import FixtureRequest
from pytest_httpserver import HTTPServer

from conftest import PROJECT_DIR
from src.connectors import _get_engine, MimeTypeError


def _get_data_url(httpserver: HTTPServer, request: FixtureRequest, filepath: str, content_type: str) -> str:
    url = '/' + request.node.originalname
    with open(filepath, 'rb') as f:
        httpserver.expect_request(url).respond_with_data(f.read(), content_type=content_type)
    return httpserver.url_for(url)


def test_get_engine_csv_with_charset(httpserver: HTTPServer, request: FixtureRequest):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download.csv')
    _get_engine(_get_data_url(httpserver, request, path, 'text/csv; charset=utf-8'))


def test_get_engine_csv_without_charset(httpserver: HTTPServer, request: FixtureRequest):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download.csv')
    _get_engine(_get_data_url(httpserver, request, path, 'text/csv'))


def test_get_engine_excel(httpserver: HTTPServer, request: FixtureRequest):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download.xlsx')
    _get_engine(_get_data_url(httpserver, request, path, 'application/xlsx'))


def test_get_engine_not_allowed_content_type_error(httpserver: HTTPServer, request: FixtureRequest):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download.json')
    with pytest.raises(MimeTypeError):
        _get_engine(_get_data_url(httpserver, request, path, 'application/json'))


def test_get_engine_xlsx_bad_content_type_error(httpserver: HTTPServer, request: FixtureRequest):
    path = os.path.join(PROJECT_DIR, 'gaodcore', 'tests', 'download.json')
    with pytest.raises(MimeTypeError):
        _get_engine(_get_data_url(httpserver, request, path, 'application/xlsx'))
