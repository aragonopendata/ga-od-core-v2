import pytest
from _pytest.fixtures import FixtureRequest
from pytest_httpserver import HTTPServer

from src.connectors import _get_engine, TypeDocumentError


def _get_data_url(httpserver: HTTPServer, request: FixtureRequest, filepath: str, content_type: str) -> str:
    url = '/' + request.node.originalname
    with open(filepath, 'rb') as f:
        httpserver.expect_request(url).respond_with_data(f.read(), content_type=content_type)
    return httpserver.url_for(url)


def test_get_engine_csv_with_charset(httpserver: HTTPServer, request: FixtureRequest):
    _get_engine(_get_data_url(httpserver, request, '../../gaodcore/tests/data.csv', 'text/csv; charset=utf-8'))


def test_get_engine_csv_without_charset(httpserver: HTTPServer, request: FixtureRequest):
    _get_engine(_get_data_url(httpserver, request, '../../gaodcore/tests/data.csv', 'text/csv'))


def test_get_engine_excel(httpserver: HTTPServer, request: FixtureRequest):
    _get_engine(_get_data_url(httpserver, request, '../../gaodcore/tests/data.xlsx', 'application/xlsx'))


def test_get_engine_not_allowed_content_type_error(httpserver: HTTPServer, request: FixtureRequest):
    with pytest.raises(TypeDocumentError):
        _get_engine(_get_data_url(httpserver, request, '../../gaodcore/tests/data.json', 'application/json'))


def test_get_engine_xlsx_bad_content_type_error(httpserver: HTTPServer, request: FixtureRequest):
    with pytest.raises(TypeDocumentError):
        _get_engine(_get_data_url(httpserver, request, '../../gaodcore/tests/data.json', 'application/xlsx'))
