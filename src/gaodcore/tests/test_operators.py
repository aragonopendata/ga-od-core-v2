import pytest
from django.test import RequestFactory
from django.test.client import Client
from rest_framework.request import Request
from sqlalchemy.sql.elements import TextClause

from connectors import _get_filter_operators
from gaodcore.operators import get_function_for_operator
from gaodcore.views import DownloadView


@pytest.fixture()
def download_view():
    return DownloadView()


def test_gt(download_view):
    factory = RequestFactory()
    django_request = factory.get('/some/path', {'filters': '{ "key1": { "$gt": 10 } }'})
    request = Request(django_request)
    assert DownloadView._get_filters(request) == {"key1": {"$gt": 10}}


class TestTranslateFilter:
    def test_translate_filter_gt(self):
        filter_field = {"key1": {"$gt": 10}}
        filter = filter_field["key1"]
        result = get_function_for_operator('$gt')("key1", filter)
        assert isinstance(result, TextClause)
        assert result.text == 'key1 > 10'

    def test_translate_filter_lt(self):
        filter_field = {"key1": {"$lt": 10}}
        filter = filter_field["key1"]
        result = get_function_for_operator('$lt')("key1", filter)
        assert isinstance(result, TextClause)
        assert result.text == 'key1 < 10'

    def test_translate_filter_eq(self):
        filter_field = {"key1": {"$eq": 10}}
        filter = filter_field["key1"]
        result = get_function_for_operator('$eq')("key1", filter)
        assert isinstance(result, TextClause)
        assert result.text == 'key1 = 10'

class TestGetFilterOperators:
    def test_get_filter_operators_gt(self):
        filters = {"key1": 2, "key2": {"$gt": 10}}
        filters_args: list = []

        filters, filters_args = _get_filter_operators(filters, filters_args)
        assert filters == {"key1": 2}

        assert len(filters_args) == 1
        assert filters_args[0] == {'key2': {'$gt': 10}}


# @pytest.fixture(params=["/GA_OD_Core/download", "/GA_OD_Core/preview"])
@pytest.fixture(params=["/GA_OD_Core/download"])
def endpoint(request):
    return request.param


class TestDownloadViewWithOperators():
    @pytest.mark.django_db
    def test_download_view_without_operator(self, endpoint: str, client: Client, full_example):
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "filters": '{ "id": 1}'})
        response = download_response.json()
        response.sort(key=lambda item: item['id'])
        assert response == [{'id': 1}]

    @pytest.mark.django_db
    def test_download_view_id_with_gt_filter(self, endpoint: str, client: Client, full_example):
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "filters": '{ "id": { "$gt": 1 } }'})
        response = download_response.json()
        response.sort(key=lambda item: item['id'])
        assert response == [{'id': 2}]

    @pytest.mark.django_db
    def test_download_view_id_with_lt_filter(self, endpoint: str, client: Client, full_example):
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "filters": '{ "id": { "$lt": 2 } }'})
        response = download_response.json()
        response.sort(key=lambda item: item['id'])
        assert response == [{'id': 1}]

    @pytest.mark.django_db
    def test_download_view_id_with_invalid_filter(self, endpoint: str, client: Client, full_example):
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "filters": '{ "id": { "$invalid": 2 } }'})
        assert download_response.status_code == 400

    @pytest.mark.django_db
    def test_download_view_id_with_like_filter(self, endpoint: str, client: Client, full_example):
        """Test download view with like filter, filter name with RX, like in 'RX-78-2 Gundam'"""
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "like": '{ "name": "RX"}'})
        response = download_response.json()
        response.sort(key=lambda item: item['id'])
        assert response == [{'id': 1}]

    @pytest.mark.django_db
    def test_download_view_id_with_like_wrong_filter(self, endpoint: str, client: Client, full_example):
        """Test download view with like filter, filter with wrong name, expecting empty result"""
        download_response = client.get(endpoint, {'view_id': full_example.resources.table.id,
                                                  "fields": ["id"],
                                                  "like": '{ "name": "AEIOU"}'})
        response = download_response.json()
        assert len(response) == 0