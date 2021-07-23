import pytest

from django.test.client import Client

from conftest import ConnectorData


@pytest.mark.django_db
def test_resources(client: Client, full_example):
    download_response = client.get(f'/GA_OD_Core/views.json')
    assert download_response.json() == [{'available': True, 'id': 1, 'name': 'test_resources'}]
