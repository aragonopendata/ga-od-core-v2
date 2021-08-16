import pytest

from django.test.client import Client


@pytest.mark.django_db
def test_resources(client: Client, full_example):
    download_response = client.get('/GA_OD_Core/views.json')
    if full_example.scheme in ['http', 'https']:
        assert download_response.json() == [{'available': True, 'id': 1, 'name': 'test_resources'}]
    else:
        assert download_response.json() == [{
            'available': True,
            'id': 1,
            'name': 'test_resources_view'
        }, {
            'available': True,
            'id': 2,
            'name': 'test_resources'
        }]
