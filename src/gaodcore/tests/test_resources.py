import pytest

from django.test.client import Client
from rest_framework.response import Response


@pytest.mark.django_db
def test_resources(auth_client_fixture: Client, create_full_example_fixture: Response):
    download_response = auth_client_fixture.get(f'/GA_OD_Core/views.json')

    assert download_response.json() == [{'available': True, 'resource_id': 1, 'resource_name': 'test_resources'}]
