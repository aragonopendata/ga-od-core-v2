import os

import pytest

import connectors
from gaodcore.tests.helpers import auth_client, create_full_example, get_uri


@pytest.mark.django_db
def test_validator(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)
    view_data = view_response.json()
    download_response = client.get(f'/GA_OD_Core/gaodcore-manager/validator.json', {
        'object_location': view_data['object_location'],
        'uri': get_uri(*pg)
    })
    with open(os.path.join(os.path.dirname(__file__), f"../../gaodcore/tests/data.json"), r'rb') as f:
        assert f.read() == download_response.content


@pytest.mark.django_db
def test_validator_object_location_error(client, django_user_model, pg):
    client = auth_client(client=client, django_user_model=django_user_model)
    download_response = client.get(f'/GA_OD_Core/gaodcore-manager/validator.json', {
        'object_location': 'fail',
        'uri': get_uri(*pg)
    })
    assert download_response.json() == ['Object "fail" is not available.']
    assert download_response.status_code == 400


@pytest.mark.django_db
def test_validator_object_database_error(client, django_user_model, pg):
    client = auth_client(client=client, django_user_model=django_user_model)
    download_response = client.get(f'/GA_OD_Core/gaodcore-manager/validator.json', {
        'object_location': 'fail',
        'uri': get_uri(*pg)
    })
    assert download_response.json() == ['Object "fail" is not available.']


@pytest.mark.django_db
def test_validator_too_many_rows(client, django_user_model, pg, request, mocker):
    client = auth_client(client=client, django_user_model=django_user_model)
    view_response = create_full_example(client, *pg, request.node.name)
    view_data = view_response.json()
    mocker.patch.object(connectors, '_RESOURCE_MAX_ROWS', 1)
    download_response = client.get(f'/GA_OD_Core/gaodcore-manager/validator.json', {
        'object_location': view_data['object_location'],
        'uri': get_uri(*pg)
    })
    assert download_response.json() == ['This resource have too many rows. For security reason this is not allowed.']
