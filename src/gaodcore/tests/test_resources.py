import pytest

from gaodcore.tests.helpers import auth_client, create_full_example


@pytest.mark.django_db
def test_resources(client, django_user_model, pg, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    create_full_example(client, *pg, request.node.originalname)
    download_response = client.get(f'/GA_OD_Core/views.json')

    assert download_response.json() == [{'available': True, 'resource_id': 1, 'resource_name': 'test_resources'}]
