import pytest

from gaodcore.tests.helpers import get_auth_client, get_uri, create_connector_ga_od_core


@pytest.mark.django_db
def test_resource_config_error(client, django_user_model, pg, request):
    client = get_auth_client(client=client, django_user_model=django_user_model)
    uri = get_uri(*pg)
    connector_data = create_connector_ga_od_core(client, request.node.name, uri)
    response = client.post(
        '/gaodcore/resource-config/', {
            "name": request.node.name,
            "enabled": True,
            "connector_config": connector_data.json()['id'],
            "object_location": 'fail'
        })
    assert response.status_code == 400
    assert response.json() == {'non_field_errors': ['Object "fail" is not available.']}
