import pytest

from gaodcore.tests.helpers import auth_client


@pytest.mark.django_db
def test_connector_config_db_error(client, django_user_model, request, pg):
    client = auth_client(client=client, django_user_model=django_user_model)
    uri = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/test"
    response = client.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Connection is not available.']}


@pytest.mark.django_db
def test_connector_config_connect_error(client, django_user_model, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    uri = f"postgresql://error:error@localhost:1/guillotina"
    response = client.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Connection is not available.']}


@pytest.mark.django_db
def test_connector_config_schema_error(client, django_user_model, request):
    client = auth_client(client=client, django_user_model=django_user_model)
    uri = f"test://postgres:postgres@localhost:1/guillotina"
    response = client.post('/GA_OD_Core_admin/manager/connector-config/', {"name": request.node.name, "uri": uri})
    assert response.status_code == 400
    assert response.json() == {'uri': ['Schema: "test" is not implemented.']}
