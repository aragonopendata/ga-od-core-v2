"""REST permission tests for the Manager API.

Only staff users (IsAdminUser) may use the connector-config, resource-config
and validator endpoints. A plain authenticated non-staff user must be
rejected, and the `uri` field (which embeds credentials) must never be
returned in responses.
"""

from urllib.parse import urlencode

import pytest
from django.test import Client

from gaodcore_manager.models import ConnectorConfig, ResourceConfig

CONNECTOR_URL = "/admin/GA_OD_Core_admin/manager/connector-config/"
RESOURCE_URL = "/admin/GA_OD_Core_admin/manager/resource-config/"
VALIDATOR_URL = "/admin/GA_OD_Core_admin/manager/validator"

# The API only enables the form parsers, so writes must be form encoded.
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"

POSTGRESQL_URI = "postgresql://user:s3cr3t@example.invalid:5432/gaodcore"


def _create_connector(name: str, uri: str = POSTGRESQL_URI, enabled: bool = True) -> ConnectorConfig:
    return ConnectorConfig.objects.create(name=name, uri=uri, enabled=enabled)


def _create_resource(name: str, connector: ConnectorConfig, enabled: bool = True) -> ResourceConfig:
    return ResourceConfig.objects.create(
        name=name,
        connector_config=connector,
        enabled=enabled,
        object_location="table",
    )


@pytest.mark.django_db
class TestConnectorConfigPermissions:
    def test_non_staff_list_forbidden(self, non_staff_client: Client):
        _create_connector("conn-list")
        response = non_staff_client.get(CONNECTOR_URL)
        assert response.status_code == 403

    def test_non_staff_retrieve_forbidden(self, non_staff_client: Client):
        connector = _create_connector("conn-retrieve")
        response = non_staff_client.get(f"{CONNECTOR_URL}{connector.id}/")
        assert response.status_code == 403

    def test_non_staff_create_forbidden(self, non_staff_client: Client):
        response = non_staff_client.post(
            CONNECTOR_URL,
            {"name": "conn-create", "uri": POSTGRESQL_URI, "enabled": True},
        )
        assert response.status_code == 403
        assert not ConnectorConfig.objects.filter(name="conn-create").exists()

    def test_non_staff_put_forbidden(self, non_staff_client: Client):
        connector = _create_connector("conn-put")
        response = non_staff_client.put(
            f"{CONNECTOR_URL}{connector.id}/",
            urlencode(
                {"name": "conn-put-renamed", "uri": POSTGRESQL_URI, "enabled": True}
            ),
            content_type=FORM_CONTENT_TYPE,
        )
        assert response.status_code == 403
        connector.refresh_from_db()
        assert connector.name == "conn-put"

    def test_non_staff_patch_does_not_persist(self, non_staff_client: Client):
        connector = _create_connector("conn-patch", enabled=True)
        response = non_staff_client.patch(
            f"{CONNECTOR_URL}{connector.id}/",
            urlencode({"enabled": False}),
            content_type=FORM_CONTENT_TYPE,
        )
        assert response.status_code == 403
        connector.refresh_from_db()
        assert connector.enabled is True

    def test_non_staff_delete_forbidden(self, non_staff_client: Client):
        connector = _create_connector("conn-delete")
        response = non_staff_client.delete(f"{CONNECTOR_URL}{connector.id}/")
        assert response.status_code == 403
        assert ConnectorConfig.objects.filter(id=connector.id).exists()

    def test_anonymous_list_rejected(self, client: Client):
        response = client.get(CONNECTOR_URL)
        assert response.status_code in (401, 403)

    def test_staff_list_ok(self, auth_client: Client):
        _create_connector("conn-staff-list")
        response = auth_client.get(CONNECTOR_URL)
        assert response.status_code == 200

    def test_staff_list_does_not_expose_uri(self, auth_client: Client):
        _create_connector("conn-secret", uri=POSTGRESQL_URI)
        response = auth_client.get(CONNECTOR_URL)
        assert response.status_code == 200
        payload = response.json()
        results = payload["results"] if isinstance(payload, dict) and "results" in payload else payload
        assert results
        for item in results:
            assert "uri" not in item
        assert "s3cr3t" not in response.content.decode()


@pytest.mark.django_db
class TestResourceConfigPermissions:
    def test_non_staff_list_forbidden(self, non_staff_client: Client):
        connector = _create_connector("res-conn-list")
        _create_resource("res-list", connector)
        response = non_staff_client.get(RESOURCE_URL)
        assert response.status_code == 403

    def test_non_staff_retrieve_forbidden(self, non_staff_client: Client):
        connector = _create_connector("res-conn-retrieve")
        resource = _create_resource("res-retrieve", connector)
        response = non_staff_client.get(f"{RESOURCE_URL}{resource.id}/")
        assert response.status_code == 403

    def test_non_staff_create_forbidden(self, non_staff_client: Client):
        connector = _create_connector("res-conn-create")
        response = non_staff_client.post(
            RESOURCE_URL,
            {
                "name": "res-create",
                "connector_config": connector.id,
                "enabled": True,
                "object_location": "table",
            },
        )
        assert response.status_code == 403
        assert not ResourceConfig.objects.filter(name="res-create").exists()

    def test_non_staff_put_forbidden(self, non_staff_client: Client):
        connector = _create_connector("res-conn-put")
        resource = _create_resource("res-put", connector)
        response = non_staff_client.put(
            f"{RESOURCE_URL}{resource.id}/",
            urlencode(
                {
                    "name": "res-put-renamed",
                    "connector_config": connector.id,
                    "enabled": True,
                    "object_location": "table",
                }
            ),
            content_type=FORM_CONTENT_TYPE,
        )
        assert response.status_code == 403
        resource.refresh_from_db()
        assert resource.name == "res-put"

    def test_non_staff_patch_does_not_persist(self, non_staff_client: Client):
        connector = _create_connector("res-conn-patch")
        resource = _create_resource("res-patch", connector, enabled=True)
        response = non_staff_client.patch(
            f"{RESOURCE_URL}{resource.id}/",
            urlencode({"enabled": False}),
            content_type=FORM_CONTENT_TYPE,
        )
        assert response.status_code == 403
        resource.refresh_from_db()
        assert resource.enabled is True

    def test_non_staff_delete_forbidden(self, non_staff_client: Client):
        connector = _create_connector("res-conn-delete")
        resource = _create_resource("res-delete", connector)
        response = non_staff_client.delete(f"{RESOURCE_URL}{resource.id}/")
        assert response.status_code == 403
        assert ResourceConfig.objects.filter(id=resource.id).exists()

    def test_anonymous_list_rejected(self, client: Client):
        response = client.get(RESOURCE_URL)
        assert response.status_code in (401, 403)

    def test_staff_list_ok(self, auth_client: Client):
        connector = _create_connector("res-conn-staff-list")
        _create_resource("res-staff-list", connector)
        response = auth_client.get(RESOURCE_URL)
        assert response.status_code == 200


@pytest.mark.django_db
class TestValidatorPermissions:
    def test_non_staff_rejected_before_external_call(self, non_staff_client: Client, settings):
        # Even if external validation were attempted this would hang/fail; make sure
        # permission checking happens first, and belt-and-braces disable the probe.
        settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = False
        response = non_staff_client.get(
            VALIDATOR_URL, {"uri": "postgresql://user:pass@10.255.255.1:5432/db"}
        )
        assert response.status_code == 403

    def test_anonymous_rejected(self, client: Client, settings):
        settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = False
        response = client.get(
            VALIDATOR_URL, {"uri": "postgresql://user:pass@10.255.255.1:5432/db"}
        )
        assert response.status_code in (401, 403)
