"""Focused tests for GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS.

External probes are mocked; no real database or network access is required.
"""

import logging
from urllib.parse import urlencode

import pytest
from _pytest.logging import LogCaptureFixture
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.test import Client

from gaodcore_manager import validators
from gaodcore_manager.apps import warn_if_external_validation_disabled
from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_project.settings import parse_bool_env

CONNECTOR_URL = "/admin/GA_OD_Core_admin/manager/connector-config/"
RESOURCE_URL = "/admin/GA_OD_Core_admin/manager/resource-config/"
VALIDATOR_URL = "/admin/GA_OD_Core_admin/manager/validator"

# The API only enables the form parsers, so writes must be form encoded.
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"

POSTGRESQL_URI = "postgresql://username:password@example.invalid:5432/gaodcore"


@pytest.fixture
def probes(monkeypatch):
    """Replaces both external probes by call counters."""

    calls = {"uri": 0, "resource": 0}

    def fake_validate_uri(uri):
        calls["uri"] += 1

    def fake_validate_resource(uri, object_location, object_location_schema):
        calls["resource"] += 1
        return [{"id": 1}]

    monkeypatch.setattr(validators, "validate_uri", fake_validate_uri)
    monkeypatch.setattr(validators, "validate_resource", fake_validate_resource)
    return calls


@pytest.fixture
def external_validation_disabled(settings):  # noqa: F811 - pytest-django fixture
    settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = False


def _create_connector(name: str, uri: str = POSTGRESQL_URI) -> ConnectorConfig:
    return ConnectorConfig.objects.create(name=name, uri=uri, enabled=True)


def _create_resource(name: str, connector: ConnectorConfig) -> ResourceConfig:
    return ResourceConfig.objects.create(
        name=name,
        connector_config=connector,
        enabled=True,
        object_location="table",
    )


def test_setting_defaults_to_enabled():
    assert settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS is True
    assert validators.external_validation_enabled() is True


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_parse_bool_env_true_values(monkeypatch, value):
    monkeypatch.setenv("GAODCORE_TEST_FLAG", value)
    assert parse_bool_env("GAODCORE_TEST_FLAG", False) is True


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "off"])
def test_parse_bool_env_false_values(monkeypatch, value):
    monkeypatch.setenv("GAODCORE_TEST_FLAG", value)
    assert parse_bool_env("GAODCORE_TEST_FLAG", True) is False


def test_parse_bool_env_absent_uses_default(monkeypatch):
    monkeypatch.delenv("GAODCORE_TEST_FLAG", raising=False)
    assert parse_bool_env("GAODCORE_TEST_FLAG", True) is True


def test_parse_bool_env_invalid_value_fails_clearly(monkeypatch):
    monkeypatch.setenv("GAODCORE_TEST_FLAG", "maybe")
    with pytest.raises(ImproperlyConfigured) as err:
        parse_bool_env("GAODCORE_TEST_FLAG", True)
    assert "GAODCORE_TEST_FLAG" in str(err.value)
    assert "maybe" in str(err.value)


def test_startup_warning_when_disabled(settings, caplog: LogCaptureFixture):  # noqa: F811
    settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = False
    with caplog.at_level(logging.WARNING, logger="gaodcore_manager.apps"):
        warn_if_external_validation_disabled()
    assert "External connector and resource validation is disabled." in caplog.text


def test_no_startup_warning_when_enabled(settings, caplog: LogCaptureFixture):  # noqa: F811
    settings.GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS = True
    with caplog.at_level(logging.WARNING, logger="gaodcore_manager.apps"):
        warn_if_external_validation_disabled()
    assert caplog.text == ""


@pytest.mark.django_db
def test_connector_create_probes_when_enabled(auth_client: Client, probes):
    response = auth_client.post(
        CONNECTOR_URL,
        {"name": "connector-enabled", "uri": POSTGRESQL_URI, "enabled": True},
    )
    assert response.status_code == 201
    assert probes["uri"] == 1


@pytest.mark.django_db
def test_connector_create_skips_probe_when_disabled(
    auth_client: Client, probes, external_validation_disabled
):
    response = auth_client.post(
        CONNECTOR_URL,
        {"name": "connector-disabled", "uri": POSTGRESQL_URI, "enabled": True},
    )
    assert response.status_code == 201
    assert probes["uri"] == 0


@pytest.mark.django_db
def test_connector_update_probes_only_when_uri_changes(auth_client: Client, probes):
    connector = _create_connector("connector-update")

    response = auth_client.patch(
        f"{CONNECTOR_URL}{connector.id}/",
        urlencode({"name": "connector-update-renamed"}),
        content_type=FORM_CONTENT_TYPE,
    )
    assert response.status_code == 200
    assert probes["uri"] == 0

    response = auth_client.patch(
        f"{CONNECTOR_URL}{connector.id}/",
        urlencode({"uri": POSTGRESQL_URI + "-other"}),
        content_type=FORM_CONTENT_TYPE,
    )
    assert response.status_code == 200
    assert probes["uri"] == 1


@pytest.mark.django_db
def test_resource_create_probes_when_enabled(auth_client: Client, probes):
    connector = _create_connector("resource-create-enabled")
    response = auth_client.post(
        RESOURCE_URL,
        {
            "name": "resource-enabled",
            "connector_config": connector.id,
            "enabled": True,
            "object_location": "table",
        },
    )
    assert response.status_code == 201
    assert probes["resource"] == 1


@pytest.mark.django_db
def test_resource_create_skips_probe_when_disabled(
    auth_client: Client, probes, external_validation_disabled
):
    connector = _create_connector("resource-create-disabled")
    response = auth_client.post(
        RESOURCE_URL,
        {
            "name": "resource-disabled",
            "connector_config": connector.id,
            "enabled": True,
            "object_location": "table",
        },
    )
    assert response.status_code == 201
    assert probes["resource"] == 0


@pytest.mark.django_db
def test_resource_local_validation_runs_when_probe_disabled(
    auth_client: Client, probes, external_validation_disabled
):
    """A postgresql resource without object_location is rejected locally."""
    connector = _create_connector("resource-local-rules")
    response = auth_client.post(
        RESOURCE_URL,
        {
            "name": "resource-local-rules",
            "connector_config": connector.id,
            "enabled": True,
        },
    )
    assert response.status_code == 400
    assert probes["resource"] == 0


@pytest.mark.django_db
def test_resource_update_probes_only_when_connection_fields_change(
    auth_client: Client, probes
):
    connector = _create_connector("resource-update")
    resource = _create_resource("resource-update", connector)

    response = auth_client.patch(
        f"{RESOURCE_URL}{resource.id}/",
        urlencode({"name": "resource-update-renamed", "enabled": False}),
        content_type=FORM_CONTENT_TYPE,
    )
    assert response.status_code == 200
    assert probes["resource"] == 0

    response = auth_client.patch(
        f"{RESOURCE_URL}{resource.id}/",
        urlencode({"object_location": "other_table"}),
        content_type=FORM_CONTENT_TYPE,
    )
    assert response.status_code == 200
    assert probes["resource"] == 1


@pytest.mark.django_db
def test_resource_partial_update_without_connector_config(
    auth_client: Client, probes
):
    """PATCH omitting connector_config falls back to the stored instance value."""
    connector = _create_connector("resource-partial")
    resource = _create_resource("resource-partial", connector)

    response = auth_client.patch(
        f"{RESOURCE_URL}{resource.id}/",
        urlencode({"object_location_schema": "public"}),
        content_type=FORM_CONTENT_TYPE,
    )
    assert response.status_code == 200
    resource.refresh_from_db()
    assert resource.object_location_schema == "public"
    assert resource.connector_config_id == connector.id
    assert probes["resource"] == 1


@pytest.mark.django_db
def test_validator_endpoint_ignores_flag(
    auth_client: Client, probes, external_validation_disabled
):
    response = auth_client.get(
        VALIDATOR_URL,
        {"uri": POSTGRESQL_URI, "object_location": "table"},
    )
    assert response.status_code == 200
    assert probes["resource"] == 1
