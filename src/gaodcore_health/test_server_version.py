"""Database versions kept by the connector health checks (ConnectorServerInfo)."""

from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from connectors import DriverConnectionError, ServerVersion
from gaodcore_health.health_check import check_connector_health_sync, record_server_version
from gaodcore_health.models import ConnectorServerInfo, HealthCheckResult
from gaodcore_manager.models import ConnectorConfig

ORACLE_19 = ServerVersion("oracle", "19.0.0.0.0")
ORACLE_23 = ServerVersion("oracle", "23.0.0.0.0")


class RecordServerVersionTests(TestCase):
    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="oracle", uri="oracle://u:p@db:1521/?service_name=X", enabled=True
        )
        self.t0 = timezone.now() - timedelta(days=3)

    def _info(self):
        return ConnectorServerInfo.objects.get(connector=self.connector)

    def test_version_sequence(self):
        t1, t2 = self.t0 + timedelta(days=1), self.t0 + timedelta(days=2)

        record_server_version(self.connector, ORACLE_19, self.t0)
        info = self._info()
        self.assertEqual((info.dialect, info.version), ("oracle", "19.0.0.0.0"))
        self.assertEqual((info.first_seen_at, info.last_seen_at), (self.t0, self.t0))
        self.assertIsNone(info.previous_version)

        # Same version: only last_seen_at moves.
        record_server_version(self.connector, ORACLE_19, t1)
        info = self._info()
        self.assertEqual((info.first_seen_at, info.last_seen_at), (self.t0, t1))
        self.assertIsNone(info.previous_version)

        # New version: the old one becomes the previous one.
        record_server_version(self.connector, ORACLE_23, t2)
        info = self._info()
        self.assertEqual(info.version, "23.0.0.0.0")
        self.assertEqual((info.first_seen_at, info.last_seen_at), (t2, t2))
        self.assertEqual((info.previous_dialect, info.previous_version), ("oracle", "19.0.0.0.0"))

    def test_engine_change_keeps_previous_engine(self):
        record_server_version(self.connector, ORACLE_19, self.t0)
        record_server_version(self.connector, ServerVersion("postgresql", "16.4"), timezone.now())

        info = self._info()
        self.assertEqual((info.dialect, info.version), ("postgresql", "16.4"))
        self.assertEqual((info.previous_dialect, info.previous_version), ("oracle", "19.0.0.0.0"))


@patch("gaodcore_health.health_check.probe_uri")
class HealthCheckStoresServerVersionTests(TestCase):
    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="oracle", uri="oracle://u:p@db:1521/?service_name=X", enabled=True
        )

    def _seed(self):
        old = timezone.now() - timedelta(days=5)
        record_server_version(self.connector, ORACLE_19, old)
        return old

    def test_successful_check_records_version(self, probe):
        probe.return_value = ORACLE_19

        result = check_connector_health_sync(self.connector, timeout=5)

        self.assertTrue(result.is_healthy)
        info = ConnectorServerInfo.objects.get(connector=self.connector)
        self.assertEqual(info.version, "19.0.0.0.0")

    def test_failed_check_keeps_last_known_version(self, probe):
        old = self._seed()
        probe.side_effect = DriverConnectionError("Connection not available.")

        result = check_connector_health_sync(self.connector, timeout=5)

        self.assertFalse(result.is_healthy)
        info = ConnectorServerInfo.objects.get(connector=self.connector)
        self.assertEqual((info.version, info.last_seen_at), ("19.0.0.0.0", old))

    def test_database_without_version_keeps_last_known_version(self, probe):
        old = self._seed()
        probe.return_value = None

        result = check_connector_health_sync(self.connector, timeout=5)

        self.assertTrue(result.is_healthy)
        info = ConnectorServerInfo.objects.get(connector=self.connector)
        self.assertEqual((info.version, info.last_seen_at), ("19.0.0.0.0", old))

    def test_connector_moved_to_an_api_loses_its_row(self, probe):
        self._seed()
        ConnectorConfig.objects.filter(pk=self.connector.pk).update(uri="https://example.org/data.csv")
        self.connector.refresh_from_db()
        probe.return_value = None

        result = check_connector_health_sync(self.connector, timeout=5)

        self.assertTrue(result.is_healthy)
        self.assertFalse(ConnectorServerInfo.objects.filter(connector=self.connector).exists())

    def test_failure_storing_the_version_does_not_change_the_result(self, probe):
        probe.return_value = ORACLE_19

        with patch(
            "gaodcore_health.health_check.record_server_version", side_effect=RuntimeError("db down")
        ), self.assertLogs("gaodcore_health.health_check", level="WARNING"):
            result = check_connector_health_sync(self.connector, timeout=5)

        self.assertTrue(result.is_healthy)
        result.save()  # the caller can still persist the healthy result
        self.assertTrue(HealthCheckResult.objects.filter(connector=self.connector, is_healthy=True).exists())


class ServerVersionPagesTests(TestCase):
    def setUp(self):
        self.oracle = ConnectorConfig.objects.create(
            name="oracle-conn", uri="oracle://u:p@db:1521/?service_name=X", enabled=True
        )
        self.unknown = ConnectorConfig.objects.create(
            name="pg-conn", uri="postgresql://u:p@pg/db", enabled=True
        )
        self.api = ConnectorConfig.objects.create(
            name="api-conn", uri="https://example.org/data.csv", enabled=True
        )
        record_server_version(self.oracle, ORACLE_19, timezone.now() - timedelta(days=2))
        # The last check failed: the page must still show the version seen before.
        HealthCheckResult.objects.create(connector=self.oracle, is_healthy=False, error_message="down")
        self.client.force_login(User.objects.create_user("staff", password="pw", is_staff=True))

    def test_list_shows_versions(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))

        self.assertEqual(response.status_code, 200)
        rows = {c["name"]: c for c in response.context["connector_health_data"]["connectors"]}
        self.assertEqual(rows["oracle-conn"]["server_info"].version, "19.0.0.0.0")
        self.assertIsNone(rows["pg-conn"]["server_info"])
        self.assertTrue(rows["pg-conn"]["is_database"])
        self.assertFalse(rows["api-conn"]["is_database"])
        self.assertContains(response, "oracle 19.0.0.0.0")

    def test_detail_shows_version_after_a_failure(self):
        response = self.client.get(reverse("gaodcore_health:connector_detail", args=[self.oracle.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "oracle 19.0.0.0.0")

    def test_detail_without_version(self):
        response = self.client.get(reverse("gaodcore_health:connector_detail", args=[self.unknown.id]))

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.context["server_info"])
