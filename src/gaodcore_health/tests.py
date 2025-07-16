"""
Tests for health monitoring functionality.
"""

import asyncio
from datetime import timedelta
from unittest.mock import patch, MagicMock

from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status

from gaodcore_manager.models import ConnectorConfig
from .models import HealthCheckResult, HealthCheckSchedule, HealthCheckAlert
from .health_check import (
    check_connector_health,
    check_all_connectors_health,
    check_and_send_alerts,
    cleanup_old_health_results,
    get_connector_health_summary,
)


class HealthCheckModelTests(TestCase):
    """Test health check models."""

    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="Test Connector",
            uri="postgresql://test:test@localhost/testdb",
            enabled=True,
        )

    def test_health_check_result_creation(self):
        """Test creating a health check result."""
        result = HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        self.assertEqual(result.connector, self.connector)
        self.assertTrue(result.is_healthy)
        self.assertEqual(result.response_time_ms, 100)
        self.assertIsNone(result.error_message)

    def test_health_check_result_unhealthy(self):
        """Test creating an unhealthy health check result."""
        result = HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=False,
            response_time_ms=5000,
            error_message="Connection timeout",
            error_type="timeout",
        )

        self.assertFalse(result.is_healthy)
        self.assertEqual(result.error_message, "Connection timeout")
        self.assertEqual(result.error_type, "timeout")

    def test_health_check_schedule_creation(self):
        """Test creating a health check schedule."""
        schedule = HealthCheckSchedule.objects.create(
            name="Default Schedule", interval_minutes=5, enabled=True
        )

        self.assertEqual(schedule.name, "Default Schedule")
        self.assertEqual(schedule.interval_minutes, 5)
        self.assertTrue(schedule.enabled)

    def test_health_check_schedule_update_run_times(self):
        """Test updating schedule run times."""
        schedule = HealthCheckSchedule.objects.create(
            name="Test Schedule", interval_minutes=10
        )

        before_update = timezone.now()
        schedule.update_run_times()
        after_update = timezone.now()

        self.assertIsNotNone(schedule.last_run)
        self.assertIsNotNone(schedule.next_run)
        self.assertTrue(before_update <= schedule.last_run <= after_update)
        self.assertEqual(schedule.next_run, schedule.last_run + timedelta(minutes=10))

    def test_health_check_alert_creation(self):
        """Test creating a health check alert."""
        alert = HealthCheckAlert.objects.create(
            connector=self.connector,
            alert_type="failure",
            threshold_minutes=5,
            is_active=True,
        )

        self.assertEqual(alert.connector, self.connector)
        self.assertEqual(alert.alert_type, "failure")
        self.assertEqual(alert.threshold_minutes, 5)
        self.assertTrue(alert.is_active)

    def test_health_check_alert_should_send_alert(self):
        """Test alert sending logic."""
        alert = HealthCheckAlert.objects.create(
            connector=self.connector,
            alert_type="failure",
            threshold_minutes=5,
            is_active=True,
        )

        # Should send alert when no previous alert
        self.assertTrue(alert.should_send_alert())

        # Mark alert as sent
        alert.mark_alert_sent()

        # Should not send alert immediately after
        self.assertFalse(alert.should_send_alert())

        # Should send alert after threshold time
        alert.last_alert_time = timezone.now() - timedelta(minutes=10)
        alert.save()
        self.assertTrue(alert.should_send_alert())


class HealthCheckFunctionTests(TransactionTestCase):
    """Test health check functions."""

    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="Test Connector",
            uri="postgresql://test:test@localhost/testdb",
            enabled=True,
        )

    @patch("gaodcore_health.health_check.validate_uri")
    def test_check_connector_health_success(self, mock_validate):
        """Test successful connector health check."""
        mock_validate.return_value = None

        result = asyncio.run(check_connector_health(self.connector))

        self.assertEqual(result.connector, self.connector)
        self.assertTrue(result.is_healthy)
        self.assertIsNotNone(result.response_time_ms)
        self.assertIsNone(result.error_message)
        mock_validate.assert_called_once_with(self.connector.uri)

    @patch("gaodcore_health.health_check.validate_uri")
    def test_check_connector_health_failure(self, mock_validate):
        """Test failed connector health check."""
        from connectors import DriverConnectionError

        mock_validate.side_effect = DriverConnectionError("Connection failed")

        result = asyncio.run(check_connector_health(self.connector))

        self.assertEqual(result.connector, self.connector)
        self.assertFalse(result.is_healthy)
        self.assertIsNotNone(result.response_time_ms)
        self.assertEqual(result.error_message, "Connection failed")
        self.assertEqual(result.error_type, "connection_error")

    @patch("gaodcore_health.health_check.validate_uri")
    def test_check_all_connectors_health(self, mock_validate):
        """Test checking all connectors health."""
        # Create another connector
        ConnectorConfig.objects.create(
            name="Test Connector 2",
            uri="mysql://test:test@localhost/testdb2",
            enabled=True,
        )

        mock_validate.return_value = None

        results = asyncio.run(check_all_connectors_health(concurrency_limit=2))

        self.assertEqual(len(results), 2)
        self.assertTrue(all(result.is_healthy for result in results))

        # Check that results were saved to database
        saved_results = HealthCheckResult.objects.all()
        self.assertEqual(saved_results.count(), 2)

    def test_cleanup_old_health_results(self):
        """Test cleaning up old health check results."""
        # Create old results
        old_time = timezone.now() - timedelta(days=35)
        HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=True,
            response_time_ms=100,
            check_time=old_time,
        )

        # Create recent results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        # Should have 2 results initially
        self.assertEqual(HealthCheckResult.objects.count(), 2)

        # Clean up old results (older than 30 days)
        cleanup_old_health_results(retention_days=30)

        # Should have 1 result after cleanup
        self.assertEqual(HealthCheckResult.objects.count(), 1)

    def test_get_connector_health_summary(self):
        """Test getting connector health summary."""
        # Create some health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )
        HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=False,
            response_time_ms=200,
            error_message="Connection failed",
        )

        summary = get_connector_health_summary(hours=24)

        self.assertEqual(summary["total_checks"], 2)
        self.assertEqual(summary["healthy_checks"], 1)
        self.assertEqual(summary["unhealthy_checks"], 1)
        self.assertIn(self.connector.name, summary["connectors"])

        connector_data = summary["connectors"][self.connector.name]
        self.assertEqual(connector_data["total_checks"], 2)
        self.assertEqual(connector_data["success_rate"], 50.0)

    def test_check_and_send_alerts(self):
        """Test alert checking and sending."""
        # Create an alert
        HealthCheckAlert.objects.create(
            connector=self.connector,
            alert_type="consecutive_failures",
            consecutive_failures_threshold=2,
            is_active=True,
        )

        # Create consecutive failures
        for _ in range(3):
            HealthCheckResult.objects.create(
                connector=self.connector,
                is_healthy=False,
                error_message="Connection failed",
            )

        with patch("gaodcore_health.health_check.send_alert") as mock_send:
            check_and_send_alerts()
            mock_send.assert_called_once()


class HealthCheckAPITests(APITestCase):
    """Test health check API endpoints."""

    def setUp(self):
        self.user = User.objects.create_user(
            username="testuser", password="testpass123"
        )
        self.client.force_authenticate(user=self.user)

        self.connector = ConnectorConfig.objects.create(
            name="Test Connector",
            uri="postgresql://test:test@localhost/testdb",
            enabled=True,
        )

    def test_health_status_endpoint(self):
        """Test health status API endpoint."""
        # Create a health check result
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        url = reverse("gaodcore_health:api_status")
        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["connector_name"], self.connector.name)
        self.assertTrue(response.data[0]["is_healthy"])

    def test_health_summary_endpoint(self):
        """Test health summary API endpoint."""
        # Create health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        url = reverse("gaodcore_health:api_summary")
        response = self.client.get(url, {"hours": 24})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_checks"], 1)
        self.assertEqual(response.data["healthy_checks"], 1)
        self.assertEqual(response.data["success_rate"], 100.0)

    def test_health_history_endpoint(self):
        """Test health history API endpoint."""
        # Create health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )
        HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=False,
            response_time_ms=200,
            error_message="Connection failed",
        )

        url = reverse("gaodcore_health:api_history")
        response = self.client.get(url, {"hours": 24, "limit": 10})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_health_history_filter_healthy_only(self):
        """Test health history endpoint with healthy_only filter."""
        # Create health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )
        HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=False,
            response_time_ms=200,
            error_message="Connection failed",
        )

        url = reverse("gaodcore_health:api_history")
        response = self.client.get(url, {"healthy_only": "true"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["is_healthy"])

    def test_connector_health_detail_endpoint(self):
        """Test connector health detail API endpoint."""
        # Create health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        url = reverse("gaodcore_health:api_connector_detail", args=[self.connector.id])
        response = self.client.get(url, {"hours": 24})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["connector_name"], self.connector.name)
        self.assertEqual(response.data["total_checks"], 1)
        self.assertEqual(response.data["success_rate"], 100.0)

    def test_connector_health_detail_not_found(self):
        """Test connector health detail endpoint with non-existent connector."""
        url = reverse("gaodcore_health:api_connector_detail", args=[999])
        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch("gaodcore_health.health_check.check_all_connectors_health")
    def test_health_check_trigger_endpoint(self, mock_check):
        """Test health check trigger API endpoint."""
        # Mock the health check function
        mock_result = MagicMock()
        mock_result.connector = self.connector
        mock_result.is_healthy = True
        mock_result.response_time_ms = 100
        mock_result.error_message = None
        mock_result.error_type = None
        mock_check.return_value = [mock_result]

        url = reverse("gaodcore_health:api_check")
        response = self.client.post(url, {"concurrency": 2})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        mock_check.assert_called_once_with(2)

    def test_health_dashboard_view(self):
        """Test health dashboard view."""
        # Create a health check result
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        url = reverse("gaodcore_health:dashboard")
        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertContains(response, "Health Monitor Dashboard")
        self.assertContains(response, self.connector.name)

    def test_unauthenticated_access(self):
        """Test that unauthenticated users cannot access health endpoints."""
        self.client.force_authenticate(user=None)

        url = reverse("gaodcore_health:api_status")
        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


class HealthCheckManagementCommandTests(TestCase):
    """Test health check management commands."""

    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="Test Connector",
            uri="postgresql://test:test@localhost/testdb",
            enabled=True,
        )

    @patch("gaodcore_health.health_check.check_all_connectors_health")
    def test_health_check_command_basic(self, mock_check):
        """Test basic health check command execution."""
        from django.core.management import call_command
        from io import StringIO

        mock_result = MagicMock()
        mock_result.connector = self.connector
        mock_result.is_healthy = True
        mock_result.response_time_ms = 100
        mock_check.return_value = [mock_result]

        out = StringIO()
        call_command("health_check", stdout=out)

        output = out.getvalue()
        self.assertIn("Health Check Summary", output)
        self.assertIn("Total Connectors: 1", output)
        mock_check.assert_called_once()

    @patch("gaodcore_health.health_check.check_specific_connector_health")
    def test_health_check_command_specific_connector(self, mock_check):
        """Test health check command for specific connector."""
        from django.core.management import call_command
        from io import StringIO

        mock_result = MagicMock()
        mock_result.connector = self.connector
        mock_result.is_healthy = True
        mock_result.response_time_ms = 100
        mock_result.error_message = None
        mock_check.return_value = mock_result

        out = StringIO()
        call_command(
            "health_check", "--connector-id", str(self.connector.id), stdout=out
        )

        output = out.getvalue()
        self.assertIn(f"Connector: {self.connector.name}", output)
        self.assertIn("✓ HEALTHY", output)
        mock_check.assert_called_once_with(self.connector.id)

    def test_health_report_command(self):
        """Test health report command."""
        from django.core.management import call_command
        from io import StringIO

        # Create some health check results
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=100
        )

        out = StringIO()
        call_command("health_report", "--hours", "24", stdout=out)

        output = out.getvalue()
        self.assertIn("Health Report", output)
        self.assertIn("Total Checks: 1", output)
        self.assertIn(self.connector.name, output)
