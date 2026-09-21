"""
Tests for health monitoring functionality.
"""

import asyncio
import re
from datetime import timedelta
from unittest.mock import patch, MagicMock

from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from gaodcore_health.models import (
    HealthCheckAlert,
    HealthCheckResult,
    HealthCheckSchedule,
    ResourceHealthCheckResult,
)
from gaodcore_health.health_check import (
    check_connector_health,
    check_all_connectors_health,
    check_and_send_alerts,
    cleanup_old_health_results,
    get_connector_health_summary,
)

PRIVATE_PREFIX = "/admin/GA_OD_Core_admin/"


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
            username="testuser", password="testpass123", is_staff=True
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

    @patch("gaodcore_health.views.check_all_connectors_health_sync")
    def test_health_check_trigger_endpoint(self, mock_check):
        """Test health check trigger API endpoint."""
        # Mock the health check function
        mock_result = HealthCheckResult(
            connector=self.connector,
            is_healthy=True,
            response_time_ms=100,
        )
        mock_check.return_value = [mock_result]

        url = reverse("gaodcore_health:api_check")
        response = self.client.post(url, {"concurrency": 2})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        mock_check.assert_called_once_with(2, timeout=None)

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


class HealthHtmlTestCase(TestCase):
    """Base fixtures for the modern Health HTML interface tests."""

    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="html-connector",
            uri="postgresql://user:secret-password@localhost/db",
            enabled=True,
        )
        self.resource = ResourceConfig.objects.create(
            name="html-resource",
            connector_config=self.connector,
            enabled=True,
            object_location="some_table",
        )
        self.staff_user = User.objects.create_user(
            "html-staff", password="pw", is_staff=True
        )
        self.regular_user = User.objects.create_user(
            "html-regular", password="pw", is_staff=False
        )

    def _html_urls(self):
        return [
            reverse("gaodcore_health:health_index"),
            reverse("gaodcore_health:connector_list"),
            reverse("gaodcore_health:resource_list"),
            reverse("gaodcore_health:connector_detail", args=[self.connector.id]),
            reverse("gaodcore_health:resource_detail", args=[self.resource.id]),
            reverse("gaodcore_health:connector_resources", args=[self.connector.id]),
            reverse("gaodcore_health:dashboard"),
        ]


class HealthHtmlAccessControlTests(HealthHtmlTestCase):
    """Every human-facing Health HTML view must be staff-only, like Manager."""

    def test_anonymous_users_are_redirected_to_the_admin_login(self):
        for url in self._html_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(
                    response.url.startswith(f"{PRIVATE_PREFIX}login/")
                )
                self.assertIn(f"next={url}", response.url)

    def test_non_staff_users_cannot_view_any_page(self):
        self.client.force_login(self.regular_user)
        for url in self._html_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(
                    response.url.startswith(f"{PRIVATE_PREFIX}login/")
                )

    def test_staff_users_can_access_every_modern_html_page(self):
        self.client.force_login(self.staff_user)
        for url in self._html_urls():
            with self.subTest(url=url):
                response = self.client.get(url, follow=True)
                self.assertEqual(response.status_code, 200)


class HealthApiAccessControlTests(HealthHtmlTestCase):
    """Every Health API endpoint must require a staff user."""

    def setUp(self):
        super().setUp()
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=42
        )
        ResourceHealthCheckResult.objects.create(
            resource=self.resource, is_healthy=True, response_time_ms=42
        )

    def _read_api_urls(self):
        return [
            reverse("gaodcore_health:api_status"),
            reverse("gaodcore_health:api_summary"),
            reverse("gaodcore_health:api_history"),
            reverse(
                "gaodcore_health:api_connector_detail", args=[self.connector.id]
            ),
            reverse("gaodcore_health:api_resource_status"),
            reverse("gaodcore_health:api_resource_summary"),
            reverse("gaodcore_health:api_resource_history"),
            reverse(
                "gaodcore_health:api_resource_detail", args=[self.resource.id]
            ),
        ]

    def test_anonymous_users_cannot_access_any_read_api(self):
        for url in self._read_api_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_authenticated_non_staff_users_cannot_access_any_read_api(self):
        self.client.force_login(self.regular_user)
        for url in self._read_api_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_staff_users_can_access_every_read_api(self):
        self.client.force_login(self.staff_user)
        for url in self._read_api_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, status.HTTP_200_OK)

    @patch("gaodcore_health.views.check_all_connectors_health_sync")
    @patch("gaodcore_health.views.check_all_resources_health_sync")
    def test_anonymous_users_cannot_trigger_health_checks(
        self, mock_resource_check, mock_connector_check
    ):
        for url in (
            reverse("gaodcore_health:api_check"),
            reverse("gaodcore_health:api_resource_check"),
        ):
            with self.subTest(url=url):
                response = self.client.post(url)
                self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        mock_connector_check.assert_not_called()
        mock_resource_check.assert_not_called()

    @patch("gaodcore_health.views.check_all_connectors_health_sync")
    @patch("gaodcore_health.views.check_all_resources_health_sync")
    def test_authenticated_non_staff_users_cannot_trigger_health_checks(
        self, mock_resource_check, mock_connector_check
    ):
        self.client.force_login(self.regular_user)
        for url in (
            reverse("gaodcore_health:api_check"),
            reverse("gaodcore_health:api_resource_check"),
        ):
            with self.subTest(url=url):
                response = self.client.post(url)
                self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        mock_connector_check.assert_not_called()
        mock_resource_check.assert_not_called()

    @patch("gaodcore_health.views.check_all_connectors_health_sync", return_value=[])
    @patch("gaodcore_health.views.check_all_resources_health_sync", return_value=[])
    def test_staff_users_can_trigger_both_health_checks(
        self, mock_resource_check, mock_connector_check
    ):
        self.client.force_login(self.staff_user)

        connector_response = self.client.post(
            reverse("gaodcore_health:api_check"), {"concurrency": 2}
        )
        resource_response = self.client.post(
            reverse("gaodcore_health:api_resource_check"),
            {"concurrency": 3},
        )

        self.assertEqual(connector_response.status_code, status.HTTP_200_OK)
        self.assertEqual(resource_response.status_code, status.HTTP_200_OK)
        mock_connector_check.assert_called_once_with(2, timeout=None)
        mock_resource_check.assert_called_once_with(3, timeout=None)


class HealthHtmlSharedShellTests(HealthHtmlTestCase):
    """Health pages must render through the shared private administration shell."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_health_pages_use_the_shared_private_base_stylesheet(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        self.assertIn("gaodcore_manager/private_base.css", content)
        self.assertIn("gaodcore_health/health.css", content)

    def test_global_navigation_marks_health_active(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        nav_match = re.search(
            r'<nav class="manager-tabs__nav">.*?</nav>', content, re.DOTALL
        )
        self.assertIsNotNone(nav_match)
        health_link = re.search(
            r'<a[^>]*href="' + re.escape(reverse("gaodcore_health:connector_list")) + r'"[^>]*>',
            nav_match.group(0),
        )
        self.assertIsNotNone(health_link)
        self.assertIn('class="active"', health_link.group(0))

    def test_secondary_navigation_marks_connector_health_active_on_connector_pages(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        subnav_match = re.search(
            r'<nav class="health-subnav__nav"[^>]*>.*?</nav>', content, re.DOTALL
        )
        self.assertIsNotNone(subnav_match)
        connectors_link = re.search(
            r'<a[^>]*href="' + re.escape(reverse("gaodcore_health:connector_list")) + r'"[^>]*>',
            subnav_match.group(0),
        )
        resources_link = re.search(
            r'<a[^>]*href="' + re.escape(reverse("gaodcore_health:resource_list")) + r'"[^>]*>',
            subnav_match.group(0),
        )
        self.assertIn('class="active"', connectors_link.group(0))
        self.assertNotIn('class="active"', resources_link.group(0))

    def test_secondary_navigation_marks_resource_health_active_on_resource_pages(self):
        response = self.client.get(reverse("gaodcore_health:resource_list"))
        content = response.content.decode()
        subnav_match = re.search(
            r'<nav class="health-subnav__nav"[^>]*>.*?</nav>', content, re.DOTALL
        )
        connectors_link = re.search(
            r'<a[^>]*href="' + re.escape(reverse("gaodcore_health:connector_list")) + r'"[^>]*>',
            subnav_match.group(0),
        )
        resources_link = re.search(
            r'<a[^>]*href="' + re.escape(reverse("gaodcore_health:resource_list")) + r'"[^>]*>',
            subnav_match.group(0),
        )
        self.assertNotIn('class="active"', connectors_link.group(0))
        self.assertIn('class="active"', resources_link.group(0))

    def test_breadcrumbs_present_on_list_and_detail_pages(self):
        cases = [
            (reverse("gaodcore_health:connector_list"), ["Salud", "Conectores"]),
            (reverse("gaodcore_health:resource_list"), ["Salud", "Recursos"]),
            (
                reverse("gaodcore_health:connector_detail", args=[self.connector.id]),
                ["Salud", "Conectores", self.connector.name],
            ),
            (
                reverse("gaodcore_health:resource_detail", args=[self.resource.id]),
                ["Salud", "Conectores", self.connector.name, self.resource.name],
            ),
            (
                reverse(
                    "gaodcore_health:connector_resources", args=[self.connector.id]
                ),
                ["Salud", "Conectores", self.connector.name],
            ),
        ]
        for url, expected_crumbs in cases:
            with self.subTest(url=url):
                response = self.client.get(url)
                content = response.content.decode()
                breadcrumb_match = re.search(
                    r'<nav class="manager-breadcrumbs".*?</nav>', content, re.DOTALL
                )
                self.assertIsNotNone(breadcrumb_match)
                breadcrumb_html = breadcrumb_match.group(0)
                for crumb in expected_crumbs:
                    self.assertIn(crumb, breadcrumb_html)


class HealthFilterAndPaginationTests(HealthHtmlTestCase):
    """Status filters and pagination must keep working after the CSS/markup migration."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_status_filter_persists_across_pagination_links(self):
        for i in range(25):
            ConnectorConfig.objects.create(
                name=f"pag-connector-{i}", uri=f"postgresql://x/y{i}", enabled=True
            )
        response = self.client.get(
            reverse("gaodcore_health:connector_list"), {"status": "unknown"}
        )
        content = response.content.decode()
        self.assertIn("status=unknown", content)

    def test_connector_list_pagination_renders_second_page(self):
        for i in range(25):
            ConnectorConfig.objects.create(
                name=f"pag-connector-{i}", uri=f"postgresql://x/y{i}", enabled=True
            )
        response = self.client.get(
            reverse("gaodcore_health:connector_list") + "?page=2"
        )
        self.assertEqual(response.status_code, 200)


class HealthStatusRenderingTests(HealthHtmlTestCase):
    """Healthy, unhealthy and unknown states must remain visible after restyling."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_connector_list_shows_unknown_badge_with_no_health_checks(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        self.assertContains(response, "health-status-badge unknown")

    def test_connector_list_shows_healthy_badge(self):
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=42
        )
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        self.assertContains(response, "health-status-badge healthy")

    def test_connector_detail_shows_unhealthy_badge_and_error_message(self):
        HealthCheckResult.objects.create(
            connector=self.connector,
            is_healthy=False,
            response_time_ms=None,
            error_message="Connection refused",
        )
        response = self.client.get(
            reverse("gaodcore_health:connector_detail", args=[self.connector.id])
        )
        content = response.content.decode()
        self.assertIn("health-status-badge unhealthy", content)
        self.assertIn("Connection refused", content)

    def test_no_emoji_status_icons_are_rendered(self):
        HealthCheckResult.objects.create(
            connector=self.connector, is_healthy=True, response_time_ms=42
        )
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        for icon in ("✅", "❌", "⚠️", "\U0001f3e5", "\U0001f50c"):
            self.assertNotIn(icon, content)


class HealthAutoRefreshTests(HealthHtmlTestCase):
    """The 30-second auto-refresh must stay exclusive to Health."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_health_pages_contain_the_refresh_timer(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        self.assertIn("window.location.reload()", content)
        self.assertIn("30000", content)

    def test_manager_pages_do_not_contain_the_health_refresh_timer(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertNotIn("30000", content)
        self.assertNotIn("Auto-refresh", content)


class HealthLegacyDashboardTests(HealthHtmlTestCase):
    """The legacy dashboard route stays named and reversible but now redirects."""

    def test_dashboard_route_is_still_named_and_reversible(self):
        url = reverse("gaodcore_health:dashboard")
        self.assertEqual(url, f"{PRIVATE_PREFIX}health/dashboard/")

    def test_dashboard_redirects_to_the_modern_connector_health_list_for_staff(self):
        self.client.force_login(self.staff_user)
        response = self.client.get(reverse("gaodcore_health:dashboard"))
        self.assertRedirects(response, reverse("gaodcore_health:connector_list"))

    def test_dashboard_requires_staff_like_every_other_html_page(self):
        response = self.client.get(reverse("gaodcore_health:dashboard"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))

        self.client.force_login(self.regular_user)
        response = self.client.get(reverse("gaodcore_health:dashboard"))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}login/"))


class SwaggerAndAdminLinksTests(HealthHtmlTestCase):
    """Private Swagger, Manager and Django Admin links remain correct from Health pages."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_health_pages_link_to_the_private_swagger_and_manager(self):
        response = self.client.get(reverse("gaodcore_health:connector_list"))
        content = response.content.decode()
        self.assertIn(reverse("admin-schema-swagger-ui"), content)
        self.assertIn(reverse("manager_web:resource-list"), content)
