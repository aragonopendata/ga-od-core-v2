"""Focused tests for the manual "check" actions of the manager web interface.

The shared validators are mocked at `gaodcore_manager.web_views.validators`, so
these tests never touch a real database origin or the network.
"""

from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils.html import escape
from rest_framework.exceptions import ValidationError

from exceptions import ErrorCodes, ServiceUnavailable
from gaodcore_manager.models import ConnectorConfig, ResourceConfig

# `gaodcore_manager.views` binds `resource_validator` by value at import time.
# The URLconf imports it lazily on the first request, so without this eager
# import it would be imported while the patch below is active and would keep
# the mock for the rest of the session, breaking the REST validator tests.
from gaodcore_manager import views  # noqa: F401  isort:skip

PRIVATE_PREFIX = "/admin/GA_OD_Core_admin/"
POSTGRESQL_URI = "postgresql://user:sup3rs3cr3t@localhost:5432/db"
URI_PASSWORD = "sup3rs3cr3t"


class WebCheckTestCase(TestCase):
    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="check-connector", uri=POSTGRESQL_URI, enabled=True
        )
        self.resource = ResourceConfig.objects.create(
            name="check-resource",
            connector_config=self.connector,
            enabled=True,
            object_location="some_table",
            object_location_schema="public",
        )
        self.staff_user = User.objects.create_user("staff", password="pw", is_staff=True)
        self.regular_user = User.objects.create_user(
            "regular", password="pw", is_staff=False
        )
        patch_uri = mock.patch(
            "gaodcore_manager.web_views.validators.uri_validator", return_value=None
        )
        patch_resource = mock.patch(
            "gaodcore_manager.web_views.validators.resource_validator",
            return_value=[{"id": 1}],
        )
        self.uri_validator = patch_uri.start()
        self.resource_validator = patch_resource.start()
        self.addCleanup(patch_uri.stop)
        self.addCleanup(patch_resource.stop)
        # Assert on the English source strings regardless of the Spanish default
        # language, so the translations cannot break these tests.
        self.client.defaults["HTTP_ACCEPT_LANGUAGE"] = "en"

    def login_staff(self):
        self.client.force_login(self.staff_user)

    @property
    def connector_check_url(self):
        return reverse("manager_web:connector-check", kwargs={"pk": self.connector.pk})

    @property
    def resource_check_url(self):
        return reverse("manager_web:resource-check", kwargs={"pk": self.resource.pk})

    def assertRenderedMessage(self, response, expected, level="success"):
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f"manager-messages__item--{level}")
        self.assertContains(response, escape(expected))

    def assertNoSecretsInMessages(self, response):
        """The notification itself must not carry the URI or its credentials.

        Only the messages block is inspected: the connector detail page renders
        the stored URI on purpose, behind its "Show URI" toggle.
        """
        content = response.content.decode()
        start = content.index('<ul class="manager-messages"')
        messages_html = content[start : content.index("</ul>", start)]
        self.assertNotIn(URI_PASSWORD, messages_html)
        self.assertNotIn(POSTGRESQL_URI, messages_html)


class TestRoutesAndTemplates(WebCheckTestCase):
    def test_check_route_names_reverse_to_the_expected_paths(self):
        self.assertEqual(
            self.resource_check_url,
            f"{PRIVATE_PREFIX}web/manager/resources/{self.resource.pk}/check/",
        )
        self.assertEqual(
            self.connector_check_url,
            f"{PRIVATE_PREFIX}web/manager/connectors/{self.connector.pk}/check/",
        )

    def test_connector_detail_renders_a_csrf_protected_check_form(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk})
        )
        content = response.content.decode()
        self.assertIn(f'method="post" action="{self.connector_check_url}"', content)
        self.assertIn("csrfmiddlewaretoken", content)
        self.assertIn("Check connection", content)

    def test_resource_detail_renders_a_csrf_protected_check_form(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        )
        content = response.content.decode()
        self.assertIn(f'method="post" action="{self.resource_check_url}"', content)
        self.assertIn("csrfmiddlewaretoken", content)
        self.assertIn("Check resource", content)

    def test_check_forms_carry_no_configuration_fields(self):
        self.login_staff()
        cases = [
            (
                reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
                self.connector_check_url,
            ),
            (
                reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
                self.resource_check_url,
            ),
        ]
        for detail_url, action in cases:
            with self.subTest(detail_url=detail_url):
                content = self.client.get(detail_url).content.decode()
                start = content.index(f'action="{action}"')
                form_html = content[start : content.index("</form>", start)]
                for forbidden in (
                    "uri",
                    URI_PASSWORD,
                    "object_location",
                    "object_location_schema",
                ):
                    self.assertNotIn(forbidden, form_html)

    def test_check_button_appears_before_edit(self):
        self.login_staff()
        content = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        ).content.decode()
        self.assertLess(content.index("Check resource"), content.index(">Edit<"))


class TestAccessControl(WebCheckTestCase):
    def _check_urls(self):
        return [self.connector_check_url, self.resource_check_url]

    def test_get_is_not_allowed_for_staff_users(self):
        self.login_staff()
        for url in self._check_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 405)
        self.uri_validator.assert_not_called()
        self.resource_validator.assert_not_called()

    def test_anonymous_post_is_redirected_to_the_login(self):
        for url in self._check_urls():
            with self.subTest(url=url):
                response = self.client.post(url)
                self.assertEqual(response.status_code, 302)
                self.assertIn("/login", response["Location"])
        self.uri_validator.assert_not_called()
        self.resource_validator.assert_not_called()

    def test_non_staff_post_is_redirected_to_the_login(self):
        self.client.force_login(self.regular_user)
        for url in self._check_urls():
            with self.subTest(url=url):
                response = self.client.post(url)
                self.assertEqual(response.status_code, 302)
                self.assertIn("/login", response["Location"])
        self.uri_validator.assert_not_called()
        self.resource_validator.assert_not_called()

    def test_missing_objects_return_404(self):
        self.login_staff()
        urls = [
            reverse("manager_web:connector-check", kwargs={"pk": 999999}),
            reverse("manager_web:resource-check", kwargs={"pk": 999999}),
        ]
        for url in urls:
            with self.subTest(url=url):
                self.assertEqual(self.client.post(url).status_code, 404)
        self.uri_validator.assert_not_called()
        self.resource_validator.assert_not_called()


class TestConnectorCheckSuccess(WebCheckTestCase):
    def test_check_calls_the_shared_validator_and_reports_success(self):
        self.login_staff()
        response = self.client.post(self.connector_check_url)
        self.uri_validator.assert_called_once_with(POSTGRESQL_URI)
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
            fetch_redirect_response=False,
        )
        followed = self.client.get(response["Location"])
        self.assertRenderedMessage(
            followed, 'The connector "check-connector" is available.'
        )

    @override_settings(GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS=False)
    def test_check_runs_even_when_external_validation_is_disabled(self):
        self.login_staff()
        self.client.post(self.connector_check_url)
        self.uri_validator.assert_called_once_with(POSTGRESQL_URI)

    def test_disabled_connectors_can_be_checked(self):
        self.connector.enabled = False
        self.connector.save()
        self.login_staff()
        response = self.client.post(self.connector_check_url, follow=True)
        self.uri_validator.assert_called_once_with(POSTGRESQL_URI)
        self.assertRenderedMessage(
            response, 'The connector "check-connector" is available.'
        )


class TestResourceCheckSuccess(WebCheckTestCase):
    def test_check_calls_the_shared_validator_and_reports_success(self):
        self.login_staff()
        response = self.client.post(self.resource_check_url)
        self.resource_validator.assert_called_once_with(
            POSTGRESQL_URI, "some_table", "public", limit=1
        )
        self.assertRedirects(
            response,
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
            fetch_redirect_response=False,
        )
        followed = self.client.get(response["Location"])
        self.assertRenderedMessage(
            followed, 'The resource "check-resource" is available and can be queried.'
        )

    def test_check_consumes_a_lazy_iterable(self):
        consumed = []

        def rows():
            consumed.append("started")
            yield {"id": 1}
            consumed.append("finished")

        self.resource_validator.return_value = rows()
        self.login_staff()
        self.client.post(self.resource_check_url)
        self.assertEqual(consumed, ["started", "finished"])

    def test_an_empty_resource_is_still_reported_as_available(self):
        self.resource_validator.return_value = iter([])
        self.login_staff()
        response = self.client.post(self.resource_check_url, follow=True)
        self.assertRenderedMessage(
            response, 'The resource "check-resource" is available and can be queried.'
        )

    def test_a_failure_raised_while_iterating_is_reported(self):
        def rows():
            raise ValidationError(
                "Resource is not available.", ErrorCodes.RESOURCE_UNAVAILABLE
            )
            yield  # pragma: no cover - generator marker

        self.resource_validator.return_value = rows()
        self.login_staff()
        response = self.client.post(self.resource_check_url, follow=True)
        self.assertRenderedMessage(
            response,
            "The connection is available, but the object configured for resource "
            '"check-resource" could not be accessed.',
            level="error",
        )

    @override_settings(GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS=False)
    def test_check_runs_even_when_external_validation_is_disabled(self):
        self.login_staff()
        self.client.post(self.resource_check_url)
        self.resource_validator.assert_called_once_with(
            POSTGRESQL_URI, "some_table", "public", limit=1
        )

    def test_disabled_resources_and_connectors_can_be_checked(self):
        self.connector.enabled = False
        self.connector.save()
        self.resource.enabled = False
        self.resource.save()
        self.login_staff()
        response = self.client.post(self.resource_check_url, follow=True)
        self.resource_validator.assert_called_once_with(
            POSTGRESQL_URI, "some_table", "public", limit=1
        )
        self.assertRenderedMessage(
            response, 'The resource "check-resource" is available and can be queried.'
        )


class TestKnownFailures(WebCheckTestCase):
    def test_connector_connection_failure_is_reported(self):
        self.uri_validator.side_effect = ServiceUnavailable(
            "Connection is not available.", code=ErrorCodes.CONNECTION_UNAVAILABLE
        )
        self.login_staff()
        response = self.client.post(self.connector_check_url)
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
            fetch_redirect_response=False,
        )
        followed = self.client.get(response["Location"])
        self.assertRenderedMessage(
            followed,
            'Could not connect to connector "check-connector". '
            "Check its credentials, server, and port.",
            level="error",
        )
        self.assertNotIn("Connection is not available.", followed.content.decode())
        self.assertNoSecretsInMessages(followed)

    def test_resource_unavailable_object_is_reported(self):
        self.resource_validator.side_effect = ValidationError(
            "Resource is not available. Table, view, function, etc... not exists.",
            ErrorCodes.RESOURCE_UNAVAILABLE,
        )
        self.login_staff()
        response = self.client.post(self.resource_check_url, follow=True)
        self.assertRenderedMessage(
            response,
            "The connection is available, but the object configured for resource "
            '"check-resource" could not be accessed.',
            level="error",
        )
        self.assertNotIn("Table, view, function", response.content.decode())
        self.assertNoSecretsInMessages(response)

    def test_connector_known_codes_are_mapped(self):
        cases = [
            (
                ValidationError("boom", ErrorCodes.SCHEMA_NOT_IMPLEMENTED),
                'The connection type configured for "check-connector" is not supported.',
            ),
            (
                ValidationError("boom", ErrorCodes.MIME_TYPE_NOT_ALLOWED),
                'The HTTP origin configured for "check-connector" did not return an '
                "allowed JSON content type.",
            ),
            (
                ServiceUnavailable("boom", code=ErrorCodes.BAD_GATEWAY),
                'The connection check for connector "check-connector" failed.',
            ),
        ]
        self.login_staff()
        for exception, expected in cases:
            with self.subTest(exception=exception):
                self.uri_validator.side_effect = exception
                response = self.client.post(self.connector_check_url, follow=True)
                self.assertRenderedMessage(response, expected, level="error")
                self.assertNotIn("boom", response.content.decode())

    def test_resource_known_codes_are_mapped(self):
        cases = [
            (
                ValidationError("boom", ErrorCodes.CONNECTION_UNAVAILABLE),
                "Could not connect to the connector used by resource "
                '"check-resource". Check the connector configuration.',
            ),
            (
                ValidationError("boom", ErrorCodes.SCHEMA_NOT_IMPLEMENTED),
                'The connection type configured for "check-resource" is not supported.',
            ),
            (
                ValidationError("boom", ErrorCodes.MIME_TYPE_NOT_ALLOWED),
                'The HTTP origin configured for "check-resource" did not return an '
                "allowed JSON content type.",
            ),
            (
                ValidationError("boom", ErrorCodes.TOO_MANY_ROWS),
                'Resource "check-resource" responds, but it exceeds the allowed row '
                "limit.",
            ),
            (
                ValidationError("boom", ErrorCodes.REQUIRED),
                'Resource "check-resource" has an invalid configuration. Review its '
                "object location and schema.",
            ),
            (
                ValidationError("boom", ErrorCodes.INVALID_FIELD),
                'Resource "check-resource" has an invalid configuration. Review its '
                "object location and schema.",
            ),
            (
                ValidationError("boom", ErrorCodes.QUERY_ERROR),
                'The check for resource "check-resource" failed.',
            ),
        ]
        self.login_staff()
        for exception, expected in cases:
            with self.subTest(exception=exception):
                self.resource_validator.side_effect = exception
                response = self.client.post(self.resource_check_url, follow=True)
                self.assertRenderedMessage(response, expected, level="error")
                self.assertNotIn("boom", response.content.decode())

    def test_a_field_keyed_validation_error_falls_back_to_the_generic_message(self):
        self.resource_validator.side_effect = ValidationError(
            {"object_location": ["boom"]}
        )
        self.login_staff()
        response = self.client.post(self.resource_check_url, follow=True)
        self.assertRenderedMessage(
            response, 'The check for resource "check-resource" failed.', level="error"
        )


class TestUnexpectedFailures(WebCheckTestCase):
    def test_unexpected_connector_failure_is_reported_and_logged_safely(self):
        self.uri_validator.side_effect = RuntimeError(POSTGRESQL_URI)
        self.login_staff()
        with mock.patch("gaodcore_manager.web_views.logger") as logger:
            response = self.client.post(self.connector_check_url)
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
            fetch_redirect_response=False,
        )
        followed = self.client.get(response["Location"])
        self.assertRenderedMessage(
            followed,
            'An unexpected error occurred while checking connector "check-connector". '
            "Consult the application logs.",
            level="error",
        )
        self.assertNoSecretsInMessages(followed)
        logger.error.assert_called_once()
        args, kwargs = logger.error.call_args
        self.assertNotIn("exc_info", kwargs)
        logged = " ".join(str(arg) for arg in args)
        self.assertIn("connector", logged)
        self.assertIn(str(self.connector.pk), logged)
        self.assertIn("RuntimeError", logged)
        self.assertNotIn(URI_PASSWORD, logged)
        self.assertNotIn(POSTGRESQL_URI, logged)
        self.assertNotIn(self.connector.name, logged)

    def test_unexpected_resource_failure_is_reported_and_logged_safely(self):
        self.resource_validator.side_effect = RuntimeError(POSTGRESQL_URI)
        self.login_staff()
        with mock.patch("gaodcore_manager.web_views.logger") as logger:
            response = self.client.post(self.resource_check_url, follow=True)
        self.assertRenderedMessage(
            response,
            'An unexpected error occurred while checking resource "check-resource". '
            "Consult the application logs.",
            level="error",
        )
        self.assertNoSecretsInMessages(response)
        logger.error.assert_called_once()
        args, kwargs = logger.error.call_args
        self.assertNotIn("exc_info", kwargs)
        logged = " ".join(str(arg) for arg in args)
        self.assertIn("resource", logged)
        self.assertIn(str(self.resource.pk), logged)
        self.assertIn("RuntimeError", logged)
        self.assertNotIn(URI_PASSWORD, logged)
        self.assertNotIn("some_table", logged)
