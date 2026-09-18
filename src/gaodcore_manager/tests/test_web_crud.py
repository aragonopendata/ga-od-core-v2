"""Focused tests for the manager web CRUD views.

External probes are monkeypatched at `gaodcore_manager.validators`, so no real
database or network access is required.
"""

from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

from gaodcore_manager import validators
from gaodcore_manager.models import ConnectorConfig, ResourceConfig

PRIVATE_PREFIX = "/admin/GA_OD_Core_admin/"
POSTGRESQL_URI = "postgresql://user:secret@localhost:5432/db"


class WebCrudTestCase(TestCase):
    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="crud-connector", uri=POSTGRESQL_URI, enabled=True
        )
        self.other_connector = ConnectorConfig.objects.create(
            name="crud-connector-other", uri=POSTGRESQL_URI + "-other", enabled=True
        )
        self.resource = ResourceConfig.objects.create(
            name="crud-resource",
            connector_config=self.connector,
            enabled=True,
            object_location="some_table",
        )
        self.staff_user = User.objects.create_user("staff", password="pw", is_staff=True)
        self.regular_user = User.objects.create_user(
            "regular", password="pw", is_staff=False
        )
        # Both probes are replaced for the whole test case; each test asserts on
        # the call counts it cares about.
        patch_uri = mock.patch.object(validators, "validate_uri")
        patch_resource = mock.patch.object(
            validators, "validate_resource", return_value=[{"id": 1}]
        )
        self.validate_uri = patch_uri.start()
        self.validate_resource = patch_resource.start()
        self.addCleanup(patch_uri.stop)
        self.addCleanup(patch_resource.stop)
        # Assert on the English source strings regardless of the Spanish default
        # language, so the Phase 3 translations cannot break these tests.
        self.client.defaults["HTTP_ACCEPT_LANGUAGE"] = "en"

    def login_staff(self):
        self.client.force_login(self.staff_user)


class TestRoutes(WebCrudTestCase):
    def test_crud_route_names_reverse_to_the_expected_paths(self):
        expected = [
            ("manager_web:resource-create", {}, f"{PRIVATE_PREFIX}web/manager/resources/new/"),
            (
                "manager_web:resource-update",
                {"pk": self.resource.pk},
                f"{PRIVATE_PREFIX}web/manager/resources/{self.resource.pk}/edit/",
            ),
            (
                "manager_web:resource-delete",
                {"pk": self.resource.pk},
                f"{PRIVATE_PREFIX}web/manager/resources/{self.resource.pk}/delete/",
            ),
            ("manager_web:connector-create", {}, f"{PRIVATE_PREFIX}web/manager/connectors/new/"),
            (
                "manager_web:connector-update",
                {"pk": self.connector.pk},
                f"{PRIVATE_PREFIX}web/manager/connectors/{self.connector.pk}/edit/",
            ),
            (
                "manager_web:connector-delete",
                {"pk": self.connector.pk},
                f"{PRIVATE_PREFIX}web/manager/connectors/{self.connector.pk}/delete/",
            ),
        ]
        for name, kwargs, expected_path in expected:
            with self.subTest(name=name):
                self.assertEqual(reverse(name, kwargs=kwargs), expected_path)


class TestAccessControl(WebCrudTestCase):
    def _get_urls(self):
        return [
            reverse("manager_web:resource-create"),
            reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk}),
            reverse("manager_web:connector-create"),
            reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk}),
        ]

    def _delete_urls(self):
        return [
            reverse("manager_web:resource-delete", kwargs={"pk": self.resource.pk}),
            reverse("manager_web:connector-delete", kwargs={"pk": self.connector.pk}),
        ]

    def test_anonymous_users_are_redirected_to_the_admin_login(self):
        for url in self._get_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertIn("/login", response["Location"])

    def test_non_staff_users_are_redirected_to_the_admin_login(self):
        self.client.force_login(self.regular_user)
        for url in self._get_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertIn("/login", response["Location"])

    def test_anonymous_post_does_not_delete_anything(self):
        for url in self._delete_urls():
            with self.subTest(url=url):
                response = self.client.post(url)
                self.assertEqual(response.status_code, 302)
        self.assertTrue(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        self.assertTrue(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())

    def test_delete_rejects_get(self):
        self.login_staff()
        for url in self._delete_urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 405)
        self.assertTrue(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        self.assertTrue(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())


class TestConnectorCrud(WebCrudTestCase):
    def test_create_form_renders(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:connector-create"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "csrfmiddlewaretoken")

    def test_create_redirects_to_detail_and_reports_success(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:connector-create"),
            {"name": "new-connector", "uri": POSTGRESQL_URI + "-new", "enabled": "on"},
        )
        created = ConnectorConfig.objects.get(name="new-connector")
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": created.pk}),
        )
        self.assertEqual(self.validate_uri.call_count, 1)
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": created.pk})
        )
        messages = [str(item) for item in response.context["messages"]]
        self.assertEqual(
            messages, ['The connector "new-connector" has been created successfully.']
        )

    def test_create_reports_external_failure_as_a_form_error(self):
        from connectors import DriverConnectionError

        self.validate_uri.side_effect = DriverConnectionError()
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:connector-create"),
            {"name": "broken", "uri": POSTGRESQL_URI + "-broken", "enabled": "on"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("uri", response.context["form"].errors)
        self.assertFalse(ConnectorConfig.objects.filter(name="broken").exists())

    def test_update_of_metadata_only_does_no_external_io(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk}),
            {"name": "crud-connector-renamed", "uri": POSTGRESQL_URI},
        )
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
        )
        self.validate_uri.assert_not_called()
        self.connector.refresh_from_db()
        self.assertEqual(self.connector.name, "crud-connector-renamed")
        self.assertFalse(self.connector.enabled)

    def test_update_of_uri_triggers_external_validation(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk}),
            {"name": "crud-connector", "uri": POSTGRESQL_URI + "-changed", "enabled": "on"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.validate_uri.call_count, 1)

    def test_delete_removes_the_connector_and_its_resources(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:connector-delete", kwargs={"pk": self.connector.pk})
        )
        self.assertRedirects(response, reverse("manager_web:connector-list"))
        self.assertFalse(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())
        self.assertFalse(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        response = self.client.get(reverse("manager_web:connector-list"))
        messages = [str(item) for item in response.context["messages"]]
        self.assertEqual(
            messages, ['The connector "crud-connector" has been deleted successfully.']
        )


class TestResourceCrud(WebCrudTestCase):
    def _payload(self, **overrides):
        payload = {
            "name": "new-resource",
            "connector_config": self.connector.pk,
            "enabled": "on",
            "object_location": "a_table",
            "object_location_schema": "",
        }
        payload.update(overrides)
        return payload

    def test_create_redirects_to_detail_and_reports_success(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-create"), self._payload()
        )
        created = ResourceConfig.objects.get(name="new-resource")
        self.assertRedirects(
            response, reverse("manager_web:resource-detail", kwargs={"pk": created.pk})
        )
        self.assertEqual(self.validate_resource.call_count, 1)
        # Empty optional fields are stored as NULL, like the REST API does.
        self.assertIsNone(created.object_location_schema)
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": created.pk})
        )
        messages = [str(item) for item in response.context["messages"]]
        self.assertEqual(
            messages, ['The resource "new-resource" has been created successfully.']
        )

    def test_local_rules_reject_a_postgresql_resource_without_object_location(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-create"),
            self._payload(object_location=""),
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "Object location is not filled.",
            response.context["form"].non_field_errors(),
        )
        self.assertFalse(ResourceConfig.objects.filter(name="new-resource").exists())

    def test_local_rules_run_when_external_validation_is_disabled(self):
        self.login_staff()
        with self.settings(GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS=False):
            response = self.client.post(
                reverse("manager_web:resource-create"),
                self._payload(object_location=""),
            )
        self.assertEqual(response.status_code, 200)
        self.validate_resource.assert_not_called()
        self.assertIn(
            "Object location is not filled.",
            response.context["form"].non_field_errors(),
        )

    def test_create_skips_the_probe_when_external_validation_is_disabled(self):
        self.login_staff()
        with self.settings(GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS=False):
            response = self.client.post(
                reverse("manager_web:resource-create"), self._payload()
            )
        self.assertEqual(response.status_code, 302)
        self.validate_resource.assert_not_called()
        self.assertTrue(ResourceConfig.objects.filter(name="new-resource").exists())

    def test_update_of_metadata_only_does_no_external_io(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk}),
            self._payload(
                name="crud-resource-renamed",
                object_location="some_table",
                enabled="",
            ),
        )
        self.assertRedirects(
            response,
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
        )
        self.validate_resource.assert_not_called()
        self.resource.refresh_from_db()
        self.assertEqual(self.resource.name, "crud-resource-renamed")
        self.assertFalse(self.resource.enabled)

    def test_update_of_object_location_triggers_external_validation(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk}),
            self._payload(name="crud-resource", object_location="another_table"),
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.validate_resource.call_count, 1)

    def test_update_of_connector_triggers_external_validation(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk}),
            self._payload(
                name="crud-resource",
                connector_config=self.other_connector.pk,
                object_location="some_table",
            ),
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.validate_resource.call_count, 1)

    def test_delete_redirects_to_the_list_and_reports_success(self):
        self.login_staff()
        response = self.client.post(
            reverse("manager_web:resource-delete", kwargs={"pk": self.resource.pk})
        )
        self.assertRedirects(response, reverse("manager_web:resource-list"))
        self.assertFalse(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        self.assertTrue(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())
        response = self.client.get(reverse("manager_web:resource-list"))
        messages = [str(item) for item in response.context["messages"]]
        self.assertEqual(
            messages, ['The resource "crud-resource" has been deleted successfully.']
        )
