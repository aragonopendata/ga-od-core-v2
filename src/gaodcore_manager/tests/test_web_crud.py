"""Focused tests for the manager web CRUD views.

External probes are monkeypatched at `gaodcore_manager.validators`, so no real
database or network access is required.
"""

import re
from unittest import mock

from django.contrib.auth.models import User
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils.html import escape

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

    def assertRenderedMessage(self, response, expected):
        """The destination page must render the success notification itself."""
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "manager-messages__item--success")
        self.assertContains(response, escape(expected))


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

    def test_create_form_renders_uri_as_a_textarea(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:connector-create"))
        content = response.content.decode()
        match = re.search(r'<textarea[^>]*name="uri"[^>]*>', content)
        self.assertIsNotNone(match)

    def test_update_form_renders_uri_as_a_textarea_with_the_current_value(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk})
        )
        content = response.content.decode()
        match = re.search(
            r'<textarea[^>]*name="uri"[^>]*>(.*?)</textarea>', content, re.DOTALL
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1).strip(), self.connector.uri)

    def test_create_accepts_a_long_uri_unchanged_through_the_textarea(self):
        self.login_staff()
        long_uri = POSTGRESQL_URI + "-" + ("x" * 500)
        response = self.client.post(
            reverse("manager_web:connector-create"),
            {"name": "long-uri-connector", "uri": long_uri, "enabled": "on"},
        )
        created = ConnectorConfig.objects.get(name="long-uri-connector")
        self.assertRedirects(
            response,
            reverse("manager_web:connector-detail", kwargs={"pk": created.pk}),
            fetch_redirect_response=False,
        )
        self.assertEqual(created.uri, long_uri)
        self.assertEqual(self.validate_uri.call_count, 1)

    def test_update_accepts_a_long_uri_unchanged_through_the_textarea(self):
        self.login_staff()
        long_uri = POSTGRESQL_URI + "-" + ("y" * 500)
        response = self.client.post(
            reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk}),
            {"name": "crud-connector", "uri": long_uri, "enabled": "on"},
        )
        self.assertEqual(response.status_code, 302)
        self.connector.refresh_from_db()
        self.assertEqual(self.connector.uri, long_uri)
        self.assertEqual(self.validate_uri.call_count, 1)

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
            fetch_redirect_response=False,
        )
        self.assertEqual(self.validate_uri.call_count, 1)
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": created.pk})
        )
        self.assertRenderedMessage(response, 'The connector "new-connector" has been created successfully.')

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
        self.assertRedirects(
            response, reverse("manager_web:connector-list"), fetch_redirect_response=False
        )
        self.assertFalse(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())
        self.assertFalse(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        response = self.client.get(reverse("manager_web:connector-list"))
        self.assertRenderedMessage(response, 'The connector "crud-connector" has been deleted successfully.')


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
            response,
            reverse("manager_web:resource-detail", kwargs={"pk": created.pk}),
            fetch_redirect_response=False,
        )
        self.assertEqual(self.validate_resource.call_count, 1)
        # Empty optional fields are stored as NULL, like the REST API does.
        self.assertIsNone(created.object_location_schema)
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": created.pk})
        )
        self.assertRenderedMessage(response, 'The resource "new-resource" has been created successfully.')

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
        self.assertRedirects(
            response, reverse("manager_web:resource-list"), fetch_redirect_response=False
        )
        self.assertFalse(ResourceConfig.objects.filter(pk=self.resource.pk).exists())
        self.assertTrue(ConnectorConfig.objects.filter(pk=self.connector.pk).exists())
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertRenderedMessage(response, 'The resource "crud-resource" has been deleted successfully.')


class TestDeletionModal(WebCrudTestCase):
    def test_list_and_detail_pages_expose_the_modal_and_its_triggers(self):
        self.login_staff()
        pages = [
            reverse("manager_web:resource-list"),
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
            reverse("manager_web:connector-list"),
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
        ]
        for url in pages:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertContains(response, 'id="delete-modal"')
                self.assertContains(response, "data-delete-url=")
                # The destructive action is a CSRF protected POST, never a GET link.
                self.assertContains(response, "csrfmiddlewaretoken")

    def test_connector_triggers_warn_about_the_resource_cascade(self):
        self.login_staff()
        for url in (
            reverse("manager_web:connector-list"),
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
        ):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertContains(
                    response,
                    escape("Its 1 associated resource will also be deleted."),
                )

    def test_connector_cascade_warning_uses_the_plural_form(self):
        ResourceConfig.objects.create(
            name="crud-resource-2",
            connector_config=self.connector,
            enabled=True,
            object_location="another_table",
        )
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk})
        )
        self.assertContains(
            response, escape("Its 2 associated resources will also be deleted.")
        )

    def test_resource_triggers_carry_no_cascade_warning(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        )
        self.assertContains(response, "data-delete-url=")
        self.assertNotContains(response, "data-delete-warning=")

    def test_edit_to_disable_link_targets_are_populated_for_list_and_detail_triggers(self):
        self.login_staff()
        resource_edit_url = reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk})
        connector_edit_url = reverse("manager_web:connector-update", kwargs={"pk": self.connector.pk})
        pages = [
            reverse("manager_web:resource-list"),
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
        ]
        for url in pages:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertContains(response, f'data-edit-url="{resource_edit_url}"')

        pages = [
            reverse("manager_web:connector-list"),
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
        ]
        for url in pages:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertContains(response, f'data-edit-url="{connector_edit_url}"')

    def test_modal_includes_the_edit_to_disable_action(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertContains(response, 'id="delete-modal-edit"')
        self.assertContains(response, "Edit to disable")

    def test_resource_advisory_text_is_present(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertContains(
            response,
            escape('If you only want to stop publishing this resource, edit it and clear "Enabled".'),
        )

    def test_connector_advisory_text_is_present(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:connector-list"))
        self.assertContains(
            response,
            escape(
                "Disabling the connector makes all its resources unavailable "
                "without deleting their configuration."
            ),
        )

    def test_list_row_edit_and_delete_controls_are_icon_only_with_accessible_labels(self):
        self.login_staff()
        resource_edit_url = reverse("manager_web:resource-update", kwargs={"pk": self.resource.pk})
        resource_delete_url = reverse("manager_web:resource-delete", kwargs={"pk": self.resource.pk})
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertIn(f'href="{resource_edit_url}"', content)
        self.assertIn(f'data-delete-url="{resource_delete_url}"', content)
        self.assertIn(escape(f"Edit {self.resource.name}"), content)
        self.assertIn(escape(f"Delete {self.resource.name}"), content)
        self.assertIn("<svg", content)
        # The row action cell only renders icon buttons, not the plain text
        # ones (the modal's own "Delete" confirm button is unrelated to this).
        actions_cell = content[content.index('class="manager-actions"'):content.index("</td>")]
        self.assertNotIn(">Edit<", actions_cell)
        self.assertNotIn(">Delete<", actions_cell)

    def test_detail_pages_keep_text_action_buttons(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        )
        self.assertContains(response, ">Edit<")
        self.assertContains(response, ">Delete<")

    def test_connector_list_counts_resources_without_a_query_per_row(self):
        """The cascade warning must not cost one COUNT per connector."""
        self.login_staff()
        url = reverse("manager_web:connector-list")

        with CaptureQueriesContext(connection) as baseline:
            self.assertEqual(self.client.get(url).status_code, 200)

        for index in range(5):
            connector = ConnectorConfig.objects.create(
                name=f"bulk-connector-{index}",
                uri=f"{POSTGRESQL_URI}-bulk-{index}",
                enabled=True,
            )
            ResourceConfig.objects.create(
                name=f"bulk-resource-{index}",
                connector_config=connector,
                enabled=True,
                object_location="t",
            )

        with CaptureQueriesContext(connection) as grown:
            self.assertEqual(self.client.get(url).status_code, 200)

        self.assertEqual(len(grown), len(baseline))


class TestSpanishTranslations(WebCrudTestCase):
    """The modal and notification strings must be reachable through the catalogue."""

    def setUp(self):
        super().setUp()
        self.client.defaults["HTTP_ACCEPT_LANGUAGE"] = "es"

    def test_modal_strings_are_translated(self):
        self.login_staff()
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk})
        )
        self.assertContains(response, "Confirmar eliminación")
        self.assertContains(
            response,
            escape(
                '¿Seguro que quieres eliminar "crud-connector"? '
                "Esta acción no se puede deshacer."
            ),
        )
        self.assertContains(
            response, escape("Se eliminará también su 1 recurso asociado.")
        )

    def test_success_messages_are_translated(self):
        self.login_staff()
        self.client.post(
            reverse("manager_web:resource-delete", kwargs={"pk": self.resource.pk})
        )
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertContains(
            response, escape('El recurso "crud-resource" se ha eliminado correctamente.')
        )

    def test_form_actions_are_translated(self):
        self.login_staff()
        response = self.client.get(reverse("manager_web:resource-create"))
        self.assertContains(response, "Nuevo recurso")
        self.assertContains(response, "Guardar")
        self.assertContains(response, "Cancelar")
