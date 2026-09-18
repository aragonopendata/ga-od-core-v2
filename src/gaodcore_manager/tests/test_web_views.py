from django.contrib.auth.models import User
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.db import connection
from django.urls import reverse

from gaodcore_manager.models import ConnectorConfig, ResourceConfig

PRIVATE_PREFIX = "/admin/GA_OD_Core_admin/"


class WebManagerTestCase(TestCase):
    def setUp(self):
        self.connector = ConnectorConfig.objects.create(
            name="test-connector",
            uri="postgresql://user:secret-password@localhost/db",
            enabled=True,
        )
        self.resource = ResourceConfig.objects.create(
            name="test-resource",
            connector_config=self.connector,
            enabled=True,
            object_location="some_table",
        )
        self.staff_user = User.objects.create_user("staff", password="pw", is_staff=True)
        self.regular_user = User.objects.create_user("regular", password="pw", is_staff=False)


class TestRouteNamesResolveToExactPaths(WebManagerTestCase):
    def test_route_names_reverse_to_the_exact_required_paths(self):
        expected = [
            ("manager_web:resource-list", {}, f"{PRIVATE_PREFIX}web/manager/"),
            (
                "manager_web:resource-detail",
                {"pk": self.resource.pk},
                f"{PRIVATE_PREFIX}web/manager/resources/{self.resource.pk}/",
            ),
            ("manager_web:connector-list", {}, f"{PRIVATE_PREFIX}web/manager/connectors/"),
            (
                "manager_web:connector-detail",
                {"pk": self.connector.pk},
                f"{PRIVATE_PREFIX}web/manager/connectors/{self.connector.pk}/",
            ),
        ]
        for name, kwargs, expected_path in expected:
            with self.subTest(name=name):
                self.assertEqual(reverse(name, kwargs=kwargs), expected_path)


class TestAccessControl(WebManagerTestCase):
    def _urls(self):
        return [
            reverse("manager_web:resource-list"),
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}),
            reverse("manager_web:connector-list"),
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}),
        ]

    def test_anonymous_users_are_redirected_to_the_admin_login(self):
        for url in self._urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}admin/login/"))
                self.assertIn(f"next={url}", response.url)

    def test_non_staff_users_cannot_view_any_page(self):
        self.client.force_login(self.regular_user)
        for url in self._urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.url.startswith(f"{PRIVATE_PREFIX}admin/login/"))

    def test_staff_users_receive_200_for_all_pages(self):
        self.client.force_login(self.staff_user)
        for url in self._urls():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)


class TestStandaloneTemplate(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_manager_pages_use_local_stylesheet_and_title(self):
        for url in (
            reverse("manager_web:resource-list"),
            reverse("manager_web:connector-list"),
        ):
            with self.subTest(url=url):
                response = self.client.get(url)
                content = response.content.decode()
                self.assertIn("gaodcore_manager/manager.css", content)
                self.assertIn("GA OD Core Manager", content)

    def test_manager_pages_do_not_load_django_admin_assets(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertNotIn("admin/css/base.css", content)
        self.assertNotIn("admin/js/", content)

    def test_manager_pages_have_no_admin_sidebar(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertNotIn("nav-sidebar", content)

    def test_navigation_tabs_present(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertIn(">Recursos<", content)
        self.assertIn(">Conectores<", content)
        self.assertIn(reverse("manager_web:resource-list"), content)
        self.assertIn(reverse("manager_web:connector-list"), content)


class TestBreadcrumbs(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_resource_list_breadcrumb(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertIn("Manager", content)
        self.assertIn("Recursos", content)

    def test_connector_list_breadcrumb(self):
        response = self.client.get(reverse("manager_web:connector-list"))
        content = response.content.decode()
        self.assertIn("Manager", content)
        self.assertIn("Conectores", content)

    def test_resource_detail_breadcrumb_includes_object_name(self):
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        )
        content = response.content.decode()
        self.assertIn(self.resource.name, content)
        self.assertIn(
            reverse("manager_web:resource-list"), content
        )

    def test_connector_detail_breadcrumb_includes_object_name(self):
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk})
        )
        content = response.content.decode()
        self.assertIn(self.connector.name, content)
        self.assertIn(
            reverse("manager_web:connector-list"), content
        )


class TestResourceList(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_resource_list_displays_resources_and_links(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        content = response.content.decode()
        self.assertIn(self.resource.name, content)
        self.assertIn(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk}), content
        )
        self.assertIn(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}), content
        )

    def test_resource_list_queryset_uses_select_related(self):
        ConnectorConfig.objects.create(name="c2", uri="postgresql://x/y", enabled=True)
        for i in range(3):
            connector = ConnectorConfig.objects.create(name=f"connector-{i}", uri=f"postgresql://x/y{i}", enabled=True)
            ResourceConfig.objects.create(
                name=f"resource-{i}", connector_config=connector, enabled=True, object_location="t"
            )

        with CaptureQueriesContext(connection) as ctx:
            response = self.client.get(reverse("manager_web:resource-list"))
        self.assertEqual(response.status_code, 200)
        # One query for resources+join, plus session/user lookups; must not scale with row count.
        self.assertLess(len(ctx.captured_queries), 10)

    def test_resource_list_pagination(self):
        connector = ConnectorConfig.objects.create(name="pag-connector", uri="postgresql://x/y", enabled=True)
        for i in range(60):
            ResourceConfig.objects.create(
                name=f"pag-resource-{i}", connector_config=connector, enabled=True, object_location="t"
            )
        # 1 (setUp) + 60 = 61 total resources.
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["resources"]), 50)
        self.assertTrue(response.context["is_paginated"])
        self.assertNotIn("Previous", response.content.decode())
        self.assertIn("Next", response.content.decode())

        response_page_2 = self.client.get(reverse("manager_web:resource-list") + "?page=2")
        self.assertEqual(response_page_2.status_code, 200)
        self.assertEqual(len(response_page_2.context["resources"]), 11)
        self.assertIn("Previous", response_page_2.content.decode())
        self.assertNotIn("Next", response_page_2.content.decode())

    def test_resource_list_pagination_uses_deterministic_id_ordering(self):
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertEqual(list(response.context["resources"].query.order_by), ["id"])

    def test_resource_list_empty_state(self):
        ResourceConfig.objects.all().delete()
        response = self.client.get(reverse("manager_web:resource-list"))
        self.assertContains(response, "No resources found.")

    def test_resource_list_search_by_name(self):
        ResourceConfig.objects.create(
            name="other-resource", connector_config=self.connector, enabled=True, object_location="t"
        )
        response = self.client.get(reverse("manager_web:resource-list"), {"q": "test-resource"})
        names = [resource.name for resource in response.context["resources"]]
        self.assertEqual(names, ["test-resource"])

    def test_resource_list_filter_by_enabled(self):
        ResourceConfig.objects.create(
            name="disabled-resource", connector_config=self.connector, enabled=False, object_location="t"
        )
        response = self.client.get(reverse("manager_web:resource-list"), {"enabled": "0"})
        names = [resource.name for resource in response.context["resources"]]
        self.assertEqual(names, ["disabled-resource"])

        response = self.client.get(reverse("manager_web:resource-list"), {"enabled": "1"})
        names = [resource.name for resource in response.context["resources"]]
        self.assertEqual(names, ["test-resource"])

    def test_resource_list_search_and_filter_persist_in_pagination_links(self):
        connector = ConnectorConfig.objects.create(name="pag-connector-2", uri="postgresql://x/y", enabled=True)
        for i in range(60):
            ResourceConfig.objects.create(
                name=f"matching-{i}", connector_config=connector, enabled=True, object_location="t"
            )
        response = self.client.get(
            reverse("manager_web:resource-list"), {"q": "matching", "enabled": "1"}
        )
        content = response.content.decode()
        self.assertIn("q=matching", content)
        self.assertIn("enabled=1", content)


class TestResourceDetail(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_resource_detail_shows_required_fields(self):
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": self.resource.pk})
        )
        content = response.content.decode()
        self.assertIn(str(self.resource.id), content)
        self.assertIn(self.resource.name, content)
        self.assertIn(self.connector.name, content)
        self.assertIn(self.resource.object_location, content)
        self.assertIn(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}), content
        )

    def test_resource_detail_shows_em_dash_for_missing_nullable_values(self):
        resource = ResourceConfig.objects.create(
            name="api-resource",
            connector_config=self.connector,
            enabled=False,
            object_location=None,
            object_location_schema=None,
        )
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": resource.pk})
        )
        self.assertContains(response, "—")

    def test_resource_detail_missing_object_returns_404(self):
        response = self.client.get(
            reverse("manager_web:resource-detail", kwargs={"pk": 999999})
        )
        self.assertEqual(response.status_code, 404)


class TestConnectorList(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_connector_list_displays_connectors_without_uri(self):
        response = self.client.get(reverse("manager_web:connector-list"))
        content = response.content.decode()
        self.assertIn(self.connector.name, content)
        self.assertIn(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk}), content
        )
        self.assertNotIn(self.connector.uri, content)
        self.assertNotIn("secret-password", content)

    def test_connector_list_missing_object_returns_404(self):
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": 999999})
        )
        self.assertEqual(response.status_code, 404)

    def test_connector_list_pagination(self):
        for i in range(60):
            ConnectorConfig.objects.create(name=f"pag-connector-{i}", uri=f"postgresql://x/y{i}", enabled=True)
        # 1 (setUp) + 60 = 61 total connectors.
        response = self.client.get(reverse("manager_web:connector-list"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["connectors"]), 50)
        self.assertTrue(response.context["is_paginated"])
        self.assertNotIn("Previous", response.content.decode())
        self.assertIn("Next", response.content.decode())

        response_page_2 = self.client.get(reverse("manager_web:connector-list") + "?page=2")
        self.assertEqual(response_page_2.status_code, 200)
        self.assertEqual(len(response_page_2.context["connectors"]), 11)
        self.assertIn("Previous", response_page_2.content.decode())
        self.assertNotIn("Next", response_page_2.content.decode())

    def test_connector_list_pagination_uses_deterministic_id_ordering(self):
        response = self.client.get(reverse("manager_web:connector-list"))
        self.assertEqual(list(response.context["connectors"].query.order_by), ["id"])

    def test_connector_list_empty_state(self):
        ResourceConfig.objects.all().delete()
        ConnectorConfig.objects.all().delete()
        response = self.client.get(reverse("manager_web:connector-list"))
        self.assertContains(response, "No connectors found.")

    def test_connector_list_search_by_name(self):
        ConnectorConfig.objects.create(name="other-connector", uri="postgresql://x/y", enabled=True)
        response = self.client.get(reverse("manager_web:connector-list"), {"q": "test-connector"})
        names = [connector.name for connector in response.context["connectors"]]
        self.assertEqual(names, ["test-connector"])

    def test_connector_list_filter_by_enabled(self):
        ConnectorConfig.objects.create(name="disabled-connector", uri="postgresql://x/y", enabled=False)
        response = self.client.get(reverse("manager_web:connector-list"), {"enabled": "0"})
        names = [connector.name for connector in response.context["connectors"]]
        self.assertEqual(names, ["disabled-connector"])

        response = self.client.get(reverse("manager_web:connector-list"), {"enabled": "1"})
        names = [connector.name for connector in response.context["connectors"]]
        self.assertEqual(names, ["test-connector"])

    def test_connector_list_search_does_not_leak_uri(self):
        response = self.client.get(reverse("manager_web:connector-list"), {"q": "test"})
        content = response.content.decode()
        self.assertNotIn(self.connector.uri, content)
        self.assertNotIn("secret-password", content)


class TestConnectorDetail(WebManagerTestCase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.staff_user)

    def test_connector_detail_masks_uri_and_includes_reveal_script(self):
        response = self.client.get(
            reverse("manager_web:connector-detail", kwargs={"pk": self.connector.pk})
        )
        content = response.content.decode()
        self.assertIn("Show URI", content)
        self.assertIn("connector-uri-toggle", content)
        self.assertIn("connector-uri-masked", content)
        # The raw URI must not appear outside of the json_script payload.
        self.assertIn(self.connector.uri, content)
        self.assertIn('id="connector-uri-data"', content)
        self.assertIn("JSON.parse", content)
        self.assertNotIn(f'value="{self.connector.uri}"', content)
        self.assertNotIn(f'data-uri="{self.connector.uri}"', content)


class TestExistingRoutesUnaffected(TestCase):
    def test_rest_api_routes_still_resolve_to_original_views(self):
        from django.urls import resolve

        from gaodcore_manager.views import ConnectorConfigView, ResourceConfigView

        expected = {
            f"{PRIVATE_PREFIX}manager/connector-config/": ConnectorConfigView,
            f"{PRIVATE_PREFIX}manager/resource-config/": ResourceConfigView,
        }
        for url, view_class in expected.items():
            with self.subTest(url=url):
                resolver_match = resolve(url)
                self.assertEqual(resolver_match.func.cls, view_class)

    def test_swagger_and_admin_routes_remain(self):
        for url in (
            f"{PRIVATE_PREFIX}ui/",
            f"{PRIVATE_PREFIX}ui/schema/",
            f"{PRIVATE_PREFIX}admin/",
        ):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertIn(response.status_code, (200, 302))
