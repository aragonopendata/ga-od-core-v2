"""URL configuration tests for the private ``/admin/GA_OD_Core_admin/`` base path.

The private surface (Django admin, manager API, health API, private schema and
Swagger UI) is served under ``/admin/GA_OD_Core_admin/``. The public API keeps
its own ``/GA_OD_Core/`` prefix and is untouched by that move.
"""
from urllib.parse import parse_qs, urlsplit

from django.contrib.auth.models import User
from django.test import RequestFactory, TestCase
from django.urls import NoReverseMatch, Resolver404, resolve, reverse

from gaodcore_project.schema_views import AdminSwaggerView
from gaodcore_project.spectacular_hooks import admin_preprocessing_hook, custom_preprocessing_hook

PRIVATE_PREFIX = '/admin/GA_OD_Core_admin/'
OLD_PRIVATE_PREFIX = '/GA_OD_Core_admin/'


class TestPrivateBasePathResolves(TestCase):
    """Every private entry point lives under the new base path."""

    def test_reverse_uses_the_new_private_prefix(self):
        """`reverse()` (and therefore redirects, forms and the admin) emits the new prefix."""
        expected = {
            'admin-index': PRIVATE_PREFIX,
            'admin-schema': f'{PRIVATE_PREFIX}ui/schema/',
            'admin-schema-swagger-ui': f'{PRIVATE_PREFIX}ui/',
            'admin:index': f'{PRIVATE_PREFIX}admin/',
            'admin:login': f'{PRIVATE_PREFIX}admin/login/',
        }
        for name, url in expected.items():
            with self.subTest(name=name):
                self.assertEqual(reverse(name), url)

    def test_main_private_endpoints_resolve(self):
        """The admin, manager, health, schema and Swagger URLs all resolve."""
        for url in (
                PRIVATE_PREFIX,
                f'{PRIVATE_PREFIX}admin/',
                f'{PRIVATE_PREFIX}manager/connector-config/',
                f'{PRIVATE_PREFIX}manager/resource-config/',
                f'{PRIVATE_PREFIX}manager/validator',
                f'{PRIVATE_PREFIX}health/api/status/',
                f'{PRIVATE_PREFIX}ui/',
                f'{PRIVATE_PREFIX}ui/schema/',
        ):
            with self.subTest(url=url):
                self.assertIsNotNone(resolve(url))

    def test_private_base_path_redirects_to_the_html_manager(self):
        """The private base path redirects to the HTML manager resource list."""
        response = self.client.get(PRIVATE_PREFIX)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('manager_web:resource-list'))

    def test_anonymous_user_following_the_private_root_reaches_the_custom_login_with_next(self):
        """Anonymous users bounce through the manager, to the private interface's own login, and back."""
        response = self.client.get(PRIVATE_PREFIX, follow=True)
        self.assertEqual(response.status_code, 200)
        final_url, _status = response.redirect_chain[-1]
        self.assertTrue(final_url.startswith(f'{PRIVATE_PREFIX}login/'))
        query = parse_qs(urlsplit(final_url).query)
        self.assertEqual(query['next'], [reverse('manager_web:resource-list')])


class TestOldPrivateBasePathIsGone(TestCase):
    """The old ``/GA_OD_Core_admin/`` path is removed, with no alias or redirect."""

    def test_old_paths_do_not_resolve(self):
        for url in (
                OLD_PRIVATE_PREFIX,
                f'{OLD_PRIVATE_PREFIX}admin/',
                f'{OLD_PRIVATE_PREFIX}manager/connector-config/',
                f'{OLD_PRIVATE_PREFIX}health/api/status/',
                f'{OLD_PRIVATE_PREFIX}ui/',
                f'{OLD_PRIVATE_PREFIX}ui/schema/',
        ):
            with self.subTest(url=url):
                with self.assertRaises(Resolver404):
                    resolve(url)

    def test_old_paths_return_404(self):
        """No compatibility redirect is served either."""
        for url in (OLD_PRIVATE_PREFIX, f'{OLD_PRIVATE_PREFIX}ui/', f'{OLD_PRIVATE_PREFIX}manager/validator'):
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 404)


class TestAdminAuthenticationRedirects(TestCase):
    """Authentication redirects point at the new private login URL."""

    def test_django_admin_redirects_anonymous_users_to_the_new_login(self):
        response = self.client.get(f'{PRIVATE_PREFIX}admin/')
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith(f'{PRIVATE_PREFIX}admin/login/'))

    def test_private_docs_redirect_anonymous_users_to_the_new_login(self):
        for url in (f'{PRIVATE_PREFIX}ui/', f'{PRIVATE_PREFIX}ui/schema/'):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.url.startswith(f'{PRIVATE_PREFIX}admin/login/'))

    def test_admin_login_page_is_served_under_the_new_path(self):
        self.assertEqual(self.client.get(f'{PRIVATE_PREFIX}admin/login/').status_code, 200)


class TestPrivateSwaggerUsesTheNewSchemaUrl(TestCase):
    """The private Swagger UI points at the private schema under the new prefix."""

    def setUp(self):
        self.client.force_login(User.objects.create_user('staff-user', password='a-test-password', is_staff=True))

    def test_swagger_ui_references_the_new_schema_url(self):
        response = self.client.get(f'{PRIVATE_PREFIX}ui/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(f'url: "{PRIVATE_PREFIX}ui/schema/"', response.content.decode())

    def test_swagger_view_builds_the_absolute_schema_url_from_the_new_prefix(self):
        """`AdminSwaggerView.get_schema_url()` reverses `admin-schema` under the new prefix."""
        view = AdminSwaggerView()
        view.request = RequestFactory().get(f'{PRIVATE_PREFIX}ui/')
        self.assertEqual(view.get_schema_url(), f'http://testserver{PRIVATE_PREFIX}ui/schema/')

    def test_private_schema_lists_private_paths_under_the_new_prefix(self):
        response = self.client.get(f'{PRIVATE_PREFIX}ui/schema/', HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 200)
        paths = response.json()['paths']
        self.assertIn(f'{PRIVATE_PREFIX}manager/connector-config/', paths)
        self.assertFalse([path for path in paths if path.startswith(OLD_PRIVATE_PREFIX)])


class TestPublicRoutesAreUnchanged(TestCase):
    """Nothing below ``/GA_OD_Core/`` moved."""

    def test_public_reverse_is_unchanged(self):
        self.assertEqual(reverse('schema'), '/GA_OD_Core/ui/schema/')
        self.assertEqual(reverse('schema-swagger-ui'), '/GA_OD_Core/ui/')

    def test_public_docs_remain_anonymous(self):
        for url in ('/GA_OD_Core/ui/', '/GA_OD_Core/ui/schema/'):
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 200)

    def test_public_endpoints_still_resolve(self):
        for url in (
                '/GA_OD_Core/views',
                '/GA_OD_Core/download',
                '/GA_OD_Core/gaodcore-transports/zaragoza/lines',
        ):
            with self.subTest(url=url):
                self.assertIsNotNone(resolve(url))

    def test_public_routes_are_not_reachable_under_the_private_prefix(self):
        with self.assertRaises(Resolver404):
            resolve(f'{PRIVATE_PREFIX}views')


class TestSchemaFilteringFollowsTheNewPrefix(TestCase):
    """The preprocessing hooks filter on the new private prefix."""

    endpoints = [
        ('/GA_OD_Core/views', 'GET', object()),
        ('/GA_OD_Core/download', 'GET', object()),
        ('/GA_OD_Core/gaodcore-transports/zaragoza/lines', 'GET', object()),
        (f'{PRIVATE_PREFIX}manager/connector-config/', 'GET', object()),
        (f'{PRIVATE_PREFIX}admin/', 'GET', object()),
        (f'{PRIVATE_PREFIX}health/api/status/', 'GET', object()),
    ]

    def test_public_hook_excludes_private_endpoints(self):
        paths = [endpoint[0] for endpoint in custom_preprocessing_hook(list(self.endpoints))]
        self.assertEqual(
            sorted(paths),
            sorted(['/GA_OD_Core/views', '/GA_OD_Core/download', '/GA_OD_Core/gaodcore-transports/zaragoza/lines']))
        self.assertFalse([path for path in paths if path.startswith(PRIVATE_PREFIX)])

    def test_admin_hook_includes_public_and_private_endpoints(self):
        paths = [endpoint[0] for endpoint in admin_preprocessing_hook(list(self.endpoints))]
        self.assertEqual(sorted(paths), sorted(endpoint[0] for endpoint in self.endpoints))

    def test_admin_hook_drops_endpoints_outside_both_prefixes(self):
        paths = [
            endpoint[0]
            for endpoint in admin_preprocessing_hook([*self.endpoints, ('/something-else/', 'GET', object())])
        ]
        self.assertNotIn('/something-else/', paths)


class TestNoLegacyAdminRouteName(TestCase):
    """There is no leftover URL name serving the old prefix."""

    def test_unknown_names_do_not_reverse(self):
        with self.assertRaises(NoReverseMatch):
            reverse('legacy-admin-index')
