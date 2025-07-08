"""
Comprehensive tests for OpenAPI schema views.
Tests schema generation views and Swagger UI views for both public and admin APIs.
"""
from django.test import TestCase, RequestFactory
from django.conf import settings
from unittest.mock import patch, Mock

from gaodcore_project.schema_views import (
    PublicSchemaView,
    AdminSchemaView,
    PublicSwaggerView,
    AdminSwaggerView
)


class TestPublicSchemaView(TestCase):
    """Test cases for PublicSchemaView."""

    def setUp(self):
        """Set up test environment."""
        self.factory = RequestFactory()
        self.view = PublicSchemaView()

    def test_custom_settings_configuration(self):
        """Test that custom settings are properly configured."""
        expected_settings = {
            'TITLE': 'GA OD Core Public API',
            'DESCRIPTION': 'Public API for accessing open data from the Government of Aragon',
            'PREPROCESSING_HOOKS': [
                'gaodcore_project.spectacular_hooks.custom_preprocessing_hook',
            ],
        }

        self.assertEqual(self.view.custom_settings, expected_settings,
                        "Custom settings should match expected configuration")

    def test_custom_settings_override_global(self):
        """Test that custom settings override global spectacular settings."""
        # Check that the title is different from global settings
        global_title = settings.SPECTACULAR_SETTINGS.get('TITLE', '')
        custom_title = self.view.custom_settings.get('TITLE', '')

        self.assertNotEqual(global_title, custom_title,
                           "Custom title should be different from global title")
        self.assertEqual(custom_title, 'GA OD Core Public API',
                        "Custom title should be for public API")

    def test_preprocessing_hook_configuration(self):
        """Test that correct preprocessing hook is configured."""
        hooks = self.view.custom_settings.get('PREPROCESSING_HOOKS', [])

        self.assertEqual(len(hooks), 1, "Should have exactly one preprocessing hook")
        self.assertEqual(hooks[0], 'gaodcore_project.spectacular_hooks.custom_preprocessing_hook',
                        "Should use custom preprocessing hook for public API")

    @patch('gaodcore_project.schema_views.SpectacularAPIView.get')
    def test_view_response(self, mock_get):
        """Test that view responds correctly to GET requests."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"openapi": "3.0.0"}'
        mock_get.return_value = mock_response

        request = self.factory.get('/GA_OD_Core/ui/schema/')
        response = self.view.get(request)

        self.assertEqual(response.status_code, 200)
        mock_get.assert_called_once_with(request)

    def test_inherits_from_spectacular_api_view(self):
        """Test that PublicSchemaView inherits from SpectacularAPIView."""
        from drf_spectacular.views import SpectacularAPIView
        self.assertTrue(issubclass(PublicSchemaView, SpectacularAPIView),
                       "PublicSchemaView should inherit from SpectacularAPIView")


class TestAdminSchemaView(TestCase):
    """Test cases for AdminSchemaView."""

    def setUp(self):
        """Set up test environment."""
        self.factory = RequestFactory()
        self.view = AdminSchemaView()

    def test_custom_settings_configuration(self):
        """Test that admin custom settings are properly configured."""
        expected_settings = {
            'TITLE': 'GA OD Core Admin API',
            'DESCRIPTION': 'Administrative API for managing connectors and resources',
            'PREPROCESSING_HOOKS': [
                'gaodcore_project.spectacular_hooks.admin_preprocessing_hook',
            ],
        }

        self.assertEqual(self.view.custom_settings, expected_settings,
                        "Admin custom settings should match expected configuration")

    def test_different_from_public_settings(self):
        """Test that admin settings are different from public settings."""
        public_view = PublicSchemaView()

        # Titles should be different
        public_title = public_view.custom_settings.get('TITLE', '')
        admin_title = self.view.custom_settings.get('TITLE', '')
        self.assertNotEqual(public_title, admin_title,
                           "Admin and public titles should be different")

        # Preprocessing hooks should be different
        public_hooks = public_view.custom_settings.get('PREPROCESSING_HOOKS', [])
        admin_hooks = self.view.custom_settings.get('PREPROCESSING_HOOKS', [])
        self.assertNotEqual(public_hooks, admin_hooks,
                           "Admin and public preprocessing hooks should be different")

    def test_preprocessing_hook_configuration(self):
        """Test that correct admin preprocessing hook is configured."""
        hooks = self.view.custom_settings.get('PREPROCESSING_HOOKS', [])

        self.assertEqual(len(hooks), 1, "Should have exactly one preprocessing hook")
        self.assertEqual(hooks[0], 'gaodcore_project.spectacular_hooks.admin_preprocessing_hook',
                        "Should use admin preprocessing hook")

    @patch('gaodcore_project.schema_views.SpectacularAPIView.get')
    def test_view_response(self, mock_get):
        """Test that admin view responds correctly to GET requests."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"openapi": "3.0.0"}'
        mock_get.return_value = mock_response

        request = self.factory.get('/GA_OD_Core_admin/ui/schema/')
        response = self.view.get(request)

        self.assertEqual(response.status_code, 200)
        mock_get.assert_called_once_with(request)


class TestPublicSwaggerView(TestCase):
    """Test cases for PublicSwaggerView."""

    def setUp(self):
        """Set up test environment."""
        self.factory = RequestFactory()
        self.view = PublicSwaggerView()

    def test_inherits_from_spectacular_swagger_view(self):
        """Test that PublicSwaggerView inherits from SpectacularSwaggerView."""
        from drf_spectacular.views import SpectacularSwaggerView
        self.assertTrue(issubclass(PublicSwaggerView, SpectacularSwaggerView),
                       "PublicSwaggerView should inherit from SpectacularSwaggerView")

    def test_get_schema_url_method_exists(self):
        """Test that get_schema_url method is implemented."""
        self.assertTrue(hasattr(self.view, 'get_schema_url'),
                       "PublicSwaggerView should have get_schema_url method")
        self.assertTrue(callable(getattr(self.view, 'get_schema_url')),
                       "get_schema_url should be callable")

    @patch('gaodcore_project.schema_views.reverse')
    def test_get_schema_url_reverse_call(self, mock_reverse):
        """Test that get_schema_url calls reverse with correct argument."""
        mock_reverse.return_value = '/GA_OD_Core/ui/schema/'

        # Create a mock request with build_absolute_uri method
        request = Mock()
        request.build_absolute_uri.return_value = 'http://testserver/GA_OD_Core/ui/schema/'
        self.view.request = request

        result = self.view.get_schema_url()

        mock_reverse.assert_called_once_with('schema')
        request.build_absolute_uri.assert_called_once_with('/GA_OD_Core/ui/schema/')
        self.assertEqual(result, 'http://testserver/GA_OD_Core/ui/schema/')

    def test_get_schema_url_with_real_request(self):
        """Test get_schema_url with a real request object."""
        request = self.factory.get('/GA_OD_Core/ui/')
        request.META['HTTP_HOST'] = 'testserver'
        self.view.request = request

        # This will test the actual URL building
        result = self.view.get_schema_url()

        self.assertIsInstance(result, str, "Should return a string URL")
        self.assertIn('schema', result, "URL should contain 'schema'")


class TestAdminSwaggerView(TestCase):
    """Test cases for AdminSwaggerView."""

    def setUp(self):
        """Set up test environment."""
        self.factory = RequestFactory()
        self.view = AdminSwaggerView()

    def test_inherits_from_spectacular_swagger_view(self):
        """Test that AdminSwaggerView inherits from SpectacularSwaggerView."""
        from drf_spectacular.views import SpectacularSwaggerView
        self.assertTrue(issubclass(AdminSwaggerView, SpectacularSwaggerView),
                       "AdminSwaggerView should inherit from SpectacularSwaggerView")

    def test_get_schema_url_method_exists(self):
        """Test that get_schema_url method is implemented."""
        self.assertTrue(hasattr(self.view, 'get_schema_url'),
                       "AdminSwaggerView should have get_schema_url method")
        self.assertTrue(callable(getattr(self.view, 'get_schema_url')),
                       "get_schema_url should be callable")

    @patch('gaodcore_project.schema_views.reverse')
    def test_get_schema_url_reverse_call(self, mock_reverse):
        """Test that get_schema_url calls reverse with correct admin argument."""
        mock_reverse.return_value = '/GA_OD_Core_admin/ui/schema/'

        # Create a mock request with build_absolute_uri method
        request = Mock()
        request.build_absolute_uri.return_value = 'http://testserver/GA_OD_Core_admin/ui/schema/'
        self.view.request = request

        result = self.view.get_schema_url()

        mock_reverse.assert_called_once_with('admin-schema')
        request.build_absolute_uri.assert_called_once_with('/GA_OD_Core_admin/ui/schema/')
        self.assertEqual(result, 'http://testserver/GA_OD_Core_admin/ui/schema/')

    def test_different_from_public_schema_url(self):
        """Test that admin schema URL is different from public schema URL."""
        # Mock reverse to return different URLs
        with patch('gaodcore_project.schema_views.reverse') as mock_reverse:
            def side_effect(name):
                if name == 'schema':
                    return '/GA_OD_Core/ui/schema/'
                elif name == 'admin-schema':
                    return '/GA_OD_Core_admin/ui/schema/'
                return '/'

            mock_reverse.side_effect = side_effect

            # Create requests for both views
            request = Mock()
            request.build_absolute_uri = lambda path: f'http://testserver{path}'

            public_view = PublicSwaggerView()
            public_view.request = request

            admin_view = AdminSwaggerView()
            admin_view.request = request

            public_url = public_view.get_schema_url()
            admin_url = admin_view.get_schema_url()

            self.assertNotEqual(public_url, admin_url,
                               "Public and admin schema URLs should be different")
            self.assertIn('admin', admin_url.lower(),
                         "Admin URL should contain 'admin'")

    def test_get_schema_url_with_real_request(self):
        """Test get_schema_url with a real request object."""
        request = self.factory.get('/GA_OD_Core_admin/ui/')
        request.META['HTTP_HOST'] = 'testserver'
        self.view.request = request

        # This will test the actual URL building
        result = self.view.get_schema_url()

        self.assertIsInstance(result, str, "Should return a string URL")
        self.assertIn('schema', result, "URL should contain 'schema'")


class TestSchemaViewsIntegration(TestCase):
    """Integration tests for schema views."""

    def test_all_views_are_different_classes(self):
        """Test that all four view classes are distinct."""
        views = [PublicSchemaView, AdminSchemaView, PublicSwaggerView, AdminSwaggerView]

        for i, view1 in enumerate(views):
            for j, view2 in enumerate(views):
                if i != j:
                    self.assertNotEqual(view1, view2,
                                      f"{view1.__name__} should be different from {view2.__name__}")

    def test_schema_views_have_custom_settings(self):
        """Test that both schema views have custom_settings attribute."""
        public_schema = PublicSchemaView()
        admin_schema = AdminSchemaView()

        self.assertTrue(hasattr(public_schema, 'custom_settings'),
                       "PublicSchemaView should have custom_settings")
        self.assertTrue(hasattr(admin_schema, 'custom_settings'),
                       "AdminSchemaView should have custom_settings")

        self.assertIsInstance(public_schema.custom_settings, dict,
                             "custom_settings should be a dictionary")
        self.assertIsInstance(admin_schema.custom_settings, dict,
                             "custom_settings should be a dictionary")

    def test_swagger_views_have_get_schema_url(self):
        """Test that both Swagger views have get_schema_url method."""
        public_swagger = PublicSwaggerView()
        admin_swagger = AdminSwaggerView()

        self.assertTrue(hasattr(public_swagger, 'get_schema_url'),
                       "PublicSwaggerView should have get_schema_url")
        self.assertTrue(hasattr(admin_swagger, 'get_schema_url'),
                       "AdminSwaggerView should have get_schema_url")

    def test_views_use_different_preprocessing_hooks(self):
        """Test that public and admin views use different preprocessing hooks."""
        public_view = PublicSchemaView()
        admin_view = AdminSchemaView()

        public_hooks = public_view.custom_settings.get('PREPROCESSING_HOOKS', [])
        admin_hooks = admin_view.custom_settings.get('PREPROCESSING_HOOKS', [])

        self.assertNotEqual(public_hooks, admin_hooks,
                           "Public and admin views should use different preprocessing hooks")

        self.assertIn('custom_preprocessing_hook', public_hooks[0],
                     "Public view should use custom_preprocessing_hook")
        self.assertIn('admin_preprocessing_hook', admin_hooks[0],
                     "Admin view should use admin_preprocessing_hook")


if __name__ == '__main__':
    import django
    django.setup()

    from django.test.utils import get_runner

    TestRunner = get_runner(settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests(["gaodcore_project.tests.test_schema_views"])
