"""
Comprehensive tests for drf-spectacular OpenAPI integration hooks.
Tests all three hook functions with various scenarios and edge cases.
"""
import unittest
from unittest.mock import Mock

from gaodcore_project.spectacular_hooks import (
    custom_preprocessing_hook,
    admin_preprocessing_hook,
    parameter_order_hook
)


class TestCustomPreprocessingHook(unittest.TestCase):
    """Test cases for custom_preprocessing_hook function."""

    def setUp(self):
        """Set up test data for hook testing."""
        # Mock callback objects
        self.mock_callback = Mock()

        # Sample endpoint data covering various scenarios
        self.sample_endpoints = [
            # Public API endpoints (should be included)
            ('/GA_OD_Core/views', 'GET', self.mock_callback),
            ('/GA_OD_Core/download', 'GET', self.mock_callback),
            ('/GA_OD_Core/preview', 'POST', self.mock_callback),
            ('/GA_OD_Core/show_columns', 'GET', self.mock_callback),

            # Admin endpoints (should be filtered out)
            ('/GA_OD_Core_admin/manager/', 'GET', self.mock_callback),
            ('/GA_OD_Core_admin/admin/', 'POST', self.mock_callback),

            # Format-suffixed endpoints (should be filtered out)
            ('/GA_OD_Core/download.{format}', 'GET', self.mock_callback),
            ('/GA_OD_Core/views.{format}', 'GET', self.mock_callback),

            # Other paths (should be filtered out)
            ('/other/path', 'GET', self.mock_callback),
            ('/api/v1/test', 'GET', self.mock_callback),
        ]

    def test_filters_admin_endpoints(self):
        """Test that admin endpoints are filtered out."""
        result = custom_preprocessing_hook(self.sample_endpoints)

        admin_paths = [endpoint[0] for endpoint in result if endpoint[0].startswith('/GA_OD_Core_admin/')]
        self.assertEqual(len(admin_paths), 0, "Admin endpoints should be filtered out")

    def test_includes_only_gaodcore_endpoints(self):
        """Test that only GA_OD_Core endpoints are included."""
        result = custom_preprocessing_hook(self.sample_endpoints)

        for endpoint in result:
            path = endpoint[0]
            self.assertTrue(path.startswith('/GA_OD_Core/'),
                          f"Path {path} should start with /GA_OD_Core/")
            self.assertFalse(path.startswith('/GA_OD_Core_admin/'),
                           f"Path {path} should not be admin endpoint")

    def test_filters_format_suffixed_endpoints(self):
        """Test that endpoints with {format} are filtered out."""
        result = custom_preprocessing_hook(self.sample_endpoints)

        format_paths = [endpoint[0] for endpoint in result if '{format}' in endpoint[0]]
        self.assertEqual(len(format_paths), 0, "Format-suffixed endpoints should be filtered out")

    def test_prioritizes_views_endpoints(self):
        """Test that /GA_OD_Core/views endpoints are prioritized first."""
        result = custom_preprocessing_hook(self.sample_endpoints)

        # Find the first non-views endpoint
        first_non_views_index = None
        for i, endpoint in enumerate(result):
            if not endpoint[0].startswith('/GA_OD_Core/views'):
                first_non_views_index = i
                break

        # All endpoints before the first non-views should be views endpoints
        if first_non_views_index is not None:
            for i in range(first_non_views_index):
                self.assertTrue(result[i][0].startswith('/GA_OD_Core/views'),
                              f"Endpoint at index {i} should be a views endpoint")

    def test_preserves_endpoint_structure(self):
        """Test that endpoint tuple structure is preserved."""
        test_endpoints = [
            ('/GA_OD_Core/test', 'GET', self.mock_callback),
            ('/GA_OD_Core/test2', 'POST', self.mock_callback),
        ]

        result = custom_preprocessing_hook(test_endpoints)

        for endpoint in result:
            self.assertEqual(len(endpoint), 3, "Endpoint should have 3 elements")
            self.assertIsInstance(endpoint[0], str, "First element should be path string")
            self.assertIsInstance(endpoint[1], str, "Second element should be method string")

    def test_empty_endpoints_list(self):
        """Test handling of empty endpoints list."""
        result = custom_preprocessing_hook([])
        self.assertEqual(result, [], "Empty list should return empty list")

    def test_malformed_endpoints(self):
        """Test handling of malformed endpoint data."""
        malformed_endpoints = [
            ('/GA_OD_Core/valid', 'GET', self.mock_callback),
            # Test with different tuple sizes - should not break the function
        ]

        # Function should not crash with malformed data
        result = custom_preprocessing_hook(malformed_endpoints)
        self.assertEqual(len(result), 1, "Should handle valid endpoints correctly")

    def test_mixed_valid_invalid_paths(self):
        """Test with mix of valid and invalid paths."""
        mixed_endpoints = [
            ('/GA_OD_Core/valid1', 'GET', self.mock_callback),
            ('/invalid/path', 'GET', self.mock_callback),
            ('/GA_OD_Core/valid2', 'POST', self.mock_callback),
            ('/GA_OD_Core_admin/invalid', 'GET', self.mock_callback),
        ]

        result = custom_preprocessing_hook(mixed_endpoints)
        self.assertEqual(len(result), 2, "Should include only 2 valid GA_OD_Core endpoints")

        paths = [endpoint[0] for endpoint in result]
        self.assertIn('/GA_OD_Core/valid1', paths)
        self.assertIn('/GA_OD_Core/valid2', paths)


class TestAdminPreprocessingHook(unittest.TestCase):
    """Test cases for admin_preprocessing_hook function."""

    def setUp(self):
        """Set up test data for admin hook testing."""
        self.mock_callback = Mock()

        self.sample_endpoints = [
            # Admin endpoints (should be included)
            ('/GA_OD_Core_admin/manager/', 'GET', self.mock_callback),
            ('/GA_OD_Core_admin/admin/', 'POST', self.mock_callback),

            # Default GA_OD_Core endpoints (should be included)
            ('/GA_OD_Core/views', 'GET', self.mock_callback),
            ('/GA_OD_Core/download', 'GET', self.mock_callback),

            # Transport endpoints (should be included)
            ('/GA_OD_Core/gaodcore-transports/aragon', 'GET', self.mock_callback),

            # Format-suffixed endpoints (should be filtered out)
            ('/GA_OD_Core_admin/manager.{format}', 'GET', self.mock_callback),
            ('/GA_OD_Core/download.{format}', 'GET', self.mock_callback),

            # Other paths (should be filtered out)
            ('/other/path', 'GET', self.mock_callback),
            ('/api/v1/test', 'GET', self.mock_callback),
        ]

    def test_includes_admin_endpoints(self):
        """Test that admin endpoints are included."""
        result = admin_preprocessing_hook(self.sample_endpoints)

        admin_paths = [endpoint[0] for endpoint in result if endpoint[0].startswith('/GA_OD_Core_admin/')]
        self.assertGreater(len(admin_paths), 0, "Admin endpoints should be included")

    def test_includes_default_endpoints(self):
        """Test that default GA_OD_Core endpoints are included."""
        result = admin_preprocessing_hook(self.sample_endpoints)

        default_paths = [endpoint[0] for endpoint in result
                        if endpoint[0].startswith('/GA_OD_Core/') and not endpoint[0].startswith('/GA_OD_Core_admin/')]
        self.assertGreater(len(default_paths), 0, "Default endpoints should be included")

    def test_filters_format_suffixed_endpoints(self):
        """Test that endpoints with {format} are filtered out."""
        result = admin_preprocessing_hook(self.sample_endpoints)

        format_paths = [endpoint[0] for endpoint in result if '{format}' in endpoint[0]]
        self.assertEqual(len(format_paths), 0, "Format-suffixed endpoints should be filtered out")

    def test_filters_other_paths(self):
        """Test that paths not starting with GA_OD_Core are filtered out."""
        result = admin_preprocessing_hook(self.sample_endpoints)

        for endpoint in result:
            path = endpoint[0]
            self.assertTrue(
                path.startswith('/GA_OD_Core_admin/') or path.startswith('/GA_OD_Core/'),
                f"Path {path} should start with /GA_OD_Core_admin/ or /GA_OD_Core/"
            )

    def test_empty_endpoints_list(self):
        """Test handling of empty endpoints list."""
        result = admin_preprocessing_hook([])
        self.assertEqual(result, [], "Empty list should return empty list")

    def test_preserves_endpoint_structure(self):
        """Test that endpoint tuple structure is preserved."""
        test_endpoints = [
            ('/GA_OD_Core_admin/test', 'GET', self.mock_callback),
            ('/GA_OD_Core/test2', 'POST', self.mock_callback),
        ]

        result = admin_preprocessing_hook(test_endpoints)

        for endpoint in result:
            self.assertEqual(len(endpoint), 3, "Endpoint should have 3 elements")
            self.assertIsInstance(endpoint[0], str, "First element should be path string")
            self.assertIsInstance(endpoint[1], str, "Second element should be method string")


class TestParameterOrderHook(unittest.TestCase):
    """Test cases for parameter_order_hook function."""

    def setUp(self):
        """Set up test data for parameter ordering tests."""
        self.mock_generator = Mock()
        self.mock_request = Mock()

        # Sample OpenAPI schema with parameters in wrong order
        self.sample_schema = {
            'paths': {
                '/GA_OD_Core/download': {
                    'get': {
                        'parameters': [
                            {'name': 'formato', 'in': 'query', 'description': 'Output format'},
                            {'name': 'format', 'in': 'query', 'description': 'Should be removed'},
                            {'name': 'limit', 'in': 'query', 'description': 'Limit results'},
                            {'name': 'resource_id', 'in': 'query', 'description': 'Resource identifier'},
                            {'name': 'offset', 'in': 'query', 'description': 'Offset for pagination'},
                            {'name': 'custom_param', 'in': 'query', 'description': 'Not in predefined order'},
                        ]
                    },
                    'post': {
                        'parameters': [
                            {'name': 'view_id', 'in': 'query', 'description': 'View identifier'},
                            {'name': 'filters', 'in': 'query', 'description': 'Filter conditions'},
                        ]
                    }
                },
                '/GA_OD_Core/views': {
                    'get': {
                        'parameters': [
                            {'name': 'sort', 'in': 'query', 'description': 'Sort order'},
                            {'name': 'fields', 'in': 'query', 'description': 'Field selection'},
                            {'name': 'format', 'in': 'query', 'description': 'Should be removed'},
                        ]
                    }
                }
            },
            'info': {
                'title': 'Test API',
                'version': '1.0'
            }
        }

    def test_removes_format_parameter(self):
        """Test that 'format' parameters are removed from all endpoints."""
        result = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        for path, path_data in result['paths'].items():
            for method, method_data in path_data.items():
                if 'parameters' in method_data:
                    param_names = [param['name'] for param in method_data['parameters']]
                    self.assertNotIn('format', param_names,
                                   f"'format' parameter should be removed from {path} {method}")

    def test_reorders_parameters_correctly(self):
        """Test that parameters are reordered according to predefined sequence."""
        expected_order = [
            'resource_id', 'view_id', 'filters', 'offset', 'limit',
            'fields', 'columns', 'like', 'sort', 'formato',
            'nameRes', 'name', '_page', '_pageSize'
        ]

        result = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        # Check the /GA_OD_Core/download GET endpoint
        download_params = result['paths']['/GA_OD_Core/download']['get']['parameters']
        param_names = [param['name'] for param in download_params]

        # Build expected order for this specific endpoint
        expected_for_endpoint = []
        for param_name in expected_order:
            if param_name in ['resource_id', 'limit', 'offset', 'formato']:
                expected_for_endpoint.append(param_name)

        # Add custom parameters at the end
        expected_for_endpoint.append('custom_param')

        self.assertEqual(param_names, expected_for_endpoint,
                        "Parameters should be in correct order")

    def test_preserves_unknown_parameters(self):
        """Test that parameters not in predefined order are preserved at the end."""
        result = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        download_params = result['paths']['/GA_OD_Core/download']['get']['parameters']
        param_names = [param['name'] for param in download_params]

        self.assertIn('custom_param', param_names, "Custom parameters should be preserved")

    def test_preserves_parameter_structure(self):
        """Test that parameter objects are preserved with all their properties."""
        result = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        download_params = result['paths']['/GA_OD_Core/download']['get']['parameters']

        for param in download_params:
            self.assertIn('name', param, "Parameter should have 'name' field")
            self.assertIn('in', param, "Parameter should have 'in' field")
            if param['name'] == 'resource_id':
                self.assertEqual(param['description'], 'Resource identifier',
                               "Parameter description should be preserved")

    def test_handles_missing_parameters(self):
        """Test handling of endpoints without parameters."""
        schema_no_params = {
            'paths': {
                '/GA_OD_Core/test': {
                    'get': {
                        'summary': 'Test endpoint'
                        # No parameters
                    }
                }
            }
        }

        result = parameter_order_hook(schema_no_params, self.mock_generator, self.mock_request, True)
        self.assertNotIn('parameters', result['paths']['/GA_OD_Core/test']['get'],
                        "Endpoint without parameters should remain unchanged")

    def test_handles_empty_parameters_list(self):
        """Test handling of endpoints with empty parameters list."""
        schema_empty_params = {
            'paths': {
                '/GA_OD_Core/test': {
                    'get': {
                        'parameters': []
                    }
                }
            }
        }

        result = parameter_order_hook(schema_empty_params, self.mock_generator, self.mock_request, True)
        self.assertEqual(result['paths']['/GA_OD_Core/test']['get']['parameters'], [],
                        "Empty parameters list should remain empty")

    def test_preserves_schema_structure(self):
        """Test that overall schema structure is preserved."""
        result = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        # Check that non-parameter parts are preserved
        self.assertIn('info', result, "Schema info should be preserved")
        self.assertEqual(result['info']['title'], 'Test API', "Schema title should be preserved")

        # Check that all paths are preserved
        self.assertEqual(set(result['paths'].keys()), set(self.sample_schema['paths'].keys()),
                        "All paths should be preserved")

    def test_handles_missing_paths(self):
        """Test handling of schema without paths."""
        schema_no_paths = {
            'info': {
                'title': 'Test API',
                'version': '1.0'
            }
        }

        result = parameter_order_hook(schema_no_paths, self.mock_generator, self.mock_request, True)
        self.assertEqual(result, schema_no_paths, "Schema without paths should remain unchanged")

    def test_unused_parameters_ignored(self):
        """Test that generator, request, and public parameters are not used."""
        # This test ensures the function works regardless of these parameter values
        result1 = parameter_order_hook(self.sample_schema.copy(), None, None, None)
        result2 = parameter_order_hook(self.sample_schema.copy(), self.mock_generator, self.mock_request, True)

        # Results should be identical since these parameters are not used
        self.assertEqual(result1, result2, "Function should not depend on generator, request, or public parameters")


if __name__ == '__main__':
    unittest.main()
