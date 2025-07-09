"""
Custom schema views for separating public and admin API documentation.
"""
from django.urls import reverse
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView


class PublicSchemaView(SpectacularAPIView):
    """
    Schema view for public API endpoints only.
    Uses custom_settings to avoid global state modification.
    """
    custom_settings = {
        'TITLE': 'GA OD Core Public API',
        'DESCRIPTION': 'Public API for accessing open data from the Government of Aragon',
        'SCHEMA_PATH_PREFIX': r'/GA_OD_Core/',
        'SCHEMA_PATH_PREFIX_TRIM': True,
        'SCHEMA_PATH_PREFIX_INSERT': '/GA_OD_Core/',
        'PREPROCESSING_HOOKS': [
            'gaodcore_project.spectacular_hooks.custom_preprocessing_hook',
        ],
    }


class AdminSchemaView(SpectacularAPIView):
    """
    Schema view for admin API endpoints only.
    Uses custom_settings to avoid global state modification.
    """
    custom_settings = {
        'TITLE': 'GA OD Core Admin API',
        'DESCRIPTION': 'Administrative API for managing connectors and resources',
        'PREPROCESSING_HOOKS': [
            'gaodcore_project.spectacular_hooks.admin_preprocessing_hook',
        ],
    }


class PublicSwaggerView(SpectacularSwaggerView):
    """
    Swagger UI view for public API.
    """
    template_name_override = 'drf_spectacular/swagger-ui.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Configure Swagger UI with standard settings
        context['swagger_settings'] = {
            'supportedSubmitMethods': ['get', 'post', 'put', 'delete', 'patch'],
            'dom_id': '#swagger-ui',
            'presets': [
                'SwaggerUIBundle.presets.apis',
                'SwaggerUIBundle.presets.standalone'
            ],
            'layout': 'StandaloneLayout',
            'tryItOutEnabled': True,
            'filter': True,
            'requestSnippetsEnabled': True,
            'defaultModelsExpandDepth': 1,
            'defaultModelExpandDepth': 1,
            'displayOperationId': False,
            'displayRequestDuration': False,
            'deepLinking': True,
            'showExtensions': True,
            'showCommonExtensions': True,
            'onComplete': 'function() { window.ui.preauthorizeBasic("basicAuth", "", ""); }'
        }
        return context

    def get_schema_url(self):
        return self.request.build_absolute_uri(reverse('schema'))


class AdminSwaggerView(SpectacularSwaggerView):
    """
    Swagger UI view for admin API.
    """
    template_name_override = 'drf_spectacular/swagger-ui.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Configure Swagger UI with standard settings
        context['swagger_settings'] = {
            'supportedSubmitMethods': ['get', 'post', 'put', 'delete', 'patch'],
            'dom_id': '#swagger-ui',
            'presets': [
                'SwaggerUIBundle.presets.apis',
                'SwaggerUIBundle.presets.standalone'
            ],
            'layout': 'StandaloneLayout',
            'tryItOutEnabled': True,
            'filter': True,
            'requestSnippetsEnabled': True,
            'defaultModelsExpandDepth': 1,
            'defaultModelExpandDepth': 1,
            'displayOperationId': False,
            'displayRequestDuration': False,
            'deepLinking': True,
            'showExtensions': True,
            'showCommonExtensions': True,
            'onComplete': 'function() { window.ui.preauthorizeBasic("basicAuth", "", ""); }'
        }
        return context

    def get_schema_url(self):
        return self.request.build_absolute_uri(reverse('admin-schema'))
