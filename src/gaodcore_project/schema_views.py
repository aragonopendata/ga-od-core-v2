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
    def get_schema_url(self):
        return self.request.build_absolute_uri(reverse('schema'))


class AdminSwaggerView(SpectacularSwaggerView):
    """
    Swagger UI view for admin API.
    """
    def get_schema_url(self):
        return self.request.build_absolute_uri(reverse('admin-schema'))
