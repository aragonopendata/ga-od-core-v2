"""
Health status mixins for reusable logic in health monitoring views.
"""

from django.utils import timezone

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from .models import HealthCheckResult, ResourceHealthCheckResult


class ConnectorHealthMixin:
    """
    Mixin to provide connector health status functionality.
    """

    def get_connector_health_data(self, connector_id=None, status_filter=None):
        """
        Get health data for connectors.

        Args:
            connector_id: Optional connector ID to filter by
            status_filter: Optional status filter ('healthy', 'unhealthy', 'unknown', 'all')

        Returns:
            dict: Health data with connectors and summary statistics
        """
        # Get connectors
        connectors = ConnectorConfig.objects.filter(enabled=True)
        if connector_id:
            connectors = connectors.filter(id=connector_id)

        connector_statuses = []

        for connector in connectors:
            latest_check = (
                HealthCheckResult.objects.filter(connector=connector)
                .order_by("-check_time")
                .first()
            )

            status_class = (
                "healthy" if latest_check and latest_check.is_healthy else "unhealthy"
            )
            if not latest_check:
                status_class = "unknown"

            connector_statuses.append(
                {
                    "id": connector.id,
                    "name": connector.name,
                    "uri": connector.uri,
                    "status": "Healthy"
                    if latest_check and latest_check.is_healthy
                    else "Unhealthy"
                    if latest_check
                    else "Unknown",
                    "status_class": status_class,
                    "last_check": latest_check.check_time if latest_check else None,
                    "response_time_ms": latest_check.response_time_ms
                    if latest_check
                    else None,
                    "error_message": latest_check.error_message
                    if latest_check and not latest_check.is_healthy
                    else None,
                    "enabled": connector.enabled,
                    "object": connector,  # Include the full object for template use
                }
            )

        # Filter by status if specified
        if status_filter and status_filter != 'all':
            connector_statuses = [
                c for c in connector_statuses if c["status_class"] == status_filter
            ]

        # Calculate summary statistics
        healthy_count = sum(
            1 for c in connector_statuses if c["status_class"] == "healthy"
        )
        unhealthy_count = sum(
            1 for c in connector_statuses if c["status_class"] == "unhealthy"
        )
        unknown_count = sum(
            1 for c in connector_statuses if c["status_class"] == "unknown"
        )

        return {
            "connectors": connector_statuses,
            "total_count": len(connector_statuses),
            "healthy_count": healthy_count,
            "unhealthy_count": unhealthy_count,
            "unknown_count": unknown_count,
        }


class ResourceHealthMixin:
    """
    Mixin to provide resource health status functionality.
    """

    def get_resource_health_data(self, connector_id=None, resource_id=None, status_filter=None):
        """
        Get health data for resources.

        Args:
            connector_id: Optional connector ID to filter resources by
            resource_id: Optional resource ID to filter by
            status_filter: Optional status filter ('healthy', 'unhealthy', 'unknown', 'all')

        Returns:
            dict: Health data with resources and summary statistics
        """
        # Get resources - only enabled resources with enabled connectors
        resources = ResourceConfig.objects.filter(
            enabled=True,
            connector_config__enabled=True
        ).select_related("connector_config")
        if connector_id:
            resources = resources.filter(connector_config_id=connector_id)
        if resource_id:
            resources = resources.filter(id=resource_id)

        resource_statuses = []

        for resource in resources:
            latest_check = (
                ResourceHealthCheckResult.objects.filter(resource=resource)
                .order_by("-check_time")
                .first()
            )

            status_class = (
                "healthy" if latest_check and latest_check.is_healthy else "unhealthy"
            )
            if not latest_check:
                status_class = "unknown"

            resource_statuses.append(
                {
                    "id": resource.id,
                    "name": resource.name,
                    "object_location": resource.object_location,
                    "connector_id": resource.connector_config.id,
                    "connector_name": resource.connector_config.name,
                    "connector_uri": resource.connector_config.uri,
                    "status": "Healthy"
                    if latest_check and latest_check.is_healthy
                    else "Unhealthy"
                    if latest_check
                    else "Unknown",
                    "status_class": status_class,
                    "last_check": latest_check.check_time if latest_check else None,
                    "response_time_ms": latest_check.response_time_ms
                    if latest_check
                    else None,
                    "error_message": latest_check.error_message
                    if latest_check and not latest_check.is_healthy
                    else None,
                    "error_type": latest_check.error_type
                    if latest_check and not latest_check.is_healthy
                    else None,
                    "enabled": resource.enabled,
                    "object": resource,  # Include the full object for template use
                }
            )

        # Filter by status if specified
        if status_filter and status_filter != 'all':
            resource_statuses = [
                r for r in resource_statuses if r["status_class"] == status_filter
            ]

        # Calculate summary statistics
        healthy_count = sum(
            1 for r in resource_statuses if r["status_class"] == "healthy"
        )
        unhealthy_count = sum(
            1 for r in resource_statuses if r["status_class"] == "unhealthy"
        )
        unknown_count = sum(
            1 for r in resource_statuses if r["status_class"] == "unknown"
        )

        return {
            "resources": resource_statuses,
            "total_count": len(resource_statuses),
            "healthy_count": healthy_count,
            "unhealthy_count": unhealthy_count,
            "unknown_count": unknown_count,
        }


class HealthContextMixin:
    """
    Mixin to provide common health context data.
    """

    def get_health_context_data(self, **kwargs):
        """
        Get common context data for health views.

        Returns:
            dict: Context data including navigation info
        """
        context = (
            super().get_context_data(**kwargs)
            if hasattr(super(), "get_context_data")
            else {}
        )

        # Add navigation context
        context.update(
            {
                "current_section": getattr(self, "current_section", "health"),
                "breadcrumbs": getattr(self, "breadcrumbs", []),
                "last_updated": timezone.now(),
            }
        )

        return context
