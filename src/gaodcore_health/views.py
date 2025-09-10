"""
Views for health monitoring API endpoints.
"""

from datetime import timedelta

from django.shortcuts import render, redirect
from django.utils import timezone
from django.db.models import Avg
from django.views.generic import ListView, DetailView
from django.contrib.auth.mixins import LoginRequiredMixin
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes

from gaodcore_manager.models import ConnectorConfig, ResourceConfig
from .models import HealthCheckResult, ResourceHealthCheckResult
from .mixins import ConnectorHealthMixin, ResourceHealthMixin, HealthContextMixin
from .serializers import (
    HealthCheckResultSerializer,
    HealthStatusSerializer,
    HealthSummarySerializer,
    ConnectorHealthDetailSerializer,
    ResourceHealthCheckResultSerializer,
    ResourceHealthStatusSerializer,
    ResourceHealthSummarySerializer,
    ResourceHealthDetailSerializer,
)
from .health_check import (
    check_all_connectors_health_sync,
    check_specific_connector_health_sync,
    get_connector_health_summary,
    check_all_resources_health_sync,
    check_specific_resource_health_sync,
    get_resource_health_summary,
)


class HealthStatusView(APIView):
    """
    Get current health status of all connectors.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get current health status of all connectors",
        description="Returns the latest health check results for all enabled connectors",
        responses={200: HealthStatusSerializer(many=True)},
    )
    def get(self, request):
        """Get current health status of all connectors."""
        connectors = ConnectorConfig.objects.filter(enabled=True)
        status_data = []

        for connector in connectors:
            latest_check = HealthCheckResult.objects.filter(connector=connector).first()

            status_data.append(
                {
                    "connector_id": connector.id,
                    "connector_name": connector.name,
                    "connector_uri": connector.uri,
                    "is_healthy": latest_check.is_healthy if latest_check else None,
                    "last_check": latest_check.check_time if latest_check else None,
                    "response_time_ms": latest_check.response_time_ms
                    if latest_check
                    else None,
                    "error_message": latest_check.error_message
                    if latest_check
                    else None,
                    "error_type": latest_check.error_type if latest_check else None,
                    "enabled": connector.enabled,
                }
            )

        serializer = HealthStatusSerializer(status_data, many=True)
        return Response(serializer.data)


class HealthSummaryView(APIView):
    """
    Get health summary statistics.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get health summary statistics",
        description="Returns aggregated health statistics for a specified time period",
        parameters=[
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back for statistics",
            ),
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                description="Optional connector ID to filter by",
            ),
        ],
        responses={200: HealthSummarySerializer},
    )
    def get(self, request):
        """Get health summary statistics."""
        hours = int(request.query_params.get("hours", 24))
        connector_id = request.query_params.get("connector_id")

        if connector_id:
            connector_id = int(connector_id)

        summary = get_connector_health_summary(connector_id, hours)

        # Calculate overall success rate
        if summary["total_checks"] > 0:
            summary["success_rate"] = (
                summary["healthy_checks"] / summary["total_checks"]
            ) * 100
        else:
            summary["success_rate"] = 0

        serializer = HealthSummarySerializer(summary)
        return Response(serializer.data)


class HealthCheckView(APIView):
    """
    Trigger health checks manually.
    """

    schema = None  # Exclude from OpenAPI schema
    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Trigger health check",
        description="Manually trigger health checks for all connectors or a specific connector",
        parameters=[
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                description="Optional connector ID to check specifically",
            ),
            OpenApiParameter(
                name="concurrency",
                type=OpenApiTypes.INT,
                default=5,
                description="Maximum concurrent checks",
            ),
            OpenApiParameter(
                name="timeout",
                type=OpenApiTypes.INT,
                description="Timeout in seconds for health checks (uses config default if not specified)",
            ),
        ],
        responses={200: HealthCheckResultSerializer(many=True)},
    )
    def post(self, request):
        """Trigger health checks."""
        connector_id = request.data.get("connector_id") or request.query_params.get(
            "connector_id"
        )
        concurrency = int(request.data.get("concurrency", 5))
        timeout = request.data.get("timeout") or request.query_params.get("timeout")
        if timeout:
            timeout = int(timeout)

        try:
            if connector_id:
                # Check specific connector
                result = check_specific_connector_health_sync(int(connector_id), timeout=timeout)
                serializer = HealthCheckResultSerializer(result)
                return Response(serializer.data)
            else:
                # Check all connectors
                results = check_all_connectors_health_sync(concurrency, timeout=timeout)
                serializer = HealthCheckResultSerializer(results, many=True)
                return Response(serializer.data)

        except ConnectorConfig.DoesNotExist:
            return Response(
                {"error": f"Connector with ID {connector_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            return Response(
                {"error": f"Health check failed: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class HealthHistoryView(APIView):
    """
    Get health check history.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get health check history",
        description="Returns historical health check results",
        parameters=[
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                description="Filter by connector ID",
            ),
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back",
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                default=100,
                description="Maximum number of results",
            ),
            OpenApiParameter(
                name="healthy_only",
                type=OpenApiTypes.BOOL,
                description="Show only healthy results",
            ),
            OpenApiParameter(
                name="unhealthy_only",
                type=OpenApiTypes.BOOL,
                description="Show only unhealthy results",
            ),
        ],
        responses={200: HealthCheckResultSerializer(many=True)},
    )
    def get(self, request):
        """Get health check history."""
        hours = int(request.query_params.get("hours", 24))
        limit = int(request.query_params.get("limit", 100))
        connector_id = request.query_params.get("connector_id")
        healthy_only = request.query_params.get("healthy_only", "").lower() == "true"
        unhealthy_only = (
            request.query_params.get("unhealthy_only", "").lower() == "true"
        )

        since = timezone.now() - timedelta(hours=hours)

        queryset = (
            HealthCheckResult.objects.filter(check_time__gte=since)
            .select_related("connector")
            .order_by("-check_time")
        )

        if connector_id:
            queryset = queryset.filter(connector_id=connector_id)

        if healthy_only:
            queryset = queryset.filter(is_healthy=True)
        elif unhealthy_only:
            queryset = queryset.filter(is_healthy=False)

        results = queryset[:limit]
        serializer = HealthCheckResultSerializer(results, many=True)
        return Response(serializer.data)


class ConnectorHealthDetailAPIView(APIView):
    """
    Get detailed health information for a specific connector.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get connector health details",
        description="Returns detailed health information for a specific connector",
        parameters=[
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                required=True,
                description="Connector ID",
            ),
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back",
            ),
        ],
        responses={200: ConnectorHealthDetailSerializer},
    )
    def get(self, request, connector_id):
        """Get detailed health information for a specific connector."""
        hours = int(request.query_params.get("hours", 24))

        try:
            connector = ConnectorConfig.objects.get(id=connector_id)
        except ConnectorConfig.DoesNotExist:
            return Response(
                {"error": f"Connector with ID {connector_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        since = timezone.now() - timedelta(hours=hours)

        results = HealthCheckResult.objects.filter(
            connector=connector, check_time__gte=since
        ).order_by("-check_time")

        if not results.exists():
            return Response(
                {
                    "connector_id": connector.id,
                    "connector_name": connector.name,
                    "total_checks": 0,
                    "healthy_checks": 0,
                    "unhealthy_checks": 0,
                    "success_rate": 0,
                    "avg_response_time_ms": None,
                    "latest_check": None,
                    "is_currently_healthy": None,
                    "recent_errors": [],
                }
            )

        total_checks = results.count()
        healthy_checks = results.filter(is_healthy=True).count()
        unhealthy_checks = total_checks - healthy_checks

        # Calculate average response time for healthy checks
        healthy_results = results.filter(
            is_healthy=True, response_time_ms__isnull=False
        )
        avg_response_time = None
        if healthy_results.exists():
            avg_response_time = healthy_results.aggregate(avg=Avg("response_time_ms"))[
                "avg"
            ]
            avg_response_time = round(avg_response_time) if avg_response_time else None

        # Get recent errors
        recent_errors = []
        error_results = results.filter(is_healthy=False)[:5]
        for error in error_results:
            recent_errors.append(
                f"{error.check_time}: {error.error_type} - {error.error_message}"
            )

        latest_check = results.first()

        data = {
            "connector_id": connector.id,
            "connector_name": connector.name,
            "total_checks": total_checks,
            "healthy_checks": healthy_checks,
            "unhealthy_checks": unhealthy_checks,
            "success_rate": (healthy_checks / total_checks * 100)
            if total_checks > 0
            else 0,
            "avg_response_time_ms": avg_response_time,
            "latest_check": latest_check.check_time,
            "is_currently_healthy": latest_check.is_healthy,
            "recent_errors": recent_errors,
        }

        serializer = ConnectorHealthDetailSerializer(data)
        return Response(serializer.data)


@extend_schema(exclude=True)
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def health_dashboard(request):
    """
    Render the health monitoring dashboard.
    """
    # Get summary data for the dashboard
    summary = get_connector_health_summary(hours=24)

    # Get current status of all connectors
    connectors = ConnectorConfig.objects.filter(enabled=True)
    connector_statuses = []

    for connector in connectors:
        latest_check = HealthCheckResult.objects.filter(connector=connector).first()

        status_class = (
            "healthy" if latest_check and latest_check.is_healthy else "unhealthy"
        )
        if not latest_check:
            status_class = "unknown"

        connector_statuses.append(
            {
                "id": connector.id,
                "name": connector.name,
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
            }
        )

    # Count statuses
    healthy_count = sum(1 for c in connector_statuses if c["status_class"] == "healthy")
    unhealthy_count = sum(
        1 for c in connector_statuses if c["status_class"] == "unhealthy"
    )
    unknown_count = sum(1 for c in connector_statuses if c["status_class"] == "unknown")

    context = {
        "summary": summary,
        "connectors": connector_statuses,
        "healthy_count": healthy_count,
        "unhealthy_count": unhealthy_count,
        "unknown_count": unknown_count,
    }

    return render(request, "health/dashboard.html", context)


# New ListView-based Health Monitoring Views


def health_index(request):
    """
    Redirect /health/ to /health/connectors/
    """
    return redirect("gaodcore_health:connector_list")


class ConnectorHealthListView(
    LoginRequiredMixin, ConnectorHealthMixin, HealthContextMixin, ListView
):
    """
    ListView for connector health monitoring.
    """

    template_name = "health/connector_list.html"
    context_object_name = "connectors"
    paginate_by = 20
    current_section = "connectors"

    def get_queryset(self):
        """Return enabled connectors - ListView will handle pagination."""
        return ConnectorConfig.objects.filter(enabled=True).order_by("id")

    def get_context_data(self, **kwargs):
        """Add health data to context."""
        context = self.get_health_context_data(**kwargs)

        # Get status filter from query parameters
        status_filter = self.request.GET.get('status', 'all')

        # Get health data for all connectors
        health_data = self.get_connector_health_data(status_filter=status_filter)

        # Add health data to context
        context.update(
            {
                "connector_health_data": health_data,
                "page_title": "Connector Health Monitor",
                "breadcrumbs": [
                    {"name": "Health", "url": None},
                    {"name": "Connectors", "url": None, "active": True},
                ],
                "current_status_filter": status_filter,
            }
        )

        return context


class ResourceHealthListView(
    LoginRequiredMixin, ResourceHealthMixin, HealthContextMixin, ListView
):
    """
    ListView for resource health monitoring.
    """

    template_name = "health/resource_list.html"
    context_object_name = "resources"
    paginate_by = 20
    current_section = "resources"

    def get_queryset(self):
        """Return enabled resources - ListView will handle pagination."""
        return (
            ResourceConfig.objects.filter(enabled=True)
            .select_related("connector_config")
            .order_by("id")
        )

    def get_context_data(self, **kwargs):
        """Add health data to context."""
        context = self.get_health_context_data(**kwargs)

        # Get status filter from query parameters
        status_filter = self.request.GET.get('status', 'all')

        # Get health data for all resources
        health_data = self.get_resource_health_data(status_filter=status_filter)

        # Add health data to context
        context.update(
            {
                "resource_health_data": health_data,
                "page_title": "Resource Health Monitor",
                "breadcrumbs": [
                    {"name": "Health", "url": None},
                    {"name": "Resources", "url": None, "active": True},
                ],
                "current_status_filter": status_filter,
            }
        )

        return context


class ConnectorResourceListView(
    LoginRequiredMixin, ResourceHealthMixin, HealthContextMixin, ListView
):
    """
    ListView for resources filtered by connector.
    """

    template_name = "health/resource_list.html"
    context_object_name = "resources"
    paginate_by = 20
    current_section = "resources"

    def get_queryset(self):
        """Return enabled resources for specific connector."""
        connector_id = self.kwargs.get("connector_id")
        return (
            ResourceConfig.objects.filter(
                enabled=True, connector_config_id=connector_id
            )
            .select_related("connector_config")
            .order_by("id")
        )

    def get_context_data(self, **kwargs):
        """Add health data to context."""
        context = self.get_health_context_data(**kwargs)

        connector_id = self.kwargs.get("connector_id")

        # Get connector info
        try:
            connector = ConnectorConfig.objects.get(id=connector_id)
        except ConnectorConfig.DoesNotExist:
            connector = None

        # Get status filter from query parameters
        status_filter = self.request.GET.get('status', 'all')

        # Get health data for resources of this connector
        health_data = self.get_resource_health_data(connector_id=connector_id, status_filter=status_filter)

        # Add health data to context
        context.update(
            {
                "resource_health_data": health_data,
                "connector": connector,
                "page_title": f"Resources - {connector.name if connector else 'Unknown Connector'}",
                "breadcrumbs": [
                    {"name": "Health", "url": None},
                    {"name": "Connectors", "url": "gaodcore_health:connector_list"},
                    {
                        "name": connector.name if connector else "Unknown",
                        "url": None,
                        "active": True,
                    },
                ],
                "current_status_filter": status_filter,
            }
        )

        return context


class ConnectorHealthDetailView(
    LoginRequiredMixin, ConnectorHealthMixin, ResourceHealthMixin, HealthContextMixin, DetailView
):
    """
    Detail view for a specific connector showing info, health data, and resources.
    """

    model = ConnectorConfig
    template_name = "health/connector_detail.html"
    context_object_name = "connector"
    pk_url_kwarg = "connector_id"
    current_section = "connectors"

    def get_queryset(self):
        """Return enabled connectors only."""
        return ConnectorConfig.objects.filter(enabled=True)

    def get_context_data(self, **kwargs):
        """Add health data and resources to context."""
        context = self.get_health_context_data(**kwargs)
        connector = self.get_object()

        # Get connector health data
        connector_health = self.get_connector_health_data(connector_id=connector.id)

        # Get resources for this connector
        resources_health = self.get_resource_health_data(connector_id=connector.id)

        # Add breadcrumbs
        context.update({
            "connector_health_data": connector_health,
            "resource_health_data": resources_health,
            "page_title": f"Connector Details - {connector.name}",
            "breadcrumbs": [
                {"name": "Health", "url": None},
                {"name": "Connectors", "url": "gaodcore_health:connector_list"},
                {"name": connector.name, "url": None, "active": True},
            ],
        })

        return context


class ResourceHealthDetailView(
    LoginRequiredMixin, ResourceHealthMixin, HealthContextMixin, DetailView
):
    """
    Detail view for a specific resource showing info and health data.
    """

    model = ResourceConfig
    template_name = "health/resource_detail.html"
    context_object_name = "resource"
    pk_url_kwarg = "resource_id"
    current_section = "resources"

    def get_queryset(self):
        """Return enabled resources with enabled connectors only."""
        return ResourceConfig.objects.filter(
            enabled=True,
            connector_config__enabled=True
        ).select_related("connector_config")

    def get_context_data(self, **kwargs):
        """Add health data to context."""
        context = self.get_health_context_data(**kwargs)
        resource = self.get_object()

        # Get resource health data
        resource_health = self.get_resource_health_data(resource_id=resource.id)

        # Add breadcrumbs
        context.update({
            "resource_health_data": resource_health,
            "page_title": f"Resource Details - {resource.name}",
            "breadcrumbs": [
                {"name": "Health", "url": None},
                {"name": "Connectors", "url": "gaodcore_health:connector_list"},
                {"name": resource.connector_config.name, "url": "gaodcore_health:connector_detail", "url_kwargs": {"connector_id": resource.connector_config.id}},
                {"name": resource.name, "url": None, "active": True},
            ],
        })

        return context


# Resource Health Check Views


class ResourceHealthStatusView(APIView):
    """
    Get current health status of all resources.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get current health status of all resources",
        description="Returns the latest health check results for all enabled resources",
        responses={200: ResourceHealthStatusSerializer(many=True)},
    )
    def get(self, request):
        """Get current health status of all resources."""
        resources = ResourceConfig.objects.filter(enabled=True).select_related(
            "connector_config"
        )
        status_data = []

        for resource in resources:
            # Get latest health check result for this resource
            latest_result = (
                ResourceHealthCheckResult.objects.filter(resource=resource)
                .order_by("-check_time")
                .first()
            )

            if latest_result:
                status_data.append(
                    {
                        "resource_id": resource.id,
                        "resource_name": resource.name,
                        "resource_object_location": resource.object_location,
                        "connector_name": resource.connector_config.name,
                        "connector_uri": resource.connector_config.uri,
                        "is_healthy": latest_result.is_healthy,
                        "last_check": latest_result.check_time,
                        "response_time_ms": latest_result.response_time_ms,
                        "error_message": latest_result.error_message,
                        "error_type": latest_result.error_type,
                        "enabled": resource.enabled,
                    }
                )
            else:
                status_data.append(
                    {
                        "resource_id": resource.id,
                        "resource_name": resource.name,
                        "resource_object_location": resource.object_location,
                        "connector_name": resource.connector_config.name,
                        "connector_uri": resource.connector_config.uri,
                        "is_healthy": None,
                        "last_check": None,
                        "response_time_ms": None,
                        "error_message": None,
                        "error_type": None,
                        "enabled": resource.enabled,
                    }
                )

        serializer = ResourceHealthStatusSerializer(status_data, many=True)
        return Response(serializer.data)


class ResourceHealthSummaryView(APIView):
    """
    Get health summary for resources.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get resource health summary",
        description="Returns health summary statistics for resources",
        parameters=[
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back for summary (default: 24)",
            ),
            OpenApiParameter(
                name="resource_id",
                type=OpenApiTypes.INT,
                description="Optional resource ID to get summary for",
            ),
        ],
        responses={200: ResourceHealthSummarySerializer},
    )
    def get(self, request):
        """Get resource health summary."""
        hours = int(request.query_params.get("hours", 24))
        resource_id = request.query_params.get("resource_id")

        if resource_id:
            resource_id = int(resource_id)

        summary = get_resource_health_summary(resource_id=resource_id, hours=hours)

        # Calculate overall success rate
        if summary["total_checks"] > 0:
            summary["success_rate"] = (
                summary["healthy_checks"] / summary["total_checks"]
            ) * 100
        else:
            summary["success_rate"] = 0.0

        serializer = ResourceHealthSummarySerializer(summary)
        return Response(serializer.data)


class ResourceHealthCheckView(APIView):
    """
    Trigger resource health checks manually.
    """

    schema = None  # Exclude from OpenAPI schema
    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Trigger resource health check",
        description="Manually trigger health checks for all resources or a specific resource",
        parameters=[
            OpenApiParameter(
                name="resource_id",
                type=OpenApiTypes.INT,
                description="Optional resource ID to check specifically",
            ),
            OpenApiParameter(
                name="concurrency",
                type=OpenApiTypes.INT,
                default=5,
                description="Maximum concurrent checks",
            ),
            OpenApiParameter(
                name="timeout",
                type=OpenApiTypes.INT,
                description="Timeout in seconds for health checks (uses config default if not specified)",
            ),
        ],
        responses={200: ResourceHealthCheckResultSerializer(many=True)},
    )
    def post(self, request):
        """Trigger resource health checks."""
        resource_id = request.data.get("resource_id") or request.query_params.get(
            "resource_id"
        )
        concurrency = int(request.data.get("concurrency", 5))
        timeout = request.data.get("timeout") or request.query_params.get("timeout")
        if timeout:
            timeout = int(timeout)

        try:
            if resource_id:
                # Check specific resource
                result = check_specific_resource_health_sync(int(resource_id), timeout=timeout)
                serializer = ResourceHealthCheckResultSerializer(result)
                return Response(serializer.data)
            else:
                # Check all resources
                results = check_all_resources_health_sync(concurrency, timeout=timeout)
                serializer = ResourceHealthCheckResultSerializer(results, many=True)
                return Response(serializer.data)

        except ResourceConfig.DoesNotExist:
            return Response(
                {"error": f"Resource with ID {resource_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            return Response(
                {"error": f"Resource health check failed: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class ResourceHealthHistoryView(APIView):
    """
    Get historical resource health check results.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get resource health check history",
        description="Returns historical health check results for resources",
        parameters=[
            OpenApiParameter(
                name="resource_id",
                type=OpenApiTypes.INT,
                description="Optional resource ID to get history for",
            ),
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back for history (default: 24)",
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                default=100,
                description="Maximum number of results to return (default: 100)",
            ),
            OpenApiParameter(
                name="healthy_only",
                type=OpenApiTypes.BOOL,
                description="Only return healthy results",
            ),
            OpenApiParameter(
                name="unhealthy_only",
                type=OpenApiTypes.BOOL,
                description="Only return unhealthy results",
            ),
        ],
        responses={200: ResourceHealthCheckResultSerializer(many=True)},
    )
    def get(self, request):
        """Get resource health check history."""
        resource_id = request.query_params.get("resource_id")
        hours = int(request.query_params.get("hours", 24))
        limit = int(request.query_params.get("limit", 100))
        healthy_only = request.query_params.get("healthy_only", "").lower() == "true"
        unhealthy_only = (
            request.query_params.get("unhealthy_only", "").lower() == "true"
        )

        since = timezone.now() - timedelta(hours=hours)

        queryset = (
            ResourceHealthCheckResult.objects.filter(check_time__gte=since)
            .select_related("resource", "resource__connector_config")
            .order_by("-check_time")
        )

        if resource_id:
            queryset = queryset.filter(resource_id=int(resource_id))

        if healthy_only:
            queryset = queryset.filter(is_healthy=True)
        elif unhealthy_only:
            queryset = queryset.filter(is_healthy=False)

        results = queryset[:limit]
        serializer = ResourceHealthCheckResultSerializer(results, many=True)
        return Response(serializer.data)


class ResourceHealthDetailAPIView(APIView):
    """
    Get detailed health information for a specific resource.
    """

    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Get detailed resource health information",
        description="Returns detailed health information for a specific resource",
        parameters=[
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back for detailed information (default: 24)",
            ),
        ],
        responses={200: ResourceHealthDetailSerializer},
    )
    def get(self, request, resource_id):
        """Get detailed health information for a specific resource."""
        hours = int(request.query_params.get("hours", 24))

        try:
            resource = ResourceConfig.objects.get(id=resource_id)
        except ResourceConfig.DoesNotExist:
            return Response(
                {"error": f"Resource with ID {resource_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        since = timezone.now() - timedelta(hours=hours)

        results = ResourceHealthCheckResult.objects.filter(
            resource=resource, check_time__gte=since
        ).order_by("-check_time")

        if not results.exists():
            return Response(
                {
                    "error": f"No health check results found for resource {resource_id} in the last {hours} hours"
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        total_checks = results.count()
        healthy_checks = results.filter(is_healthy=True).count()
        unhealthy_checks = total_checks - healthy_checks

        # Calculate average response time for healthy checks
        healthy_results = results.filter(is_healthy=True)
        avg_response_time = None
        if healthy_results.exists():
            avg_response_time = healthy_results.aggregate(avg=Avg("response_time_ms"))[
                "avg"
            ]

        # Get recent errors
        recent_errors = list(
            results.filter(is_healthy=False, error_message__isnull=False).values_list(
                "error_message", flat=True
            )[:5]
        )

        detail = {
            "resource_id": resource.id,
            "resource_name": resource.name,
            "total_checks": total_checks,
            "healthy_checks": healthy_checks,
            "unhealthy_checks": unhealthy_checks,
            "success_rate": (healthy_checks / total_checks * 100)
            if total_checks > 0
            else 0,
            "avg_response_time_ms": round(avg_response_time)
            if avg_response_time
            else None,
            "latest_check": results.first().check_time,
            "is_currently_healthy": results.first().is_healthy,
            "recent_errors": recent_errors,
        }

        serializer = ResourceHealthDetailSerializer(detail)
        return Response(serializer.data)
