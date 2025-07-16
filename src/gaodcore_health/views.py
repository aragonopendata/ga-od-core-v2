"""
Views for health monitoring API endpoints.
"""

import asyncio
from datetime import timedelta

from django.shortcuts import render
from django.utils import timezone
from django.db.models import Avg
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes

from gaodcore_manager.models import ConnectorConfig
from .models import HealthCheckResult
from .serializers import (
    HealthCheckResultSerializer,
    HealthStatusSerializer,
    HealthSummarySerializer,
    ConnectorHealthDetailSerializer
)
from .health_check import (
    check_all_connectors_health,
    check_specific_connector_health,
    get_connector_health_summary
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
        responses={200: HealthStatusSerializer(many=True)}
    )
    def get(self, request):
        """Get current health status of all connectors."""
        connectors = ConnectorConfig.objects.filter(enabled=True)
        status_data = []

        for connector in connectors:
            latest_check = HealthCheckResult.objects.filter(
                connector=connector
            ).first()

            status_data.append({
                'connector_id': connector.id,
                'connector_name': connector.name,
                'connector_uri': connector.uri,
                'is_healthy': latest_check.is_healthy if latest_check else None,
                'last_check': latest_check.check_time if latest_check else None,
                'response_time_ms': latest_check.response_time_ms if latest_check else None,
                'error_message': latest_check.error_message if latest_check else None,
                'error_type': latest_check.error_type if latest_check else None,
                'enabled': connector.enabled
            })

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
                description="Hours to look back for statistics"
            ),
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                description="Optional connector ID to filter by"
            )
        ],
        responses={200: HealthSummarySerializer}
    )
    def get(self, request):
        """Get health summary statistics."""
        hours = int(request.query_params.get('hours', 24))
        connector_id = request.query_params.get('connector_id')

        if connector_id:
            connector_id = int(connector_id)

        summary = get_connector_health_summary(connector_id, hours)

        # Calculate overall success rate
        if summary['total_checks'] > 0:
            summary['success_rate'] = (summary['healthy_checks'] / summary['total_checks']) * 100
        else:
            summary['success_rate'] = 0

        serializer = HealthSummarySerializer(summary)
        return Response(serializer.data)


class HealthCheckView(APIView):
    """
    Trigger health checks manually.
    """
    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["health"],
        summary="Trigger health check",
        description="Manually trigger health checks for all connectors or a specific connector",
        parameters=[
            OpenApiParameter(
                name="connector_id",
                type=OpenApiTypes.INT,
                description="Optional connector ID to check specifically"
            ),
            OpenApiParameter(
                name="concurrency",
                type=OpenApiTypes.INT,
                default=5,
                description="Maximum concurrent checks"
            ),
            OpenApiParameter(
                name="timeout",
                type=OpenApiTypes.INT,
                description="Timeout in seconds for health checks (uses config default if not specified)"
            )
        ],
        responses={200: HealthCheckResultSerializer(many=True)}
    )
    def post(self, request):
        """Trigger health checks."""
        connector_id = request.data.get('connector_id') or request.query_params.get('connector_id')
        concurrency = int(request.data.get('concurrency', 5))
        timeout = request.data.get('timeout') or request.query_params.get('timeout')
        if timeout:
            timeout = int(timeout)

        try:
            if connector_id:
                # Check specific connector
                result = asyncio.run(check_specific_connector_health(int(connector_id), timeout=timeout))
                serializer = HealthCheckResultSerializer(result)
                return Response(serializer.data)
            else:
                # Check all connectors
                results = asyncio.run(check_all_connectors_health(concurrency, timeout=timeout))
                serializer = HealthCheckResultSerializer(results, many=True)
                return Response(serializer.data)

        except ConnectorConfig.DoesNotExist:
            return Response(
                {'error': f'Connector with ID {connector_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {'error': f'Health check failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
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
                description="Filter by connector ID"
            ),
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back"
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                default=100,
                description="Maximum number of results"
            ),
            OpenApiParameter(
                name="healthy_only",
                type=OpenApiTypes.BOOL,
                description="Show only healthy results"
            ),
            OpenApiParameter(
                name="unhealthy_only",
                type=OpenApiTypes.BOOL,
                description="Show only unhealthy results"
            )
        ],
        responses={200: HealthCheckResultSerializer(many=True)}
    )
    def get(self, request):
        """Get health check history."""
        hours = int(request.query_params.get('hours', 24))
        limit = int(request.query_params.get('limit', 100))
        connector_id = request.query_params.get('connector_id')
        healthy_only = request.query_params.get('healthy_only', '').lower() == 'true'
        unhealthy_only = request.query_params.get('unhealthy_only', '').lower() == 'true'

        since = timezone.now() - timedelta(hours=hours)

        queryset = HealthCheckResult.objects.filter(
            check_time__gte=since
        ).select_related('connector').order_by('-check_time')

        if connector_id:
            queryset = queryset.filter(connector_id=connector_id)

        if healthy_only:
            queryset = queryset.filter(is_healthy=True)
        elif unhealthy_only:
            queryset = queryset.filter(is_healthy=False)

        results = queryset[:limit]
        serializer = HealthCheckResultSerializer(results, many=True)
        return Response(serializer.data)


class ConnectorHealthDetailView(APIView):
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
                description="Connector ID"
            ),
            OpenApiParameter(
                name="hours",
                type=OpenApiTypes.INT,
                default=24,
                description="Hours to look back"
            )
        ],
        responses={200: ConnectorHealthDetailSerializer}
    )
    def get(self, request, connector_id):
        """Get detailed health information for a specific connector."""
        hours = int(request.query_params.get('hours', 24))

        try:
            connector = ConnectorConfig.objects.get(id=connector_id)
        except ConnectorConfig.DoesNotExist:
            return Response(
                {'error': f'Connector with ID {connector_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        since = timezone.now() - timedelta(hours=hours)

        results = HealthCheckResult.objects.filter(
            connector=connector,
            check_time__gte=since
        ).order_by('-check_time')

        if not results.exists():
            return Response({
                'connector_id': connector.id,
                'connector_name': connector.name,
                'total_checks': 0,
                'healthy_checks': 0,
                'unhealthy_checks': 0,
                'success_rate': 0,
                'avg_response_time_ms': None,
                'latest_check': None,
                'is_currently_healthy': None,
                'recent_errors': []
            })

        total_checks = results.count()
        healthy_checks = results.filter(is_healthy=True).count()
        unhealthy_checks = total_checks - healthy_checks

        # Calculate average response time for healthy checks
        healthy_results = results.filter(is_healthy=True, response_time_ms__isnull=False)
        avg_response_time = None
        if healthy_results.exists():
            avg_response_time = healthy_results.aggregate(avg=Avg('response_time_ms'))['avg']
            avg_response_time = round(avg_response_time) if avg_response_time else None

        # Get recent errors
        recent_errors = []
        error_results = results.filter(is_healthy=False)[:5]
        for error in error_results:
            recent_errors.append(f"{error.check_time}: {error.error_type} - {error.error_message}")

        latest_check = results.first()

        data = {
            'connector_id': connector.id,
            'connector_name': connector.name,
            'total_checks': total_checks,
            'healthy_checks': healthy_checks,
            'unhealthy_checks': unhealthy_checks,
            'success_rate': (healthy_checks / total_checks * 100) if total_checks > 0 else 0,
            'avg_response_time_ms': avg_response_time,
            'latest_check': latest_check.check_time,
            'is_currently_healthy': latest_check.is_healthy,
            'recent_errors': recent_errors
        }

        serializer = ConnectorHealthDetailSerializer(data)
        return Response(serializer.data)


@api_view(['GET'])
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
        latest_check = HealthCheckResult.objects.filter(
            connector=connector
        ).first()

        status_class = 'healthy' if latest_check and latest_check.is_healthy else 'unhealthy'
        if not latest_check:
            status_class = 'unknown'

        connector_statuses.append({
            'id': connector.id,
            'name': connector.name,
            'status': 'Healthy' if latest_check and latest_check.is_healthy else 'Unhealthy' if latest_check else 'Unknown',
            'status_class': status_class,
            'last_check': latest_check.check_time if latest_check else None,
            'response_time_ms': latest_check.response_time_ms if latest_check else None,
            'error_message': latest_check.error_message if latest_check and not latest_check.is_healthy else None
        })

    # Count statuses
    healthy_count = sum(1 for c in connector_statuses if c['status_class'] == 'healthy')
    unhealthy_count = sum(1 for c in connector_statuses if c['status_class'] == 'unhealthy')
    unknown_count = sum(1 for c in connector_statuses if c['status_class'] == 'unknown')

    context = {
        'summary': summary,
        'connectors': connector_statuses,
        'healthy_count': healthy_count,
        'unhealthy_count': unhealthy_count,
        'unknown_count': unknown_count,
    }

    return render(request, 'health/dashboard.html', context)
