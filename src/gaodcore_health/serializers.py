"""
Serializers for health monitoring API endpoints.
"""

from rest_framework import serializers
from .models import (
    HealthCheckResult,
    HealthCheckSchedule,
    HealthCheckAlert,
    ResourceHealthCheckResult,
    ResourceHealthCheckAlert,
)


class HealthCheckResultSerializer(serializers.ModelSerializer):
    """Serializer for HealthCheckResult model."""

    connector_name = serializers.CharField(source="connector.name", read_only=True)
    connector_uri = serializers.CharField(source="connector.uri", read_only=True)

    class Meta:
        model = HealthCheckResult
        fields = [
            "id",
            "connector",
            "connector_name",
            "connector_uri",
            "check_time",
            "is_healthy",
            "response_time_ms",
            "error_message",
            "error_type",
        ]
        read_only_fields = ["id", "check_time", "connector_name", "connector_uri"]


class HealthCheckScheduleSerializer(serializers.ModelSerializer):
    """Serializer for HealthCheckSchedule model."""

    class Meta:
        model = HealthCheckSchedule
        fields = [
            "id",
            "name",
            "interval_minutes",
            "enabled",
            "last_run",
            "next_run",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class HealthCheckAlertSerializer(serializers.ModelSerializer):
    """Serializer for HealthCheckAlert model."""

    connector_name = serializers.CharField(source="connector.name", read_only=True)

    class Meta:
        model = HealthCheckAlert
        fields = [
            "id",
            "connector",
            "connector_name",
            "alert_type",
            "threshold_minutes",
            "consecutive_failures_threshold",
            "is_active",
            "last_alert_time",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "connector_name"]


class HealthStatusSerializer(serializers.Serializer):
    """Serializer for current health status response."""

    connector_id = serializers.IntegerField()
    connector_name = serializers.CharField()
    connector_uri = serializers.CharField()
    is_healthy = serializers.BooleanField(allow_null=True)
    last_check = serializers.DateTimeField(allow_null=True)
    response_time_ms = serializers.IntegerField(allow_null=True)
    error_message = serializers.CharField(allow_null=True)
    error_type = serializers.CharField(allow_null=True)
    enabled = serializers.BooleanField()


class HealthSummarySerializer(serializers.Serializer):
    """Serializer for health summary response."""

    period = serializers.CharField()
    total_checks = serializers.IntegerField()
    healthy_checks = serializers.IntegerField()
    unhealthy_checks = serializers.IntegerField()
    success_rate = serializers.FloatField()
    connectors = serializers.DictField()


class ConnectorHealthDetailSerializer(serializers.Serializer):
    """Serializer for individual connector health details."""

    connector_id = serializers.IntegerField()
    connector_name = serializers.CharField()
    total_checks = serializers.IntegerField()
    healthy_checks = serializers.IntegerField()
    unhealthy_checks = serializers.IntegerField()
    success_rate = serializers.FloatField()
    avg_response_time_ms = serializers.IntegerField(allow_null=True)
    latest_check = serializers.DateTimeField(allow_null=True)
    is_currently_healthy = serializers.BooleanField()
    recent_errors = serializers.ListField(child=serializers.CharField(), required=False)


# Resource Health Check Serializers


class ResourceHealthCheckResultSerializer(serializers.ModelSerializer):
    """Serializer for ResourceHealthCheckResult model."""

    resource_name = serializers.CharField(source="resource.name", read_only=True)
    resource_object_location = serializers.CharField(
        source="resource.object_location", read_only=True
    )
    connector_name = serializers.CharField(
        source="resource.connector_config.name", read_only=True
    )
    connector_uri = serializers.CharField(
        source="resource.connector_config.uri", read_only=True
    )

    class Meta:
        model = ResourceHealthCheckResult
        fields = [
            "id",
            "resource",
            "resource_name",
            "resource_object_location",
            "connector_name",
            "connector_uri",
            "check_time",
            "is_healthy",
            "response_time_ms",
            "error_message",
            "error_type",
        ]
        read_only_fields = [
            "id",
            "check_time",
            "resource_name",
            "resource_object_location",
            "connector_name",
            "connector_uri",
        ]


class ResourceHealthCheckAlertSerializer(serializers.ModelSerializer):
    """Serializer for ResourceHealthCheckAlert model."""

    resource_name = serializers.CharField(source="resource.name", read_only=True)

    class Meta:
        model = ResourceHealthCheckAlert
        fields = [
            "id",
            "resource",
            "resource_name",
            "alert_type",
            "threshold_minutes",
            "consecutive_failures_threshold",
            "is_active",
            "last_alert_time",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at", "resource_name"]


class ResourceHealthStatusSerializer(serializers.Serializer):
    """Serializer for current resource health status response."""

    resource_id = serializers.IntegerField()
    resource_name = serializers.CharField()
    resource_object_location = serializers.CharField()
    connector_name = serializers.CharField()
    connector_uri = serializers.CharField()
    is_healthy = serializers.BooleanField(allow_null=True)
    last_check = serializers.DateTimeField(allow_null=True)
    response_time_ms = serializers.IntegerField(allow_null=True)
    error_message = serializers.CharField(allow_null=True)
    error_type = serializers.CharField(allow_null=True)
    enabled = serializers.BooleanField()


class ResourceHealthSummarySerializer(serializers.Serializer):
    """Serializer for resource health summary response."""

    period = serializers.CharField()
    total_checks = serializers.IntegerField()
    healthy_checks = serializers.IntegerField()
    unhealthy_checks = serializers.IntegerField()
    success_rate = serializers.FloatField()
    resources = serializers.DictField()


class ResourceHealthDetailSerializer(serializers.Serializer):
    """Serializer for individual resource health details."""

    resource_id = serializers.IntegerField()
    resource_name = serializers.CharField()
    total_checks = serializers.IntegerField()
    healthy_checks = serializers.IntegerField()
    unhealthy_checks = serializers.IntegerField()
    success_rate = serializers.FloatField()
    avg_response_time_ms = serializers.IntegerField(allow_null=True)
    latest_check = serializers.DateTimeField(allow_null=True)
    is_currently_healthy = serializers.BooleanField()
    recent_errors = serializers.ListField(child=serializers.CharField(), required=False)
