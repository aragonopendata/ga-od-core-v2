from django.contrib import admin
from django.utils.html import format_html

from .models import (
    HealthCheckResult,
    HealthCheckSchedule,
    HealthCheckAlert,
    ResourceHealthCheckResult,
    ResourceHealthCheckAlert,
)


@admin.register(HealthCheckResult)
class HealthCheckResultAdmin(admin.ModelAdmin):
    list_display = [
        "connector",
        "check_time",
        "health_status",
        "response_time_ms",
        "error_type_display",
    ]
    list_filter = ["is_healthy", "error_type", "check_time", "connector"]
    search_fields = ["connector__name", "error_message"]
    readonly_fields = [
        "connector",
        "check_time",
        "is_healthy",
        "response_time_ms",
        "error_message",
        "error_type",
    ]
    ordering = ["-check_time"]
    date_hierarchy = "check_time"

    def health_status(self, obj):
        if obj.is_healthy:
            return format_html('<span style="color: green;">✓ Healthy</span>')
        else:
            return format_html('<span style="color: red;">✗ Unhealthy</span>')

    health_status.short_description = "Status"

    def error_type_display(self, obj):
        if obj.error_type:
            return format_html('<span style="color: red;">{}</span>', obj.error_type)
        return "-"

    error_type_display.short_description = "Error Type"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(HealthCheckSchedule)
class HealthCheckScheduleAdmin(admin.ModelAdmin):
    list_display = ["name", "interval_minutes", "enabled", "last_run", "next_run"]
    list_filter = ["enabled"]
    search_fields = ["name"]
    readonly_fields = ["last_run", "next_run", "created_at", "updated_at"]

    fieldsets = (
        ("Basic Information", {"fields": ("name", "interval_minutes", "enabled")}),
        (
            "Schedule Information",
            {"fields": ("last_run", "next_run"), "classes": ("collapse",)},
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )


@admin.register(HealthCheckAlert)
class HealthCheckAlertAdmin(admin.ModelAdmin):
    list_display = [
        "connector",
        "alert_type",
        "is_active",
        "threshold_minutes",
        "last_alert_time",
    ]
    list_filter = ["alert_type", "is_active", "connector"]
    search_fields = ["connector__name"]
    readonly_fields = ["last_alert_time", "created_at", "updated_at"]

    fieldsets = (
        ("Basic Information", {"fields": ("connector", "alert_type", "is_active")}),
        (
            "Alert Configuration",
            {"fields": ("threshold_minutes", "consecutive_failures_threshold")},
        ),
        ("Status", {"fields": ("last_alert_time",), "classes": ("collapse",)}),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("connector")


# Custom admin actions
def run_health_check(modeladmin, request, queryset):
    """Admin action to run health checks for selected connectors."""
    from .health_check import check_specific_connector_health_sync

    results = []
    for connector in queryset:
        try:
            result = check_specific_connector_health_sync(connector.id)
            results.append(
                f"✓ {connector.name}: {'Healthy' if result.is_healthy else 'Unhealthy'}"
            )
        except Exception as e:
            results.append(f"✗ {connector.name}: Error - {str(e)}")

    modeladmin.message_user(request, "Health check results:\n" + "\n".join(results))


run_health_check.short_description = "Run health check for selected connectors"


# Add the action to ConnectorConfig admin if it exists
try:
    from gaodcore_manager.admin import ConnectorConfigAdmin

    # Add health check action to existing ConnectorConfig admin
    if hasattr(ConnectorConfigAdmin, "actions"):
        if isinstance(ConnectorConfigAdmin.actions, tuple):
            ConnectorConfigAdmin.actions = list(ConnectorConfigAdmin.actions) + [
                run_health_check
            ]
        else:
            ConnectorConfigAdmin.actions.append(run_health_check)
    else:
        ConnectorConfigAdmin.actions = [run_health_check]

except ImportError:
    pass


# Resource Health Check Admin


@admin.register(ResourceHealthCheckResult)
class ResourceHealthCheckResultAdmin(admin.ModelAdmin):
    list_display = [
        "resource",
        "check_time",
        "health_status",
        "response_time_ms",
        "error_type_display",
    ]
    list_filter = ["is_healthy", "error_type", "check_time", "resource"]
    search_fields = ["resource__name", "error_message"]
    readonly_fields = [
        "resource",
        "check_time",
        "is_healthy",
        "response_time_ms",
        "error_message",
        "error_type",
    ]
    ordering = ["-check_time"]
    date_hierarchy = "check_time"

    def health_status(self, obj):
        if obj.is_healthy:
            return format_html('<span style="color: green;">✓ Healthy</span>')
        else:
            return format_html('<span style="color: red;">✗ Unhealthy</span>')

    health_status.short_description = "Status"

    def error_type_display(self, obj):
        if obj.error_type:
            return format_html('<span style="color: red;">{}</span>', obj.error_type)
        return "-"

    error_type_display.short_description = "Error Type"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related("resource", "resource__connector_config")
        )


@admin.register(ResourceHealthCheckAlert)
class ResourceHealthCheckAlertAdmin(admin.ModelAdmin):
    list_display = [
        "resource",
        "alert_type",
        "is_active",
        "threshold_minutes",
        "last_alert_time",
    ]
    list_filter = ["alert_type", "is_active", "resource"]
    search_fields = ["resource__name"]
    readonly_fields = ["last_alert_time", "created_at", "updated_at"]

    fieldsets = (
        ("Basic Information", {"fields": ("resource", "alert_type", "is_active")}),
        (
            "Alert Configuration",
            {"fields": ("threshold_minutes", "consecutive_failures_threshold")},
        ),
        ("Status", {"fields": ("last_alert_time",), "classes": ("collapse",)}),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related("resource", "resource__connector_config")
        )


# Custom admin actions for resources
def run_resource_health_check(modeladmin, request, queryset):
    """Admin action to run health checks for selected resources."""
    from .health_check import check_specific_resource_health_sync

    results = []
    for resource in queryset:
        try:
            result = check_specific_resource_health_sync(resource.id)
            results.append(
                f"✓ {resource.name}: {'Healthy' if result.is_healthy else 'Unhealthy'}"
            )
        except Exception as e:
            results.append(f"✗ {resource.name}: Error - {str(e)}")

    modeladmin.message_user(
        request, "Resource health check results:\n" + "\n".join(results)
    )


run_resource_health_check.short_description = "Run health check for selected resources"


# Add the action to ResourceConfig admin if it exists
try:
    from gaodcore_manager.admin import ResourceConfigAdmin

    # Add health check action to existing ResourceConfig admin
    if hasattr(ResourceConfigAdmin, "actions"):
        if isinstance(ResourceConfigAdmin.actions, tuple):
            ResourceConfigAdmin.actions = list(ResourceConfigAdmin.actions) + [
                run_resource_health_check
            ]
        else:
            ResourceConfigAdmin.actions.append(run_resource_health_check)
    else:
        ResourceConfigAdmin.actions = [run_resource_health_check]

except ImportError:
    pass
