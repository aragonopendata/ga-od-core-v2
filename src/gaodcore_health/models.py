from django.db import models
from django.utils import timezone
from gaodcore_manager.models import ConnectorConfig, ResourceConfig


class HealthCheckResult(models.Model):
    """
    Model to store health check results for connectors.

    This model tracks the health status of each connector over time,
    storing response times, error messages, and health status.
    """

    connector = models.ForeignKey(
        ConnectorConfig,
        on_delete=models.CASCADE,
        help_text="The connector that was checked",
    )
    check_time = models.DateTimeField(
        auto_now_add=True, help_text="Timestamp when the health check was performed"
    )
    is_healthy = models.BooleanField(
        help_text="Whether the connector is healthy (True) or not (False)"
    )
    response_time_ms = models.IntegerField(
        null=True, blank=True, help_text="Response time in milliseconds"
    )
    error_message = models.TextField(
        null=True, blank=True, help_text="Error message if health check failed"
    )
    error_type = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Type of error (connection_error, timeout, unknown_error, etc.)",
    )

    class Meta:
        ordering = ["-check_time"]
        indexes = [
            models.Index(fields=["connector", "-check_time"]),
            models.Index(fields=["is_healthy", "-check_time"]),
            models.Index(fields=["check_time"]),
        ]

    def __str__(self):
        status = "healthy" if self.is_healthy else "unhealthy"
        return f"{self.connector.name} - {status} at {self.check_time}"


class HealthCheckSchedule(models.Model):
    """
    Model to manage health check scheduling configuration.

    This model defines when and how often health checks should run.
    """

    name = models.CharField(
        max_length=255, unique=True, help_text="Name of the health check schedule"
    )
    interval_minutes = models.IntegerField(
        default=5, help_text="Interval between health checks in minutes"
    )
    enabled = models.BooleanField(
        default=True, help_text="Whether this schedule is enabled"
    )
    last_run = models.DateTimeField(
        null=True, blank=True, help_text="Timestamp of the last health check run"
    )
    next_run = models.DateTimeField(
        null=True, blank=True, help_text="Timestamp of the next scheduled health check"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return f"{self.name} - every {self.interval_minutes} minutes"

    def update_run_times(self):
        """Update last_run to now and calculate next_run"""
        from datetime import timedelta

        self.last_run = timezone.now()
        self.next_run = self.last_run + timedelta(minutes=self.interval_minutes)
        self.save()


class HealthCheckAlert(models.Model):
    """
    Model to configure alerts for health check failures.

    This model defines alerting rules for when connectors fail health checks.
    """

    ALERT_TYPES = [
        ("failure", "Failure"),
        ("recovery", "Recovery"),
        ("timeout", "Timeout"),
        ("consecutive_failures", "Consecutive Failures"),
    ]

    connector = models.ForeignKey(
        ConnectorConfig,
        on_delete=models.CASCADE,
        help_text="The connector to monitor for alerts",
    )
    alert_type = models.CharField(
        max_length=50,
        choices=ALERT_TYPES,
        default="failure",
        help_text="Type of alert (failure, recovery, timeout, consecutive_failures)",
    )
    threshold_minutes = models.IntegerField(
        default=5, help_text="Threshold in minutes before triggering alert"
    )
    consecutive_failures_threshold = models.IntegerField(
        default=3, help_text="Number of consecutive failures before triggering alert"
    )
    is_active = models.BooleanField(
        default=True, help_text="Whether this alert is active"
    )
    last_alert_time = models.DateTimeField(
        null=True, blank=True, help_text="Timestamp of the last alert sent"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["connector__name", "alert_type"]
        unique_together = ["connector", "alert_type"]

    def __str__(self):
        return f"{self.connector.name} - {self.alert_type} alert"

    def should_send_alert(self):
        """Check if an alert should be sent based on threshold"""
        if not self.is_active:
            return False

        if self.last_alert_time is None:
            return True

        from datetime import timedelta

        threshold_time = timezone.now() - timedelta(minutes=self.threshold_minutes)
        return self.last_alert_time <= threshold_time

    def mark_alert_sent(self):
        """Mark that an alert has been sent"""
        self.last_alert_time = timezone.now()
        self.save()


class ResourceHealthCheckResult(models.Model):
    """
    Model to store health check results for resources.

    This model tracks the health status of each resource over time,
    storing response times, error messages, and health status.
    """

    resource = models.ForeignKey(
        ResourceConfig,
        on_delete=models.CASCADE,
        help_text="The resource that was checked",
    )
    check_time = models.DateTimeField(
        auto_now_add=True, help_text="Timestamp when the health check was performed"
    )
    is_healthy = models.BooleanField(
        help_text="Whether the resource is healthy (True) or not (False)"
    )
    response_time_ms = models.IntegerField(
        null=True, blank=True, help_text="Response time in milliseconds"
    )
    error_message = models.TextField(
        null=True, blank=True, help_text="Error message if health check failed"
    )
    error_type = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Type of error (connection_error, timeout, object_error, unknown_error, etc.)",
    )

    class Meta:
        ordering = ["-check_time"]
        indexes = [
            models.Index(fields=["resource", "-check_time"]),
            models.Index(fields=["is_healthy", "-check_time"]),
            models.Index(fields=["check_time"]),
        ]

    def __str__(self):
        status = "healthy" if self.is_healthy else "unhealthy"
        return f"{self.resource.name} - {status} at {self.check_time}"


class ResourceHealthCheckAlert(models.Model):
    """
    Model to configure alerts for resource health check failures.

    This model defines alerting rules for when resources fail health checks.
    """

    ALERT_TYPES = [
        ("failure", "Failure"),
        ("recovery", "Recovery"),
        ("timeout", "Timeout"),
        ("consecutive_failures", "Consecutive Failures"),
    ]

    resource = models.ForeignKey(
        ResourceConfig,
        on_delete=models.CASCADE,
        help_text="The resource to monitor for alerts",
    )
    alert_type = models.CharField(
        max_length=50,
        choices=ALERT_TYPES,
        default="failure",
        help_text="Type of alert (failure, recovery, timeout, consecutive_failures)",
    )
    threshold_minutes = models.IntegerField(
        default=5, help_text="Threshold in minutes before triggering alert"
    )
    consecutive_failures_threshold = models.IntegerField(
        default=3, help_text="Number of consecutive failures before triggering alert"
    )
    is_active = models.BooleanField(
        default=True, help_text="Whether this alert is active"
    )
    last_alert_time = models.DateTimeField(
        null=True, blank=True, help_text="Timestamp of the last alert sent"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["resource__name", "alert_type"]
        unique_together = ["resource", "alert_type"]

    def __str__(self):
        return f"{self.resource.name} - {self.alert_type} alert"

    def should_send_alert(self):
        """Check if an alert should be sent based on threshold"""
        if not self.is_active:
            return False

        if self.last_alert_time is None:
            return True

        from datetime import timedelta

        threshold_time = timezone.now() - timedelta(minutes=self.threshold_minutes)
        return self.last_alert_time <= threshold_time

    def mark_alert_sent(self):
        """Mark that an alert has been sent"""
        self.last_alert_time = timezone.now()
        self.save()
