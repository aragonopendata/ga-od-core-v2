"""
Health check logic for GA-OD-Core-v2.

This module provides functions to check the health of connectors and resources
by reusing existing validation logic and implementing async health checks.
"""

import asyncio
import logging
import time
from datetime import timedelta
from typing import List, Optional

from django.utils import timezone
from django.db import transaction
from django.db import models

from connectors import validate_uri, DriverConnectionError, NoObjectError
from gaodcore_manager.models import ConnectorConfig
from gaodcore_project.config import Config
from utils import gather_limited
from .models import HealthCheckResult, HealthCheckAlert


logger = logging.getLogger(__name__)


def check_connector_health_sync(connector: ConnectorConfig) -> HealthCheckResult:
    """
    Perform health check on a single connector (synchronous version).

    Args:
        connector: The ConnectorConfig instance to check

    Returns:
        HealthCheckResult: The result of the health check
    """
    logger.info(f"Starting health check for connector: {connector.name}")
    start_time = time.time()

    try:
        # Run validation synchronously
        validate_uri(connector.uri)

        response_time = int((time.time() - start_time) * 1000)

        result = HealthCheckResult(
            connector=connector,
            is_healthy=True,
            response_time_ms=response_time
        )

        logger.info(f"Health check passed for connector: {connector.name} ({response_time}ms)")
        return result

    except DriverConnectionError as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='connection_error'
        )
        logger.warning(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result

    except NoObjectError as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='object_error'
        )
        logger.warning(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result

    except Exception as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='unknown_error'
        )
        logger.error(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result


def check_all_connectors_health_sync(concurrency_limit: Optional[int] = None) -> List[HealthCheckResult]:
    """
    Perform health checks on all enabled connectors (synchronous version).

    Args:
        concurrency_limit: Maximum number of concurrent health checks (ignored in sync version)

    Returns:
        List[HealthCheckResult]: List of health check results
    """
    # Get concurrency limit from config if not provided
    if concurrency_limit is None:
        config = Config.get_config()
        concurrency_limit = config.common_config.health_monitoring.concurrency_limit

    connectors = ConnectorConfig.objects.filter(enabled=True)

    if not connectors.exists():
        logger.info("No enabled connectors found for health check")
        return []

    logger.info(f"Starting health checks for {connectors.count()} connectors")

    # Run health checks synchronously
    results = []
    for connector in connectors:
        result = check_connector_health_sync(connector)
        results.append(result)

    # Bulk save results to database
    with transaction.atomic():
        HealthCheckResult.objects.bulk_create(results)

    # Log summary
    healthy_count = sum(1 for result in results if result.is_healthy)
    unhealthy_count = len(results) - healthy_count

    logger.info(f"Health check completed: {healthy_count} healthy, {unhealthy_count} unhealthy")

    return results


def check_specific_connector_health_sync(connector_id: int) -> HealthCheckResult:
    """
    Check health of a specific connector by ID (synchronous version).

    Args:
        connector_id: ID of the connector to check

    Returns:
        HealthCheckResult: The health check result

    Raises:
        ConnectorConfig.DoesNotExist: If connector not found
    """
    connector = ConnectorConfig.objects.get(id=connector_id)
    result = check_connector_health_sync(connector)

    # Save individual result
    result.save()

    return result


async def check_connector_health(connector: ConnectorConfig) -> HealthCheckResult:
    """
    Perform health check on a single connector.

    Args:
        connector: The ConnectorConfig instance to check

    Returns:
        HealthCheckResult: The result of the health check
    """
    logger.info(f"Starting health check for connector: {connector.name}")
    start_time = time.time()

    try:
        # Run validation in a thread to avoid blocking the event loop
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, validate_uri, connector.uri)
        except RuntimeError:
            # No event loop running, run directly
            validate_uri(connector.uri)

        response_time = int((time.time() - start_time) * 1000)

        result = HealthCheckResult(
            connector=connector,
            is_healthy=True,
            response_time_ms=response_time
        )

        logger.info(f"Health check passed for connector: {connector.name} ({response_time}ms)")
        return result

    except DriverConnectionError as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='connection_error'
        )
        logger.warning(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result

    except NoObjectError as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='object_error'
        )
        logger.warning(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result

    except Exception as e:
        response_time = int((time.time() - start_time) * 1000)
        result = HealthCheckResult(
            connector=connector,
            is_healthy=False,
            response_time_ms=response_time,
            error_message=str(e),
            error_type='unknown_error'
        )
        logger.error(f"Health check failed for connector: {connector.name} - {str(e)}")
        return result


async def check_all_connectors_health(concurrency_limit: Optional[int] = None) -> List[HealthCheckResult]:
    """
    Perform health checks on all enabled connectors.

    Args:
        concurrency_limit: Maximum number of concurrent health checks (defaults to config)

    Returns:
        List[HealthCheckResult]: List of health check results
    """
    # Get concurrency limit from config if not provided
    if concurrency_limit is None:
        config = Config.get_config()
        concurrency_limit = config.common_config.health_monitoring.concurrency_limit

    connectors = ConnectorConfig.objects.filter(enabled=True)

    if not connectors.exists():
        logger.info("No enabled connectors found for health check")
        return []

    logger.info(f"Starting health checks for {connectors.count()} connectors with concurrency limit {concurrency_limit}")

    # Create health check tasks
    tasks = [check_connector_health(connector) for connector in connectors]

    # Use existing gather_limited for concurrency control
    results = await gather_limited(concurrency_limit, tasks)

    # Bulk save results to database
    with transaction.atomic():
        HealthCheckResult.objects.bulk_create(results)

    # Log summary
    healthy_count = sum(1 for result in results if result.is_healthy)
    unhealthy_count = len(results) - healthy_count

    logger.info(f"Health check completed: {healthy_count} healthy, {unhealthy_count} unhealthy")

    return results


async def check_specific_connector_health(connector_id: int) -> HealthCheckResult:
    """
    Check health of a specific connector by ID.

    Args:
        connector_id: ID of the connector to check

    Returns:
        HealthCheckResult: The health check result

    Raises:
        ConnectorConfig.DoesNotExist: If connector not found
    """
    connector = ConnectorConfig.objects.get(id=connector_id)
    result = await check_connector_health(connector)

    # Save individual result
    result.save()

    return result


def check_and_send_alerts():
    """
    Check for failing connectors and send alerts based on configured thresholds.

    This function checks recent health check results and sends alerts
    for connectors that have failed according to their alert configurations.
    """
    logger.info("Checking for health check alerts")

    alerts = HealthCheckAlert.objects.filter(is_active=True).select_related('connector')

    for alert in alerts:
        connector = alert.connector

        # Get recent health check results for this connector
        recent_results = HealthCheckResult.objects.filter(
            connector=connector,
            check_time__gte=timezone.now() - timedelta(minutes=alert.threshold_minutes)
        ).order_by('-check_time')

        if not recent_results.exists():
            continue

        # Check for consecutive failures
        if alert.alert_type == 'consecutive_failures':
            consecutive_failures = 0
            for result in recent_results:
                if not result.is_healthy:
                    consecutive_failures += 1
                else:
                    break

            if consecutive_failures >= alert.consecutive_failures_threshold:
                if alert.should_send_alert():
                    send_alert(alert, f"Connector {connector.name} has {consecutive_failures} consecutive failures")
                    alert.mark_alert_sent()

        # Check for general failures
        elif alert.alert_type == 'failure':
            latest_result = recent_results.first()
            if latest_result and not latest_result.is_healthy:
                if alert.should_send_alert():
                    send_alert(alert, f"Connector {connector.name} health check failed: {latest_result.error_message}")
                    alert.mark_alert_sent()

        # Check for recovery
        elif alert.alert_type == 'recovery':
            if recent_results.count() >= 2:
                latest_result = recent_results.first()
                previous_result = recent_results[1]

                if latest_result.is_healthy and not previous_result.is_healthy:
                    if alert.should_send_alert():
                        send_alert(alert, f"Connector {connector.name} has recovered")
                        alert.mark_alert_sent()


def send_alert(alert: HealthCheckAlert, message: str):
    """
    Send an alert notification.

    Args:
        alert: The HealthCheckAlert configuration
        message: The alert message to send
    """
    logger.warning(f"HEALTH ALERT: {message}")

    # TODO: Implement actual alert sending (email, webhook, etc.)
    # For now, just log the alert
    print(f"HEALTH ALERT [{alert.alert_type}]: {message}")


def cleanup_old_health_results(retention_days: Optional[int] = None):
    """
    Clean up old health check results to prevent database bloat.

    Args:
        retention_days: Number of days to retain health check results (defaults to config)
    """
    # Get retention days from config if not provided
    if retention_days is None:
        config = Config.get_config()
        retention_days = config.common_config.health_monitoring.retention_days

    cutoff_date = timezone.now() - timedelta(days=retention_days)

    deleted_count = HealthCheckResult.objects.filter(
        check_time__lt=cutoff_date
    ).delete()[0]

    logger.info(f"Cleaned up {deleted_count} old health check results (retention: {retention_days} days)")


def get_connector_health_summary(connector_id: Optional[int] = None, hours: int = 24) -> dict:
    """
    Get health summary for connectors.

    Args:
        connector_id: Optional specific connector ID to get summary for
        hours: Number of hours to look back

    Returns:
        dict: Health summary data
    """
    since = timezone.now() - timedelta(hours=hours)

    results_query = HealthCheckResult.objects.filter(check_time__gte=since)

    if connector_id:
        results_query = results_query.filter(connector_id=connector_id)

    results = results_query.select_related('connector')

    if not results.exists():
        return {
            'period': f'Last {hours} hours',
            'total_checks': 0,
            'healthy_checks': 0,
            'unhealthy_checks': 0,
            'connectors': {}
        }

    summary = {
        'period': f'Last {hours} hours',
        'total_checks': results.count(),
        'healthy_checks': results.filter(is_healthy=True).count(),
        'unhealthy_checks': results.filter(is_healthy=False).count(),
        'connectors': {}
    }

    # Group by connector
    connectors = ConnectorConfig.objects.filter(enabled=True)
    if connector_id:
        connectors = connectors.filter(id=connector_id)

    for connector in connectors:
        connector_results = results.filter(connector=connector)

        if connector_results.exists():
            healthy_count = connector_results.filter(is_healthy=True).count()
            total_count = connector_results.count()

            # Calculate average response time for healthy checks
            healthy_results = connector_results.filter(is_healthy=True, response_time_ms__isnull=False)
            avg_response_time = None
            if healthy_results.exists():
                avg_response_time = healthy_results.aggregate(
                    avg=models.Avg('response_time_ms')
                )['avg']

            summary['connectors'][connector.name] = {
                'connector_id': connector.id,
                'total_checks': total_count,
                'healthy_checks': healthy_count,
                'unhealthy_checks': total_count - healthy_count,
                'success_rate': (healthy_count / total_count * 100) if total_count > 0 else 0,
                'avg_response_time_ms': round(avg_response_time) if avg_response_time else None,
                'latest_check': connector_results.first().check_time,
                'is_currently_healthy': connector_results.first().is_healthy
            }

    return summary
