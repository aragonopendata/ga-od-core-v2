"""
Django management command to run health checks on connectors.

This command can be used to perform health checks manually or scheduled via cron.
"""

import json

from django.core.management.base import BaseCommand, CommandError

from gaodcore_health.health_check import (
    check_all_connectors_health_sync,
    check_specific_connector_health_sync,
    check_all_resources_health_sync,
    check_specific_resource_health_sync,
    check_and_send_alerts,
    cleanup_old_health_results,
    get_connector_health_summary,
)
from gaodcore_health.models import HealthCheckSchedule
from gaodcore_manager.models import ConnectorConfig, ResourceConfig


class Command(BaseCommand):
    help = "Perform health checks on connectors and resources"

    def add_arguments(self, parser):
        parser.add_argument(
            "--connector-id", type=int, help="Check specific connector by ID"
        )
        parser.add_argument(
            "--resource-id", type=int, help="Check specific resource by ID"
        )
        parser.add_argument(
            "--type",
            choices=["connectors", "resources", "both"],
            default="both",
            help="Type of health check to perform (default: both)",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=5,
            help="Maximum number of concurrent health checks (default: 5)",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            help="Timeout in seconds for health checks (uses config default if not specified)",
        )
        parser.add_argument(
            "--report", action="store_true", help="Generate and display health report"
        )
        parser.add_argument(
            "--report-hours",
            type=int,
            default=24,
            help="Hours to look back for health report (default: 24)",
        )
        parser.add_argument(
            "--alerts",
            action="store_true",
            help="Check and send alerts based on health results",
        )
        parser.add_argument(
            "--cleanup", action="store_true", help="Clean up old health check results"
        )
        parser.add_argument(
            "--retention-days",
            type=int,
            default=30,
            help="Days to retain health check results (default: 30)",
        )
        parser.add_argument(
            "--json", action="store_true", help="Output results in JSON format"
        )
        parser.add_argument(
            "--quiet", action="store_true", help="Suppress output except for errors"
        )

    def handle(self, *args, **options):
        """Main command handler."""
        try:
            # Set up logging level
            if options["quiet"]:
                self.stdout = open("/dev/null", "w")

            # Handle cleanup
            if options["cleanup"]:
                self.cleanup_old_results(options["retention_days"])
                return

            # Handle alerts
            if options["alerts"]:
                self.check_alerts()
                return

            # Handle report generation
            if options["report"]:
                self.generate_report(
                    options["connector_id"], options["report_hours"], options["json"]
                )
                return

            # Handle health checks
            if options["connector_id"]:
                self.check_specific_connector(
                    options["connector_id"], options["json"], options["timeout"]
                )
            elif options["resource_id"]:
                self.check_specific_resource(
                    options["resource_id"], options["json"], options["timeout"]
                )
            else:
                check_type = options["type"]
                if check_type == "connectors":
                    self.check_all_connectors(
                        options["concurrency"], options["json"], options["timeout"]
                    )
                elif check_type == "resources":
                    self.check_all_resources(
                        options["concurrency"], options["json"], options["timeout"]
                    )
                else:  # both
                    self.check_all_connectors(
                        options["concurrency"], options["json"], options["timeout"]
                    )
                    self.check_all_resources(
                        options["concurrency"], options["json"], options["timeout"]
                    )

        except Exception as e:
            raise CommandError(f"Health check failed: {str(e)}")

    def check_specific_connector(
        self, connector_id: int, json_output: bool = False, timeout: int = None
    ):
        """Check health of a specific connector."""
        try:
            result = check_specific_connector_health_sync(connector_id, timeout=timeout)

            if json_output:
                output = {
                    "connector_id": result.connector.id,
                    "connector_name": result.connector.name,
                    "is_healthy": result.is_healthy,
                    "check_time": result.check_time.isoformat(),
                    "response_time_ms": result.response_time_ms,
                    "error_message": result.error_message,
                    "error_type": result.error_type,
                }
                self.stdout.write(json.dumps(output, indent=2))
            else:
                status = "✓ HEALTHY" if result.is_healthy else "✗ UNHEALTHY"
                self.stdout.write(f"Connector: {result.connector.name}")
                self.stdout.write(f"Status: {status}")
                self.stdout.write(f"Response Time: {result.response_time_ms}ms")

                if not result.is_healthy:
                    self.stdout.write(
                        self.style.ERROR(f"Error: {result.error_message}")
                    )

        except ConnectorConfig.DoesNotExist:
            raise CommandError(f"Connector with ID {connector_id} not found")

    def check_all_connectors(
        self, concurrency: int, json_output: bool = False, timeout: int = None
    ):
        """Check health of all enabled connectors."""
        results = check_all_connectors_health_sync(concurrency, timeout=timeout)

        if json_output:
            output = []
            for result in results:
                output.append(
                    {
                        "connector_id": result.connector.id,
                        "connector_name": result.connector.name,
                        "is_healthy": result.is_healthy,
                        "check_time": result.check_time.isoformat(),
                        "response_time_ms": result.response_time_ms,
                        "error_message": result.error_message,
                        "error_type": result.error_type,
                    }
                )
            self.stdout.write(json.dumps(output, indent=2))
        else:
            healthy_count = sum(1 for r in results if r.is_healthy)
            unhealthy_count = len(results) - healthy_count

            self.stdout.write("\nHealth Check Summary:")
            self.stdout.write(f"Total Connectors: {len(results)}")
            self.stdout.write(self.style.SUCCESS(f"Healthy: {healthy_count}"))

            if unhealthy_count > 0:
                self.stdout.write(self.style.ERROR(f"Unhealthy: {unhealthy_count}"))

            self.stdout.write("\nDetailed Results:")
            for result in results:
                status = "✓" if result.is_healthy else "✗"
                style = self.style.SUCCESS if result.is_healthy else self.style.ERROR

                line = f"{status} {result.connector.name} ({result.response_time_ms}ms)"
                if not result.is_healthy:
                    error_info = result.error_message
                    if result.error_type:
                        error_info = f"[{result.error_type}] {error_info}"
                    line += f" - {error_info}"

                self.stdout.write(style(line))

    def generate_report(
        self, connector_id: int = None, hours: int = 24, json_output: bool = False
    ):
        """Generate health report."""
        summary = get_connector_health_summary(connector_id, hours)

        if json_output:
            self.stdout.write(json.dumps(summary, indent=2, default=str))
        else:
            self.stdout.write(f"\nHealth Report - {summary['period']}")
            self.stdout.write("=" * 50)
            self.stdout.write(f"Total Checks: {summary['total_checks']}")
            self.stdout.write(f"Healthy Checks: {summary['healthy_checks']}")
            self.stdout.write(f"Unhealthy Checks: {summary['unhealthy_checks']}")

            if summary["total_checks"] > 0:
                success_rate = (
                    summary["healthy_checks"] / summary["total_checks"]
                ) * 100
                self.stdout.write(f"Overall Success Rate: {success_rate:.1f}%")

            self.stdout.write("\nConnector Details:")
            for name, data in summary["connectors"].items():
                status = "✓" if data["is_currently_healthy"] else "✗"
                self.stdout.write(f"{status} {name}:")
                self.stdout.write(f"  Success Rate: {data['success_rate']:.1f}%")
                self.stdout.write(
                    f"  Avg Response Time: {data['avg_response_time_ms']}ms"
                )
                self.stdout.write(f"  Total Checks: {data['total_checks']}")
                self.stdout.write(f"  Last Check: {data['latest_check']}")

    def check_alerts(self):
        """Check and send alerts."""
        self.stdout.write("Checking for health alerts...")
        check_and_send_alerts()
        self.stdout.write("Alert check completed.")

    def cleanup_old_results(self, retention_days: int):
        """Clean up old health check results."""
        self.stdout.write(
            f"Cleaning up health results older than {retention_days} days..."
        )
        cleanup_old_health_results(retention_days)
        self.stdout.write("Cleanup completed.")

    def update_schedule(self, schedule_name: str = "default"):
        """Update the health check schedule."""
        try:
            schedule = HealthCheckSchedule.objects.get(name=schedule_name)
            schedule.update_run_times()
            self.stdout.write(f"Updated schedule: {schedule_name}")
        except HealthCheckSchedule.DoesNotExist:
            # Create default schedule if it doesn't exist
            schedule = HealthCheckSchedule.objects.create(
                name=schedule_name, interval_minutes=5, enabled=True
            )
            schedule.update_run_times()
            self.stdout.write(f"Created default schedule: {schedule_name}")

    def check_specific_resource(
        self, resource_id: int, json_output: bool = False, timeout: int = None
    ):
        """Check health of a specific resource."""
        try:
            result = check_specific_resource_health_sync(resource_id, timeout=timeout)

            if json_output:
                output = {
                    "resource_id": result.resource.id,
                    "resource_name": result.resource.name,
                    "is_healthy": result.is_healthy,
                    "check_time": result.check_time.isoformat(),
                    "response_time_ms": result.response_time_ms,
                    "error_message": result.error_message,
                    "error_type": result.error_type,
                }
                self.stdout.write(json.dumps(output, indent=2))
            else:
                status = "✓ HEALTHY" if result.is_healthy else "✗ UNHEALTHY"
                self.stdout.write(f"Resource: {result.resource.name}")
                self.stdout.write(f"Status: {status}")
                self.stdout.write(f"Response Time: {result.response_time_ms}ms")
                if result.error_message:
                    self.stdout.write(f"Error: {result.error_message}")

        except ResourceConfig.DoesNotExist:
            raise CommandError(f"Resource with ID {resource_id} not found")

    def check_all_resources(
        self, concurrency: int, json_output: bool = False, timeout: int = None
    ):
        """Check health of all enabled resources."""
        results = check_all_resources_health_sync(concurrency, timeout=timeout)

        if json_output:
            output = []
            for result in results:
                output.append(
                    {
                        "resource_id": result.resource.id,
                        "resource_name": result.resource.name,
                        "is_healthy": result.is_healthy,
                        "check_time": result.check_time.isoformat(),
                        "response_time_ms": result.response_time_ms,
                        "error_message": result.error_message,
                        "error_type": result.error_type,
                    }
                )
            self.stdout.write(json.dumps(output, indent=2))
        else:
            healthy_count = sum(1 for r in results if r.is_healthy)
            unhealthy_count = len(results) - healthy_count

            self.stdout.write("\nResource Health Check Summary:")
            self.stdout.write(f"Total Resources: {len(results)}")
            self.stdout.write(self.style.SUCCESS(f"Healthy: {healthy_count}"))

            if unhealthy_count > 0:
                self.stdout.write(self.style.ERROR(f"Unhealthy: {unhealthy_count}"))

                # Show unhealthy resources
                for result in results:
                    if not result.is_healthy:
                        error_info = result.error_message
                        if result.error_type:
                            error_info = f"[{result.error_type}] {error_info}"
                        self.stdout.write(
                            self.style.ERROR(
                                f"  - {result.resource.name}: {error_info}"
                            )
                        )
