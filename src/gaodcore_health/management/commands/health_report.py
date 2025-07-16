"""
Django management command to generate health reports.

This command generates detailed health reports for connectors.
"""

import json
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from gaodcore_health.health_check import get_connector_health_summary
from gaodcore_health.models import HealthCheckResult
from gaodcore_manager.models import ConnectorConfig


class Command(BaseCommand):
    help = "Generate health reports for connectors"

    def add_arguments(self, parser):
        parser.add_argument(
            "--connector-id", type=int, help="Generate report for specific connector"
        )
        parser.add_argument(
            "--hours", type=int, default=24, help="Hours to look back (default: 24)"
        )
        parser.add_argument(
            "--days", type=int, help="Days to look back (overrides --hours)"
        )
        parser.add_argument("--json", action="store_true", help="Output in JSON format")
        parser.add_argument("--csv", action="store_true", help="Output in CSV format")
        parser.add_argument(
            "--detailed", action="store_true", help="Include detailed error information"
        )

    def handle(self, *args, **options):
        """Main command handler."""
        try:
            # Calculate time period
            hours = options["hours"]
            if options["days"]:
                hours = options["days"] * 24

            # Generate summary
            summary = get_connector_health_summary(options["connector_id"], hours)

            # Output format
            if options["json"]:
                self.output_json(summary)
            elif options["csv"]:
                self.output_csv(summary)
            else:
                self.output_text(summary, options["detailed"])

        except Exception as e:
            raise CommandError(f"Report generation failed: {str(e)}")

    def output_json(self, summary: dict):
        """Output report in JSON format."""
        self.stdout.write(json.dumps(summary, indent=2, default=str))

    def output_csv(self, summary: dict):
        """Output report in CSV format."""
        self.stdout.write(
            "connector_name,connector_id,total_checks,healthy_checks,unhealthy_checks,success_rate,avg_response_time_ms,is_currently_healthy,latest_check"
        )

        for name, data in summary["connectors"].items():
            line = f"{name},{data['connector_id']},{data['total_checks']},{data['healthy_checks']},{data['unhealthy_checks']},{data['success_rate']:.1f},{data['avg_response_time_ms']},{data['is_currently_healthy']},{data['latest_check']}"
            self.stdout.write(line)

    def output_text(self, summary: dict, detailed: bool = False):
        """Output report in human-readable text format."""
        self.stdout.write(f"\nHealth Report - {summary['period']}")
        self.stdout.write("=" * 60)

        # Overall summary
        self.stdout.write(f"Total Checks: {summary['total_checks']}")
        self.stdout.write(f"Healthy Checks: {summary['healthy_checks']}")
        self.stdout.write(f"Unhealthy Checks: {summary['unhealthy_checks']}")

        if summary["total_checks"] > 0:
            success_rate = (summary["healthy_checks"] / summary["total_checks"]) * 100
            self.stdout.write(f"Overall Success Rate: {success_rate:.1f}%")

        # Connector details
        self.stdout.write("\nConnector Health Details:")
        self.stdout.write("-" * 60)

        for name, data in summary["connectors"].items():
            status_icon = "✓" if data["is_currently_healthy"] else "✗"
            status_color = (
                self.style.SUCCESS if data["is_currently_healthy"] else self.style.ERROR
            )

            self.stdout.write(status_color(f"{status_icon} {name}"))
            self.stdout.write(f"  Success Rate: {data['success_rate']:.1f}%")
            self.stdout.write(f"  Avg Response Time: {data['avg_response_time_ms']}ms")
            self.stdout.write(f"  Total Checks: {data['total_checks']}")
            self.stdout.write(f"  Healthy: {data['healthy_checks']}")
            self.stdout.write(f"  Unhealthy: {data['unhealthy_checks']}")
            self.stdout.write(f"  Last Check: {data['latest_check']}")

            if detailed and not data["is_currently_healthy"]:
                self.output_detailed_errors(data["connector_id"])

            self.stdout.write("")

    def output_detailed_errors(self, connector_id: int):
        """Output detailed error information for a connector."""
        recent_errors = HealthCheckResult.objects.filter(
            connector_id=connector_id,
            is_healthy=False,
            check_time__gte=timezone.now() - timedelta(hours=24),
        ).order_by("-check_time")[:5]

        if recent_errors.exists():
            self.stdout.write("  Recent Errors:")
            for error in recent_errors:
                self.stdout.write(
                    f"    {error.check_time}: {error.error_type} - {error.error_message}"
                )

    def generate_weekly_report(self):
        """Generate a weekly health report."""
        self.stdout.write("Weekly Health Report")
        self.stdout.write("=" * 50)

        # Get data for each day of the week
        for i in range(7):
            day_start = timezone.now() - timedelta(days=i + 1)
            day_end = timezone.now() - timedelta(days=i)

            results = HealthCheckResult.objects.filter(
                check_time__gte=day_start, check_time__lt=day_end
            )

            if results.exists():
                healthy_count = results.filter(is_healthy=True).count()
                total_count = results.count()
                success_rate = (healthy_count / total_count) * 100

                self.stdout.write(
                    f"{day_start.strftime('%Y-%m-%d')}: {success_rate:.1f}% ({healthy_count}/{total_count})"
                )
            else:
                self.stdout.write(f"{day_start.strftime('%Y-%m-%d')}: No data")

    def generate_connector_trend(self, connector_id: int, days: int = 7):
        """Generate trend analysis for a specific connector."""
        try:
            connector = ConnectorConfig.objects.get(id=connector_id)
        except ConnectorConfig.DoesNotExist:
            raise CommandError(f"Connector with ID {connector_id} not found")

        self.stdout.write(f"Trend Analysis for: {connector.name}")
        self.stdout.write("=" * 50)

        for i in range(days):
            day_start = timezone.now() - timedelta(days=i + 1)
            day_end = timezone.now() - timedelta(days=i)

            results = HealthCheckResult.objects.filter(
                connector=connector, check_time__gte=day_start, check_time__lt=day_end
            )

            if results.exists():
                healthy_count = results.filter(is_healthy=True).count()
                total_count = results.count()
                success_rate = (healthy_count / total_count) * 100

                # Calculate average response time
                from django.db import models

                avg_response = results.filter(
                    is_healthy=True, response_time_ms__isnull=False
                ).aggregate(avg=models.Avg("response_time_ms"))["avg"]

                avg_response_str = f"{avg_response:.0f}ms" if avg_response else "N/A"

                status = "✓" if success_rate > 90 else "⚠" if success_rate > 70 else "✗"

                self.stdout.write(
                    f"{day_start.strftime('%Y-%m-%d')}: {status} {success_rate:.1f}% (avg: {avg_response_str})"
                )
            else:
                self.stdout.write(f"{day_start.strftime('%Y-%m-%d')}: No data")
