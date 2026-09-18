"""Populate the development database with deterministic demo data.

The generated ConnectorConfig and ResourceConfig records exist only to exercise
the read-only HTML manager by hand: list and detail pages, enabled/disabled
states, null and non-null optional fields, URI reveal and pagination.
"""

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from gaodcore_manager.models import ConnectorConfig, ResourceConfig

DEMO_PREFIX = "WEB-DEMO"

CONNECTOR_COUNT = 8
RESOURCE_COUNT = 120

# Fictitious URIs only: network hosts use the reserved `.invalid` domain so the
# data can never address a real service.
CONNECTOR_URIS = (
    "postgresql://demo_user:demo_password@postgres.demo.invalid/demo_db",
    "mysql://demo_user:demo_password@mysql.demo.invalid/demo_db",
    "oracle://demo_user:demo_password@oracle.demo.invalid/demo_service",
    "mssql://demo_user:demo_password@sqlserver.demo.invalid/demo_db",
    "sqlite:////tmp/gaodcore-web-demo.sqlite",
    "https://api.demo.invalid/data/example.json",
    "https://api.demo.invalid/data/example.csv",
    "https://api.demo.invalid/data/example.xml",
)

# Connectors 6, 7 and 8 are HTTP APIs: their resources have no object location.
FIRST_HTTP_CONNECTOR_NUMBER = 6


def connector_name(number: int) -> str:
    return f"{DEMO_PREFIX} Connector {number:02d}"


def resource_name(number: int) -> str:
    return f"{DEMO_PREFIX} Resource {number:03d}"


class Command(BaseCommand):
    help = "Create deterministic demo data to manually validate the HTML manager (development only)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Delete the demo data created by this command instead of populating it.",
        )

    def handle(self, *args, **options):
        if not settings.DEBUG:
            raise CommandError(
                "seed_manager_web_demo is a development-only utility and refuses to run "
                "when settings.DEBUG is False."
            )

        if options["clear"]:
            self._clear()
        else:
            self._populate()

    @transaction.atomic
    def _populate(self):
        connectors_created = connectors_updated = 0
        connectors = {}
        for number in range(1, CONNECTOR_COUNT + 1):
            connector, created = ConnectorConfig.objects.update_or_create(
                name=connector_name(number),
                defaults={
                    "uri": CONNECTOR_URIS[number - 1],
                    "enabled": number % 2 == 1,
                },
            )
            connectors[number] = connector
            if created:
                connectors_created += 1
            else:
                connectors_updated += 1

        resources_created = resources_updated = 0
        database_backed = 0
        for number in range(1, RESOURCE_COUNT + 1):
            connector_number = (number - 1) % CONNECTOR_COUNT + 1
            if connector_number >= FIRST_HTTP_CONNECTOR_NUMBER:
                object_location = None
                object_location_schema = None
            else:
                database_backed += 1
                object_location = f"demo_object_{number:03d}"
                object_location_schema = None if database_backed % 5 == 0 else "public"

            _, created = ResourceConfig.objects.update_or_create(
                name=resource_name(number),
                defaults={
                    "connector_config": connectors[connector_number],
                    "enabled": number % 3 != 0,
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                },
            )
            if created:
                resources_created += 1
            else:
                resources_updated += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Demo data ready: connectors created={connectors_created} "
                f"updated={connectors_updated}; resources created={resources_created} "
                f"updated={resources_updated}."
            )
        )

    @transaction.atomic
    def _clear(self):
        demo_connectors = ConnectorConfig.objects.filter(name__startswith=DEMO_PREFIX)
        foreign = ResourceConfig.objects.filter(connector_config__in=demo_connectors).exclude(
            name__startswith=DEMO_PREFIX
        )
        if foreign.exists():
            raise CommandError(
                "Refusing to clear: demo connectors are referenced by non-demo resources "
                f"({', '.join(foreign.values_list('name', flat=True)[:5])}). Nothing was deleted."
            )

        demo_resources = ResourceConfig.objects.filter(name__startswith=DEMO_PREFIX)
        resources_deleted = demo_resources.count()
        connectors_deleted = demo_connectors.count()
        demo_resources.delete()
        demo_connectors.delete()

        self.stdout.write(
            self.style.SUCCESS(
                f"Demo data cleared: resources deleted={resources_deleted}; "
                f"connectors deleted={connectors_deleted}."
            )
        )
