from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings

from gaodcore_manager.models import ConnectorConfig, ResourceConfig

DEMO_PREFIX = "WEB-DEMO"


def demo_connectors():
    return ConnectorConfig.objects.filter(name__startswith=DEMO_PREFIX)


def demo_resources():
    return ResourceConfig.objects.filter(name__startswith=DEMO_PREFIX)


@override_settings(DEBUG=True)
class SeedManagerWebDemoTestCase(TestCase):
    def test_population_creates_the_deterministic_demo_dataset(self):
        call_command("seed_manager_web_demo")

        self.assertEqual(demo_connectors().count(), 8)
        self.assertEqual(demo_resources().count(), 120)

        self.assertTrue(ConnectorConfig.objects.get(name="WEB-DEMO Connector 01").enabled)
        self.assertFalse(ConnectorConfig.objects.get(name="WEB-DEMO Connector 02").enabled)

        database_resource = ResourceConfig.objects.get(name="WEB-DEMO Resource 001")
        self.assertEqual(database_resource.object_location, "demo_object_001")
        self.assertEqual(database_resource.object_location_schema, "public")
        self.assertTrue(database_resource.enabled)

        api_resource = ResourceConfig.objects.get(name="WEB-DEMO Resource 006")
        self.assertIsNone(api_resource.object_location)
        self.assertIsNone(api_resource.object_location_schema)
        self.assertFalse(api_resource.enabled)

        self.assertTrue(demo_resources().filter(object_location__isnull=False,
                                                object_location_schema__isnull=True).exists())
        self.assertTrue(demo_connectors().filter(uri__startswith="https://").exists())

    def test_running_twice_is_idempotent_and_restores_altered_values(self):
        call_command("seed_manager_web_demo")
        ResourceConfig.objects.filter(name="WEB-DEMO Resource 001").update(
            enabled=False, object_location="tampered", object_location_schema=None
        )

        call_command("seed_manager_web_demo")

        self.assertEqual(demo_connectors().count(), 8)
        self.assertEqual(demo_resources().count(), 120)
        restored = ResourceConfig.objects.get(name="WEB-DEMO Resource 001")
        self.assertTrue(restored.enabled)
        self.assertEqual(restored.object_location, "demo_object_001")
        self.assertEqual(restored.object_location_schema, "public")

    def test_clear_removes_only_demo_records(self):
        other_connector = ConnectorConfig.objects.create(
            name="real-connector", uri="postgresql://u:p@real.example/db", enabled=True
        )
        other_resource = ResourceConfig.objects.create(
            name="real-resource", connector_config=other_connector, enabled=True,
            object_location="real_table",
        )

        call_command("seed_manager_web_demo")
        call_command("seed_manager_web_demo", "--clear")

        self.assertEqual(demo_connectors().count(), 0)
        self.assertEqual(demo_resources().count(), 0)
        other_connector.refresh_from_db()
        other_resource.refresh_from_db()
        self.assertEqual(other_resource.connector_config_id, other_connector.pk)
        self.assertEqual(other_resource.object_location, "real_table")

    @override_settings(DEBUG=False)
    def test_command_refuses_to_run_outside_debug(self):
        with self.assertRaises(CommandError):
            call_command("seed_manager_web_demo")
        with self.assertRaises(CommandError):
            call_command("seed_manager_web_demo", "--clear")
        self.assertEqual(demo_connectors().count(), 0)
