"""GAODCore-transport app."""

from django.apps import AppConfig


class GAODCORETransportsConfig(AppConfig):
    """GAODCore-transport app."""
    name = 'gaodcore_transports'

    def ready(self):
        from gaodcore_project import signals  # noqa
