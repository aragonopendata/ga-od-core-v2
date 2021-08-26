"""GAODCore django application."""
from django.apps import AppConfig


class GAODCore(AppConfig):
    """GAODCore django application."""
    name = 'GA_OD_Core'

    def ready(self):
        from gaodcore_project import signals  # noqa
