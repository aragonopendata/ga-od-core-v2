from django.apps import AppConfig


class GAODCORETransportsConfig(AppConfig):
    name = 'gaodcore_transports'

    def ready(self):
        from gaodcore_project import signals  # noqa