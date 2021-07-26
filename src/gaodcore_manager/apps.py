from django.apps import AppConfig


class GaodcoreManagerConfig(AppConfig):
    name = 'gaodcore_manager'

    def ready(self):
        from gaodcore_project import signals  # noqa
        