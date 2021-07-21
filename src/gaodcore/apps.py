from django.apps import AppConfig


class GAODCORE(AppConfig):
    name = 'gaodcore'

    def ready(self):
        from gaodcore_project import signals  # noqa