from django.apps import AppConfig


class GAODCORE(AppConfig):
    name = 'GA_OD_CORE'

    def ready(self):
        from gaodcore_project import signals  # noqa
