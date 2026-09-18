import logging

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class GaodcoreManagerConfig(AppConfig):
    name = 'gaodcore_manager'

    def ready(self):
        from gaodcore_project import signals  # noqa

        warn_if_external_validation_disabled()


def warn_if_external_validation_disabled():
    """Makes an unsafe deployment visible in the logs, once per process."""
    if not getattr(settings, "GAODCORE_VALIDATE_EXTERNAL_CONNECTIONS", True):
        logger.warning(
            "External connector and resource validation is disabled. "
            "Configurations may be saved without verifying availability."
        )
