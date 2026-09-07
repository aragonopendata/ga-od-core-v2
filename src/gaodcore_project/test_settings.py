"""Isolated Django settings for the local test suite.

Reuses gaodcore_project.settings (and therefore CONFIG_PATH, which should point at
config-tst.yaml) but forces an in-memory SQLite database and a local-memory cache so
tests never depend on a reachable PostgreSQL server or shared cache table.

Activation:
    export CONFIG_PATH=<repo>/config-tst.yaml
    export DJANGO_SETTINGS_MODULE=gaodcore_project.test_settings
    pytest src

Do not point CONFIG_PATH at a config file containing production credentials.
"""

from gaodcore_project.settings import *  # noqa: F401,F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}
