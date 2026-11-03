"""Tests for the external YAML config models."""

import pytest
from pydantic import ValidationError

from gaodcore_project.config import CommonConfig, Config


def _common_data() -> dict:
    return {
        "allowed_hosts": ["*"],
        "secret_key": "x",
        "debug": False,
        "databases": {},
        "cache_ttl": 1,
    }


def test_download_timing_log_defaults_to_true():
    assert CommonConfig(**_common_data()).download_timing_log is True


def test_download_timing_log_can_be_disabled():
    assert CommonConfig(**_common_data(), download_timing_log=False).download_timing_log is False


def test_config_tst_yaml_loads_with_timing_enabled():
    config = Config.get_config()
    assert config.common_config.download_timing_log is True


def test_oracle_arraysize_defaults_to_5000():
    assert CommonConfig(**_common_data()).oracle_arraysize == 5000


def test_oracle_arraysize_can_be_overridden():
    assert CommonConfig(**_common_data(), oracle_arraysize=250).oracle_arraysize == 250


def test_conn_max_age_defaults_to_60():
    assert CommonConfig(**_common_data()).conn_max_age == 60


def test_conn_max_age_can_be_overridden_and_zero_is_valid():
    assert CommonConfig(**_common_data(), conn_max_age=300).conn_max_age == 300
    assert CommonConfig(**_common_data(), conn_max_age=0).conn_max_age == 0


def test_conn_max_age_rejects_negative_values():
    with pytest.raises(ValidationError):
        CommonConfig(**_common_data(), conn_max_age=-1)


def test_settings_databases_use_persistent_connections():
    from gaodcore_project import settings as project_settings

    default = project_settings.DATABASES["default"]
    assert default["CONN_MAX_AGE"] == 60
    assert default["CONN_HEALTH_CHECKS"] is True
