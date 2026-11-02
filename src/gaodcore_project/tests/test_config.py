"""Tests for the external YAML config models."""

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
