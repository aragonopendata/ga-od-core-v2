"""Tests for src/gunicorn.conf.py: settings from the environment and the recycling of workers that hold too much memory."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_CONF_PATH = Path(__file__).resolve().parents[2] / "gunicorn.conf.py"
MIB = 1024 * 1024


@pytest.fixture(name="conf")
def fixture_conf():
    """Load gunicorn.conf.py as a module (its name is not importable)."""
    spec = importlib.util.spec_from_file_location("gunicorn_conf_under_test", _CONF_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load(env):
    spec = importlib.util.spec_from_file_location("gunicorn_conf_under_test", _CONF_PATH)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict("os.environ", env, clear=True):
        spec.loader.exec_module(module)
    return module


def _worker():
    return SimpleNamespace(pid=1234, alive=True, log=mock.Mock())


def _request():
    return SimpleNamespace(method="GET", path="/GA_OD_Core/download")


def _run(conf, worker, rss_bytes, env_value=None):
    env = {} if env_value is None else {"GUNICORN_MAX_WORKER_RSS_MB": env_value}
    with mock.patch.dict("os.environ", env, clear=True), \
            mock.patch.object(conf, "_current_rss_bytes", return_value=rss_bytes):
        conf.post_request(worker, _request(), {}, None)


def test_worker_above_default_limit_is_recycled(conf):
    worker = _worker()
    _run(conf, worker, 3000 * MIB)
    assert worker.alive is False
    message = worker.log.warning.call_args[0][0] % worker.log.warning.call_args[0][1:]
    assert message == ("Recycling worker (pid:1234): RSS 3000 MiB above the 1024 MiB limit "
                       "after GET /GA_OD_Core/download")


def test_worker_below_limit_keeps_running(conf):
    worker = _worker()
    _run(conf, worker, 300 * MIB)
    assert worker.alive is True
    worker.log.warning.assert_not_called()


def test_limit_is_read_from_environment(conf):
    worker = _worker()
    _run(conf, worker, 600 * MIB, env_value="512")
    assert worker.alive is False


def test_zero_disables_the_check(conf):
    worker = _worker()
    _run(conf, worker, 9000 * MIB, env_value="0")
    assert worker.alive is True


def test_invalid_environment_value_falls_back_to_default(conf):
    worker = _worker()
    _run(conf, worker, 900 * MIB, env_value="mucho")
    assert worker.alive is True
    _run(conf, worker, 1100 * MIB, env_value="mucho")
    assert worker.alive is False


def test_unknown_rss_keeps_worker_running(conf):
    worker = _worker()
    _run(conf, worker, None)
    assert worker.alive is True


def test_current_rss_reads_this_process(conf):
    rss = conf._current_rss_bytes()  # pylint: disable=protected-access
    assert rss is None or rss > 10 * MIB


def test_defaults_without_environment(conf):
    module = _load({})
    assert module.worker_class == "gaodcore_project.gunicorn_worker.DrainingThreadWorker"
    assert (module.workers, module.threads) == (9, 4)
    assert (module.timeout, module.graceful_timeout) == (240, 1800)
    assert (module.max_requests, module.max_requests_jitter) == (1000, 100)
    assert module.worker_connections == 6
    assert module._max_worker_rss_bytes() == 1024 * MIB  # pylint: disable=protected-access


def test_settings_are_read_from_environment(conf):
    module = _load({
        "GUNICORN_WORKERS": "3",
        "GUNICORN_THREADS": "8",
        "GUNICORN_TIMEOUT": "60",
        "GUNICORN_GRACEFUL_TIMEOUT": "600",
        "GUNICORN_MAX_REQUESTS": "0",
        "GUNICORN_MAX_REQUESTS_JITTER": "0",
    })
    assert (module.workers, module.threads, module.timeout, module.graceful_timeout) == (3, 8, 60, 600)
    assert (module.max_requests, module.max_requests_jitter) == (0, 0)
    assert module.worker_connections == 10


def test_worker_connections_can_be_set_explicitly(conf):
    assert _load({"GUNICORN_THREADS": "2", "GUNICORN_WORKER_CONNECTIONS": "50"}).worker_connections == 50


@pytest.mark.parametrize("value", ["mucho", "", "  ", "1.5"], ids=["text", "empty", "blank", "float"])
def test_invalid_settings_fall_back_to_defaults(conf, value):
    module = _load({"GUNICORN_WORKERS": value, "GUNICORN_THREADS": value, "GUNICORN_TIMEOUT": value})
    assert (module.workers, module.threads, module.timeout) == (9, 4, 240)


@pytest.mark.parametrize("name", ["GUNICORN_WORKERS", "GUNICORN_THREADS"])
def test_workers_and_threads_are_at_least_one(conf, name):
    module = _load({name: "0"})
    assert min(module.workers, module.threads) >= 1


def test_worker_already_stopping_is_left_alone(conf):
    worker = _worker()
    worker.alive = False
    _run(conf, worker, 9000 * MIB)
    worker.log.warning.assert_not_called()


def test_worker_class_is_the_draining_one(conf):
    module = _load({})
    assert module.worker_class == "gaodcore_project.gunicorn_worker.DrainingThreadWorker"
    assert not hasattr(module, "pre_request")
