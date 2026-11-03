"""The thick-mode initialization of oracledb runs once per process even if several gunicorn threads ask for it."""

import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import connectors


def test_oracle_client_is_initialized_once_with_concurrent_threads():
    calls = []

    def slow_init():
        calls.append(threading.get_ident())
        time.sleep(0.1)  # a window for the other threads to get in if there were no lock

    fake_oracledb = SimpleNamespace(init_oracle_client=slow_init)
    barrier = threading.Barrier(6)

    def ask_for_engine():
        barrier.wait()
        try:
            connectors._get_engine("oracle://user:pass@localhost:1/XE")  # pylint: disable=protected-access
        except Exception:  # pylint: disable=broad-except
            pass  # the engine itself is not the point: nothing listens there

    with mock.patch.dict(sys.modules, {"oracledb": fake_oracledb}), \
            mock.patch.object(connectors, "_oracle_client_initialized", False):
        threads = [threading.Thread(target=ask_for_engine) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(calls) == 1
        assert connectors._oracle_client_initialized is True  # pylint: disable=protected-access
