from itertools import count

import pytest

import gaodcore.operators as _operators_module


@pytest.fixture(autouse=True)
def reset_bind_counter():
    _operators_module._bind_counter = count(1)
