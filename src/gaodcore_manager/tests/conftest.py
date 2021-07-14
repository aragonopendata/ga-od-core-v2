import pytest

import connectors


@pytest.fixture
def mock_resource_max_rows(monkeypatch):
    monkeypatch.setattr(connectors, "_RESOURCE_MAX_ROWS", 1)
