"""ArrivalTimeView without network: the upstream calls are replaced."""

import pytest
from django.core.cache import cache

from gaodcore_transports.views import zaragoza

URL = "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time.json"


@pytest.fixture(autouse=True)
def _clear_cache():
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def upstream(monkeypatch):
    arrivals = {
        "40": {"nombre": "Maria Agustin 59", "arrival_time": [{"id": "0", "name": "Línea 111", "remaining_time": "01:37:02"}]},
        "1": {"nombre": "Parada Desconocida", "arrival_time": []},
    }

    async def fake_download_async(_session, url, auth=None):
        return arrivals[url.rsplit("/", 1)[1]]

    monkeypatch.setattr(zaragoza, "_get_stops", lambda: [{"stop_id": "40"}, {"stop_id": "1"}])
    monkeypatch.setattr(zaragoza, "download_async", fake_download_async)


@pytest.mark.django_db
def test_each_arrival_row_carries_the_stop_in_stop_id_and_cod_parada(client, upstream):
    response = client.get(URL)
    assert response.status_code == 200
    assert response.json() == [
        {"id": "0", "name": "Línea 111", "remaining_time": "01:37:02", "stop_id": "40", "cod_parada": "40"}
    ]
