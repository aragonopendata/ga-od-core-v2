import pytest
from django.test import Client


@pytest.mark.parametrize("url,", [
    "/GA_OD_Core/gaodcore-transports/aragon/vehicles.json",
    "/GA_OD_Core/gaodcore-transports/aragon/drivers.json",
    "/GA_OD_Core/gaodcore-transports/aragon/live-position-latest.json",
    "/GA_OD_Core/gaodcore-transports/aragon/vehicle-journey-history-latest.json",
    "/GA_OD_Core/gaodcore-transports/aragon/distance-travelled.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/routes.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_route.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/notices.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/origins.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/destinations.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines_ori_des.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/times_route.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/exp_ori_des.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_ori_des.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_ori_des.json",
    "/GA_OD_Core/gaodcore-transports/zaragoza/sae.json",

])
@pytest.mark.django_db
def test_transport_views_json(client: Client, url: str):
    response = client.get(url)
    assert response.status_code == 200
    assert response.content

    for row in response.json():
        for value in row.values():
            assert type(value) in [int, str, float, bool]


@pytest.mark.parametrize("url,", [
    "/GA_OD_Core/gaodcore-transports/aragon/vehicles.xlsx",
    "/GA_OD_Core/gaodcore-transports/aragon/drivers.xlsx",
    "/GA_OD_Core/gaodcore-transports/aragon/live-position-latest.xlsx",
    "/GA_OD_Core/gaodcore-transports/aragon/vehicle-journey-history-latest.xlsx",
    "/GA_OD_Core/gaodcore-transports/aragon/distance-travelled.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/routes.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_route.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/notices.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/origins.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/destinations.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines_ori_des.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/times_route.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/exp_ori_des.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_ori_des.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_ori_des.xlsx",
    "/GA_OD_Core/gaodcore-transports/zaragoza/sae.xlsx"
])
@pytest.mark.django_db
def test_transport_views_xlsx(client: Client, url: str):
    response = client.get(url)
    assert response.status_code == 200
    assert response.content
