import pytest
from django.test import Client


@pytest.mark.parametrize("url,", [
    "/GA_OD_Core/gaodcore-transports/aragon/vehicles.csv",
    "/GA_OD_Core/gaodcore-transports/aragon/drivers.csv",
    "/GA_OD_Core/gaodcore-transports/aragon/live-position-latest.csv",
    "/GA_OD_Core/gaodcore-transports/aragon/vehicle-journey-history-latest.csv",
    "/GA_OD_Core/gaodcore-transports/aragon/distance-travelled.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/routes.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_route.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/notices.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/origins.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/destinations.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/lines_ori_des.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/times_route.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/exp_ori_des.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/stops_ori_des.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/arrival_ori_des.csv",
    "/GA_OD_Core/gaodcore-transports/zaragoza/sae.csv"
])
@pytest.mark.django_db
def test_transport_views(client: Client, url: str):
    response = client.get(url)
    assert response.status_code == 200
    assert response.content
