
import pytest
from django.test import Client

@pytest.mark.parametrize("url,", [
    "/apireader/transportes/aragon/vehicles.json",
    "/apireader/transportes/aragon/drivers.json",
    "/apireader/transportes/aragon/live-position-latest.json",
    "/apireader/transportes/aragon/vehicle-journey-history-latest.json",
    "/apireader/transportes/aragon/distance-travelled.json",
    
    "/apireader/transportes/zaragoza/lines.json",
    "/apireader/transportes/zaragoza/stops.json",
    "/apireader/transportes/zaragoza/routes.json",
    "/apireader/transportes/zaragoza/stops_route.json",
    "/apireader/transportes/zaragoza/arrival_time.json",
    "/apireader/transportes/zaragoza/notices.json",
    "/apireader/transportes/zaragoza/origins.json",
    "/apireader/transportes/zaragoza/destinations.json",
    "/apireader/transportes/zaragoza/lines_ori_des.json",
    "/apireader/transportes/zaragoza/times_route.json",
    "/apireader/transportes/zaragoza/exp_ori_des.json",
    "/apireader/transportes/zaragoza/stops_ori_des.json",
    "/apireader/transportes/zaragoza/arrival_ori_des.json",
    "/apireader/transportes/zaragoza/sae.json"])
@pytest.mark.django_db #para ejecutar eso antes de la función
def test_transporte_views(client: Client, url: str):
    """
Con esto tenemos una instancia cliente.
Podremos realizar simulación de llamadas (lo que realizamos con request)
    """
    response = client.get(url)
    assert response.status_code == 200  #si no es un TRUE,      rompe la ejecución
    assert response.json()              #si no devuelve nada,   rompe la ejecución
