import csv
import io
from typing import Dict

import pandas
import pytest
import yaml
import xml.etree.ElementTree as ET

from django.test import Client


@pytest.mark.parametrize("url, fields", [
    ["/GA_OD_Core/gaodcore-transports/aragon/vehicles",
    {'id': str, 'registration': str, 'name': str, 'groupId': str, 'groupName': str, 'odometerValue': float,
     'odometerType': str, 'featureTags_0': str, 'defaultDriverId': str, 'status': str, 'idlingFidelity': int,
     'engineTotalHours': float}],
    ["/GA_OD_Core/gaodcore-transports/aragon/drivers", {'id': str, 'name': str, 'groupId': str, 'groupName': str, 'customerId': str, 'active': bool, 'activeDate': str, 'defaultVehicleId': str}],
    ["/GA_OD_Core/gaodcore-transports/aragon/live-position-latest", {'assetRegistration': str, 'assetName': str, 'assetId': str, 'driverId': str, 'driverName': str, 'driverGroupId': str,  'driverGroupName': str, 'assetGroupId': str, 'assetGroupName': str, 'date': str, 'status': str, 'eventType': str, 'speed': float, 'odometer':float, 'heading': float, 'latitude': float, 'longitude': float, 'road': str, 'roadNumber': str, 'city': str, 'postCode': str, 'country': str, 'formattedAddress': str, 'privacy': bool, 'assetType': str}],
    ["/GA_OD_Core/gaodcore-transports/aragon/vehicle-journey-history-latest", {'eventDate': str, 'vehicleId': str, 'vehicleRegistration': str, 'vehicleName': str, 'vehicleGroupName': str, 'driverId': str, 'driverName': str, 'driverGroupName': str, 'eventStatus': str, 'longitude': float, 'latitude': float, 'formattedAddress': str, 'speed': float, 'heading': float, 'cumulativeRuntime': int, 'cumulativeRuntimeType': str, 'cumulativeIdleTime': int, 'cumulativePtoTime': int , 'cumulativeDistance': float, 'cumulativeFuelUsage': float}],
    ["/GA_OD_Core/gaodcore-transports/aragon/distance-travelled", {'assetRegistration': str, 'assetId': str, 'assetName':str, 'groupName':str, 'distance':float, 'odometerType': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/lines", {'id': str, 'id_linea': str, 'name': str, 'from': str, 'to': str, 'url': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/stops", {'stop_id': str, 'stop_name': str, 'stop_lat': str, 'stop_lon': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/routes", {'depttime': str, 'isreturn': str, 'name': str, 'route': str, 'from': str, 'to': str, 'line_id': str }],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/stops_route", {'number': str, 'code': str, 'name': str, 'latitud': str, 'longitud': str, 'line_id': str, 'route': str, 'isreturn': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time", {'id': str, 'id_linea': str, 'name':str, 'route': str, 'direction': str, 'bus': str, 'departure_time': str, 'reamining_time': str, 'TR': str, 'inc': str, 'url': str, 'stop_id': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/notices", {'id': str, 'id_linea': str, 'texto': str, 'url':str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/origins", {'orden': str, 'id': str, 'origin': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/destinations", {'id': str, 'destination': str, 'origin_id': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/lines_ori_des", {'id': str, 'id_linea': str, 'name': str, 'origin_id': str, 'destination_id': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/times_route", {'order': str, 'code': str, 'name': str, 'arrival_time': str, 'line_id': str, 'direction': int, 'bus': int, 'departure_time': int}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/exp_ori_des", {'line': str, 'departure_time': str, 'ndepttime': str, 'direction': str, 'code': str, 'name': str, 'route': str, 'from': str, 'to': str, 'origin_id': str, 'destination_id': str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/stops_ori_des", {'orden': str, 'code': str, 'municipio':str, 'name': str, 'origin_id': str, 'destination_id':str}],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/arrival_ori_des", {'code': str, 'name': str, 'origen': str, 'destino': str, 'id': str, 'route': str, 'direction': str, 'url':str, 'departure_time': str, 'origin_id': str, 'destination_id': str }],
    ["/GA_OD_Core/gaodcore-transports/zaragoza/sae", {'bus':str, 'linea': str, 'nombre_linea':str, 'latitud': str, 'longitud': str, 'momento': str}],
])

@pytest.mark.parametrize("accept,", ['text/html', 'application/yaml', 'application/json', 'text/csv', 'application/xlsx', 'application/xml'])
@pytest.mark.django_db
def test_transport_views_xlsx(client: Client, accept: str, url: str, fields: Dict[str, type]):
    response = client.get(url, HTTP_ACCEPT=accept)
    assert response.status_code == 200
    assert response.content
    if accept == 'application/json' or accept == 'application/yaml':
        if accept == 'application/json':
            data = response.json()[0]
        else:
            data = yaml.load(response.content)[0]
        assert fields.keys() == data.keys()

        for field, field_type in fields.items():
            assert type(data[field]) == field_type
    elif accept == 'application/xlsx':
        df = pandas.read_excel(io.BytesIO(response.content))
        assert set(df.columns.values) == fields.keys()
    elif accept == 'application/xml':
        root = ET.fromstring(response.content)
        assert {field.tag for field in root[0]} == fields.keys()
        
    elif accept == 'text/csv':
        reader = csv.DictReader(io.StringIO(response.content.decode()))
        assert set(reader.fieldnames) == fields.keys()
    elif accept == 'text/html':
        assert response.content 
    else:
        raise NotImplementedError

