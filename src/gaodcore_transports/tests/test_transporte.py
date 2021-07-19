import csv
import io
from typing import Dict

import pandas
import pytest
import yaml
from django.test import Client


@pytest.mark.parametrize("url, fields", [
    ["/GA_OD_Core/gaodcore-transports/aragon/vehicles",
     {'id': str, 'registration': str, 'name': str, 'groupId': str, 'groupName': str, 'odometerValue': float,
      'odometerType': str, 'featureTags_0': str, 'defaultDriverId': str, 'status': str, 'idlingFidelity': int,
      'engineTotalHours': float}],
    # ["/GA_OD_Core/gaodcore-transports/aragon/drivers", {}],
    # ["/GA_OD_Core/gaodcore-transports/aragon/livetion-latest.xlsx", {}],
    # ["/GA_OD_Core/gaodcore-transports/aragon/vehicleney-history-latest.xlsx", {}],
    # ["/GA_OD_Core/gaodcore-transports/aragon/distanceelled.xlsx", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/lines", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/stops", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/routes", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/stops_route", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/arrival_time", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/notices", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/origins", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/destinations", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/lines_ori_des", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/times_route", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/exp_ori_des", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/stops_ori_des", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/arrival_ori_des", {}],
    # ["/GA_OD_Core/gaodcore-transports/zaragoza/sae", {}],
])
@pytest.mark.parametrize("accept,", ['text/html', 'application/json', 'text/csv', 'application/xlsx', 'application/xml'])
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
        pass  # TODO
    elif accept == 'text/csv':
        reader = csv.DictReader(io.StringIO(response.content.decode()))
        assert set(reader.fieldnames) == fields.keys()
    elif accept == 'text/html':
        pass
    else:
        raise NotImplementedError


