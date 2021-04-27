
from typing import List

from django.shortcuts import render

from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnList

from api_reader.serializers import DictSerializer

import pandas as pd
import numpy as np
import requests
import json
import yaml
from datetime import datetime, timedelta

def get_return_list(data: List[dict]) -> ReturnList:
    return_list = ReturnList(serializer=DictSerializer)
    parsed_data = json.loads(json.dumps(data, cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)
    return return_list

def get_config(campo1,campo2):
    with open(r'./api_reader/config.yaml') as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)
        return conf[campo1][campo2]

def get_vehicles():
    url = get_config('transporte_aragon','host_aragon') + get_config('transporte_aragon','api_base_url') + get_config('transporte_aragon','api_list_vehicles')
    r = requests.get(url.format(customerId=get_config('transporte_aragon','customerId')), 
                                    auth=(get_config('transporte_aragon','usuario'), get_config('transporte_aragon','contr')))
    return r

# Create your views here.
class ListVehicleView(APIView):
    def get(self, request: Request, format=None):    
        data = pd.json_normalize(get_vehicles().json()['items']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class ListDriverView(APIView):
    def get(self, request: Request, format=None):
        url = get_config('transporte_aragon','host_aragon') + get_config('transporte_aragon','api_base_url') + get_config('transporte_aragon','api_list_driver')
        r = requests.get(url.format(customerId=get_config('transporte_aragon','customerId')), 
                                    auth=(get_config('transporte_aragon','usuario'), get_config('transporte_aragon','contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class LivePositionLatestView(APIView):
    def get(self, request: Request, format=None):     
        url = get_config('transporte_aragon','host_aragon') + get_config('transporte_aragon','api_base_url') + get_config('transporte_aragon','api_live_position_latest')
        r = requests.get(url.format(customerId=get_config('transporte_aragon','customerId')), 
                                    auth=(get_config('transporte_aragon','usuario'), get_config('transporte_aragon','contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class VehicleJourneyHistoryLatestView(APIView):

    #TODO: Tarda alrededor de 1.5 min en generar los resultados

    def get(self, request: Request, format=None):
        fromDateTime = (datetime.now()- timedelta(days=13)).strftime('%Y-%m-%dT00:00:00')

        df = pd.json_normalize(get_vehicles().json()['items'])
        list_vehicleIds = list(df['id'])

        df = []
        for vehicleIds in list_vehicleIds:
            url = get_config('transporte_aragon','host_aragon') + get_config('transporte_aragon','api_base_url') + get_config('transporte_aragon','api_vehicle_journey_history_latest')
            r = requests.get(url.format(customerId=get_config('transporte_aragon','customerId'),
                                        fromDateTime=fromDateTime,
                                        vehicleIds=vehicleIds), 
                            auth=(get_config('transporte_aragon','usuario'), get_config('transporte_aragon','contr')))
            tmp = pd.json_normalize(r.json()['items'])
            tmp['vehicleIds'] = vehicleIds
            df.append(tmp)
        
        data = pd.concat(df).reset_index(drop=True).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class DistanceTravelledView(APIView):

    #TODO: Tarda alrededor de 1.5 min en generar los resultados

    def get(self, request: Request, format=None):

        startDate = '2015-06-01T22:00:00'
        endDate = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        df = pd.json_normalize(get_vehicles().json()['items'])
        list_vehicleIds = list(df['id'])

        df = []
        for vehicleIds in list_vehicleIds:
            url = get_config('transporte_aragon','host_aragon') + get_config('transporte_aragon','api_base_url') + get_config('transporte_aragon','api_distance_travelled')
            r = requests.get(url.format(customerId = get_config('transporte_aragon','customerId'),
                                        startDate = startDate,
                                        endDate = endDate,
                                        vehicleIds = vehicleIds), 
                            auth=(get_config('transporte_aragon','usuario'), get_config('transporte_aragon','contr')))
            tmp = pd.json_normalize(r.json())
            tmp['vehicleIds'] = vehicleIds
            df.append(tmp)
        
        data = pd.concat(df).reset_index(drop=True).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser