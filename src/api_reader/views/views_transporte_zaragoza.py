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

def get_lines():
    url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_lines')
    r = requests.get(url)
    return r

def get_stops():
    url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_stops')
    r = requests.get(url)
    return r

def get_origins():
    url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_origins')
    r = requests.get(url)
    return r

def get_oridesdf():
    df_origins = pd.json_normalize(get_origins().json()['origins'])
    list_origins_id = list(df_origins['id'])
    url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_destinations')
    df = []
    for origins_id in list_origins_id:
        try: 
            r = requests.get(url.format(id = origins_id))
            tmp = pd.json_normalize(r.json()['destinations'])
            tmp['origins_id'] = origins_id
            df.append(tmp)
        except : pass
    return pd.concat(df)

def get_routesdf():
    df_lines = pd.json_normalize(get_lines().json()['lines'])
    list_id_linea = list(df_lines['id'])
    url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_routes')
    df = []
    for id_linea in list_id_linea:
        try: 
            r = requests.get(url.format(id=id_linea))
            tmp = pd.json_normalize(r.json()['routes'])
            tmp['id_linea'] = id_linea
            df.append(tmp)
        except : pass
    return pd.concat(df)

# Create your views here.
class LineasView(APIView):
    def get(self, request: Request, format=None):
        data = pd.json_normalize(get_lines().json()['lines']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class LineasStopsView(APIView):
    def get(self, request: Request, format=None):
        data = pd.json_normalize(get_stops().json()['stops']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class RoutesView(APIView):
    def get(self, request: Request, format=None):
        
        #TODO: Tarda en generar los resultados

        data = get_routesdf().replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class StopsRouteView(APIView):
    def get(self, request: Request, format=None):
        df_unicos = get_routesdf().drop_duplicates(subset=['isreturn','route','id_linea']).drop('depttime',axis=1).reset_index(drop=True)
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_stops_route')
        df = []
        for index, row in df_unicos.iterrows():
            try: 
                r = requests.get(url.format(id_linea = str(row['id_linea']),    
                                            id_ruta = str(row['route']), 
                                            sentido = str(row['isreturn'])))
                tmp = pd.json_normalize(r.json()['stops_route'])
                tmp['id_linea']=row['id_linea']
                tmp['route']=row['route']
                tmp['isreturn']=row['isreturn']
                df.append(tmp)
            except:pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class ArrivalTimeView(APIView):
    def get(self, request: Request, format=None):
        
        #TODO: Tarda en generar los resultados

        df_stops = pd.json_normalize(get_stops().json()['stops'])
        list_id_parada = list(df_stops['stop_id'])
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_arrival_time')
        df = []
        for id_parada in list_id_parada:
            try: 
                r = requests.get(url.format(code_parada = id_parada))
                tmp = pd.json_normalize(r.json()['arrival_time'])
                tmp['id_parada'] = id_parada
                df.append(tmp)
            except : pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))
        
    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class NoticesView(APIView):
    def get(self, request: Request, format=None):     
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_notices')
        r = requests.get(url)
        data = pd.json_normalize(r.json()['notices']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class OriginsView(APIView):
    def get(self, request: Request, format=None):     
        data = pd.json_normalize(get_origins().json()['origins']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class DestinationsView(APIView):
    def get(self, request: Request, format=None):     
        
        #TODO: Tarda en generar los resultados

        data = get_oridesdf().replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class LinesOriDesView(APIView):
    def get(self, request: Request, format=None):
        
        #TODO: Tarda en generar los resultados

        df_unicos = get_oridesdf().drop_duplicates(subset=['id','origins_id']).reset_index(drop=True)
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_lines_ori_des')
        df = []
        for index, row in df_unicos.iterrows():
            try: 
                r = requests.get(url.format(Origen = row['origins_id'], 
                                            Destino = row['id']))
                tmp = pd.json_normalize(r.json()['lines_ori_des'])
                tmp['id_origen']=row['origins_id']
                tmp['id_destino']=row['id']
                df.append(tmp)
            except:pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class TimesRouteView(APIView):
    def get(self, request: Request, format=None):

        #TODO: Tarda en generar los resultados
        #TODO: Realizar la consulta para todas las expediciones posibles (ExpOriDesView)

        df_lines = pd.json_normalize(get_lines().json()['lines'])
        list_id_linea = list(df_lines['id'])
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_times_route')
        bus = 0 #Si no se conoce el bus, se utiliza un valor 0.
        departure_time = 43200 #introducir el tiempo de salida (12:00) - Habría que hacerlo con la consulta ExpOriDesView
        df = []
        for id_linea in list_id_linea:
            for direction in [0,1]:
                try: 
                    r = requests.get(url.format(id_linea = id_linea,                bus = bus, 
                                                departure_time = departure_time,    direction = direction))
                    tmp = pd.json_normalize(r.json()['times_route'])
                    tmp['id_linea']= id_linea
                    tmp['direction']= direction
                    tmp['bus'] = bus
                    tmp['departure_time'] = departure_time
                    df.append(tmp)
                except : pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class ExpOriDesView(APIView):
    def get(self, request: Request, format=None):

        #TODO: Tarda en generar los resultados

        df_unicos = get_oridesdf().drop_duplicates(subset=['id','origins_id']).reset_index(drop=True)
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_exp_ori_des')
        df = []
        for index, row in df_unicos.iterrows():
            try: 
                r = requests.get(url.format(Origen = row['origins_id'], 
                                            Destino = row['id']))
                tmp = pd.json_normalize(r.json()['exp_ori_des'].values())
                tmp['id_origen']=row['origins_id']
                tmp['id_destino']=row['id']
                df.append(tmp)
            except:pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class StopsOriDesView(APIView):
    def get(self, request: Request, format=None):

        #TODO: Tarda en generar los resultados

        df_unicos = get_oridesdf().drop_duplicates(subset=['id','origins_id']).reset_index(drop=True)
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_stops_ori_des')
        df = []
        for index, row in df_unicos.iterrows():
            try: 
                r = requests.get(url.format(Origen = row['origins_id'], 
                                            Destino = row['id']))
                tmp = pd.json_normalize(r.json()['stops_ori_des'])
                tmp['id_origen']=row['origins_id']
                tmp['id_destino']=row['id']
                df.append(tmp)
            except:pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class ArrivalOriDesView(APIView):
    def get(self, request: Request, format=None):

        #TODO: Tarda en generar los resultados

        df_unicos = get_oridesdf().drop_duplicates(subset=['id','origins_id']).reset_index(drop=True)
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_arrival_ori_des')
        df = []
        for index, row in df_unicos.iterrows():
            try: 
                r = requests.get(url.format(Origen = row['origins_id'], 
                                            Destino = row['id']))
                tmp = pd.json_normalize(r.json()['arrival_ori_des'])
                tmp['id_origen']=row['origins_id']
                tmp['id_destino']=row['id']
                df.append(tmp)
            except:pass
        data = pd.concat(df).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser

class SAEView(APIView):
    def get(self, request: Request, format=None):     
        url = get_config('transporte_zaragoza','host_zaragoza') + get_config('transporte_zaragoza','api_base_url') + get_config('transporte_zaragoza','api_sae')
        r = requests.get(url)
        data = pd.json_normalize(r.json()['sae']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser   