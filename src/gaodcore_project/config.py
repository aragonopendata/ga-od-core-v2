import os
from abc import ABCMeta
from typing import Optional, Dict
from urllib.parse import urljoin

import yaml
from pydantic import BaseModel
from sys import platform

try:
    _CONFIG_PATH = os.environ['CONFIG_PATH']
except KeyError:
    if platform == "linux" or platform == "linux2":
        _CONFIG_PATH = '/etc/gaodcore/config-tst.yaml'
    elif platform == "win32":
        _CONFIG_PATH = 'C:\\GA-OD-CORE-V2\\config-tst.yaml'
    else:
        raise NotImplementedError()


class GetURL(metaclass=ABCMeta):
    # @property
    # def base_url(self) -> str:
    #     pass
    #
    # @property
    # def endpoints(self) -> object:
    #     pass

    def get_url(self, endpoint: str, **kwargs) -> str:
        return urljoin(self.base_url, getattr(self.endpoints, endpoint)).format(**kwargs)


class AragonTransportEndpoints(BaseModel):
    vehicles: str
    drivers: str
    live_position_latest: str
    vehicle_journey_history_latest: str
    distance_travelled: str


class AragonTransport(BaseModel, GetURL):
    user: str
    password: str
    customer_id: int
    base_url: str
    endpoints: AragonTransportEndpoints


class ZaragozaTransportEndpoints(BaseModel):
    lines: str
    stops: str
    routes: str
    stops_route: str
    arrival_time: str
    notices: str
    origins: str
    destinations: str
    lines_ori_des: str
    times_route: str
    exp_ori_des: str
    stops_ori_des: str
    sae: str
    arrival_ori_des: str


class ZaragozaTransport(BaseModel, GetURL):
    base_url: str
    max_concurrency: int
    endpoints: ZaragozaTransportEndpoints

# Mikel

class GoogleAnalyticsEndpoints(BaseModel):
    analytics: str

'''
class GoogleAnalytics(BaseModel, GetURL):
    start_date: str
    end_date: str
    metrics: str
    dimensions: str
    filters: str 
    include_empty_rows: bool 
    max_results: int  
    output: str 
    samplingLevel: str 
    segment: str 
    sort: str 
    start_index: int 
    fields: str #Selector specifying which fields to include in a partial response.
    endpoints: GoogleAnalyticsEndpoints
'''
class Analytics(BaseModel, GetURL):
    base_url: str
    app_path: str
    key_file: str
    service_account: str

class Transport(BaseModel):
    aragon: AragonTransport
    zaragoza: ZaragozaTransport


class Projects(BaseModel):
    transport: Transport
    google_analytics: Analytics


class Database(BaseModel):
    ENGINE: str
    NAME: str
    USER: Optional[str] = None
    PASSWORD: Optional[str] = None
    HOST: Optional[str] = None
    PORT: Optional[str] = None


class CommonConfig(BaseModel):
    secret_key: str
    debug: bool
    databases: Dict[str, Database]
    cache_ttl: int


class Config(BaseModel):
    common_config: CommonConfig
    projects: Projects

    @classmethod
    def get_config(cls) -> 'Config':
        with open(_CONFIG_PATH, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return Config.parse_obj(data)
