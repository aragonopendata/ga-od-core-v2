"""Configuration module."""

import os
from abc import ABCMeta
from typing import Optional, Dict, List, Union
from urllib.parse import urljoin
from sys import platform
import yaml
from pydantic import BaseModel

try:
    _CONFIG_PATH = os.environ['CONFIG_PATH']
except KeyError as err:
    if platform == "win32":
        _CONFIG_PATH = 'C:\\ga-od-core-v2\\config-tst.yaml'
    elif platform in ("linux", "linux2"):
        _CONFIG_PATH = '/etc/gaodcore/config-tst.yaml'
    else:
        raise NotImplementedError("System not supported") from err


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


class Transport(BaseModel):
    aragon: AragonTransport
    zaragoza: ZaragozaTransport


class Projects(BaseModel):
    transport: Transport


class Database(BaseModel):
    ENGINE: str
    NAME: str
    USER: Optional[str] = None
    PASSWORD: Optional[str] = None
    HOST: Optional[str] = None
    PORT: Optional[Union[str, int]] = None


class CommonConfig(BaseModel):
    allowed_hosts: List[str]
    secret_key: str
    debug: bool
    databases: Dict[str, Database]
    cache_ttl: int


class Config(BaseModel):
    common_config: CommonConfig
    projects: Projects

    @classmethod
    def get_config(cls) -> 'Config':
        with open(_CONFIG_PATH, 'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        return Config.parse_obj(data)
