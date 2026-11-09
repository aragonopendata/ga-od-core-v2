"""Configuration module."""

import os
from abc import ABCMeta
from typing import Optional, Dict, List, Union
from urllib.parse import urljoin
from sys import platform
import yaml
from pydantic import BaseModel, Field

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


class HealthAlertsConfig(BaseModel):
    enabled: bool = True
    failure_threshold_minutes: int = 10
    recovery_notification: bool = True
    consecutive_failures_threshold: int = 3


class HealthMonitoringConfig(BaseModel):
    enabled: bool = True
    check_interval_minutes: int = 5
    timeout_seconds: int = 30
    network_timeout_seconds: int = 3
    connector_dependency_window_minutes: int = 5
    concurrency_limit: int = 5
    retention_days: int = 30
    alerts: HealthAlertsConfig = HealthAlertsConfig()


class DownloadLimitsConfig(BaseModel):
    """Limits for large downloads (``common_config.download_limits``).

    A download is large when its resource had at least ``heavy_rows`` rows in its last whole download (without
    filters, like, fields, limit or offset) or is listed in ``heavy_resource_ids``, and the request has no ``limit``
    or one of at least ``heavy_rows``. Only large downloads
    are limited, with PostgreSQL advisory locks of the configuration database.
    """

    enabled: bool = True
    heavy_rows: int = Field(default=100000, ge=1)
    # Slots per resource are numbered inside 4 bits of the lock key, hence the upper bound.
    max_per_resource: int = Field(default=2, ge=1, le=16)
    max_total: int = Field(default=4, ge=1)
    retry_after: int = Field(default=120, ge=1)
    heavy_resource_ids: List[int] = []


class CommonConfig(BaseModel):
    allowed_hosts: List[str]
    csrf_trusted_origins: Optional[List[str]] = []
    secret_key: str
    debug: bool
    databases: Dict[str, Database]
    cache_ttl: int
    health_monitoring: HealthMonitoringConfig = HealthMonitoringConfig()
    download_timing_log: bool = True
    oracle_arraysize: int = Field(default=5000, ge=1)
    conn_max_age: int = Field(default=60, ge=0)
    download_streaming: bool = True
    download_block_rows: int = Field(default=1000, ge=1)
    download_limits: DownloadLimitsConfig = DownloadLimitsConfig()


class Config(BaseModel):
    common_config: CommonConfig
    projects: Projects

    @classmethod
    def get_config(cls) -> 'Config':
        with open(_CONFIG_PATH, 'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        return Config.parse_obj(data)
