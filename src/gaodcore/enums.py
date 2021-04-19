from enum import Enum


class Mimetype(Enum):
    CSV = 'text/csv'
    TEXT = 'text/plain'
    JSON = 'application/json'
    XML = 'application/xml'
    XLS = 'application/vnd.ms-excel'
    XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'