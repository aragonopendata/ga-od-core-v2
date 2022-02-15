"""Module that deal with external resources."""

import csv
import json
import logging
from operator import contains
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from http import HTTPStatus
from io import StringIO
from sqlite3 import Date
from typing import Optional, Dict, List, Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

import cchardet
import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData, Column, Boolean, Text, Integer, DateTime, Time, REAL
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

_DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite'}
_HTTP_SCHEMAS = {'http', 'https'}
_RESOURCE_MAX_ROWS = 1048576
_TEMPORAL_TABLE_NAME = 'temporal_table'
_SQLALCHEMY_MAP_TYPE = {bool: Boolean, str: Text, int: Integer, float: REAL, date: Date, datetime: DateTime, time: Time}
_RESOURCE_MAX_ROWS_EXCEL = 1048576

class MimeType(Enum):
    """Enum with some mimetype and his different values."""
    CSV = ('text/csv', )
    XLSX = (
        'application/xlsx',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
    JSON = ('application/json', )


class NotImplementedSchemaError(Exception):
    """Schema of the URI is not implemented. Schema examples: http, https, postgresql, ftp, etc."""


class DriverConnectionError(Exception):
    """Connection cannot be established."""


class NoObjectError(Exception):
    """Object is not available. Connection was successfully done but object is not available."""

class TooManyRowsErrorExcel(Exception):
    """Object Excel have too many rows to process. Excel not allow more rows."""

class TooManyRowsError(Exception):
    """Object have too many rows to process. Excel not allow more rows."""


class FieldNoExistsError(Exception):
    """Resource does not have a field."""


class SortFieldNoExistsError(Exception):
    """Resource does not have a field."""


class MimeTypeError(Exception):
    """Type of document is not csv or excel."""


@dataclass
class OrderBy:
    """Defines field and is ascending sort. This is used to generate SQL queries."""
    field: str
    ascending: bool


def _get_model(*, engine: Engine, object_location: str, object_location_schema: str) -> Table:
    # Note: if object_location is None meaning that original data is not in a database so is writen in a temporarily
    #  table.
    object_location = object_location or _TEMPORAL_TABLE_NAME
    meta_data = MetaData(bind=engine)
    try:
        return Table(object_location, meta_data, autoload=True, autoload_with=engine, schema=object_location_schema)
    except sqlalchemy.exc.NoSuchTableError as err:
        logging.exception("Object not available.")
        raise NoObjectError("Object not available.") from err
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError, sqlalchemy.exc.ProgrammingError) as err:
        logging.exception("Connection not available.")
        raise DriverConnectionError("Connection not available.") from err


def get_resource_columns(uri: str, object_location: Optional[str],
                         object_location_schema: Optional[str]) -> Iterable[Dict[str, str]]:
    """From resource get a list of dictionaries with column name and data type.

    @param cache: if not exists connection save on cache.
    @return: list of dictionaries with column name and data type
    """
    engine = _get_engine(uri)
    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    data = ({"COLUMN_NAME": column.description, "DATA_TYPE": str(column.type)} for column in model.columns)
    engine.dispose()
    return data


def validate_resource(*, uri: str, object_location: Optional[str],
                      object_location_schema: Optional[str]) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available . Return data of resource, a iterable of
    dictionaries."""
    _validate_max_rows_allowed(uri, object_location, object_location_schema=object_location_schema)
    return get_resource_data(uri=uri,
                             object_location=object_location,
                             object_location_schema=object_location_schema,
                             filters={},
                             fields=[],
                             sort=[])

def validator_max_excel_allowed(uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0):
    """Validate if resource  have less rows than allowed."""
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    column_dict = {column.name: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    session = session_maker()
    data = session.query(model).filter_by(**filters).order_by(*_get_sort_methods(column_dict, sort)).with_entities(
        *[model.c[col.name].label(col.name) for col in model.columns]).offset(offset).limit(limit).all()
    session.close()
    engine.dispose()
    
    return len(data) <= _RESOURCE_MAX_ROWS_EXCEL


def _validate_max_rows_allowed(uri: str, object_location: Optional[str], object_location_schema: Optional[str]):
    """Validate if resource is aviable."""
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    session = session_maker()
    try:
        num_rows = session.query(*[model.c[col.name].label(col.name) for col in model.columns]).count()
    except sqlalchemy.exc.ProgrammingError as err:
        logging.exception("Object not available.")
        raise NoObjectError("Object not available.") from err

    session.close()
    engine.dispose()


def get_resource_data(*,
                      uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0) -> Iterable[Dict[str, Any]]:
    """Return a iterable of dictionaries with data of resource."""
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    column_dict = {column.name: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    
    session = session_maker()
    
    filters_args= _get_filter_by_args(filters, model)
    print(*filters_args)
   
    data = session.query(model).filter(*filters_args).order_by(*_get_sort_methods(column_dict, sort)).with_entities(
    *[model.c[col.name].label(col.name) for col in model.columns]).offset(offset).limit(limit).all()


        # data = session.query(model).filter_by(**filters).filter(model.columns.nombre.like("%2%")).order_by(*_get_sort_methods(column_dict, sort)).with_entities(
    #  *[model.c[col.name].label(col.name) for col in model.columns]).offset(offset).limit(limit).all()



    
    # FIXME:
    #  check https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    
    session.close()
    engine.dispose()

    return (dict(zip([column.name for column in columns], row)) for row in data)


def _get_columns(columns_dict: Dict[str, Column], column_names: List[str]) -> Iterable[Column]:
    """Get SQLAlchemy column instances from column names."""
    if column_names:
        try:
            return [columns_dict[column_name] for column_name in column_names]
        except KeyError as err:
            raise FieldNoExistsError(f'Field: {err.args[0]} not exists.') from err
    else:
        return columns_dict.values()


def _get_filter_by_args(dict_args: dict, model_class: Table):
      
    filters = []
    for key, value in dict_args.items():  # type: str, any
        if key.endswith(' like'):
            key = key[:-5]
            filters.append(str(model_class) +"." + str(key) +" Like '*" + str(value) +"*'" )
        elif key.endswith('___max'):
            key = key[:-6]
            item = str(model_class) +"."+ str(key) +" >> " + str(value)
            filters.append(item)
            
        elif key.endswith('__min'):
            key = key[:-5]
            filters.append(str(model_class) +"." + str(key) +" << " + str(value)  )
        elif key.endswith('__max'):
            key = key[:-5]
            filters.append(getattr(model_class, key) <= value)
        else:
            filters.append(str(model_class) +"." + str(key) +" == " + str(value)  )
        
    return filters



def _get_sort_methods(column_dict: Dict[str, Column], sort: List[OrderBy]):
    """Create a list of SQLAlchemy column instances that represent query sorting."""
    sort_methods = []
    for item in sort:
        try:
            column = column_dict[item.field]
        except KeyError as err:
            raise SortFieldNoExistsError(f'Sort field: {err.args[0]} not exists.') from err
        if item.ascending:
            sort_methods.append(column)
        else:
            sort_methods.append(column.desc())
    print (sort_methods)
    return sort_methods


def validate_uri(uri: str) -> None:
    """Validate if URI is available as resource."""
    engine = _get_engine(uri)
    try:
        with engine.connect() as _:
            pass
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError, sqlalchemy.exc.ProgrammingError) as err:
        logging.exception("Connection not available.")
        raise DriverConnectionError("Connection not available.") from err


def _csv_to_dict(data: bytes, charset: str) -> List[Dict[str, Any]]:
    if not charset:
        charset = cchardet.detect(data)['encoding']

    data = data.decode(charset)
    dialect = csv.Sniffer().sniff(data)
    return list(csv.DictReader(StringIO(data), dialect=dialect))


def _get_table_from_dict(data: List[Dict[str, Any]], engine: Engine, meta_data: MetaData) -> Table:
    fields_to_check = list(data[0].keys())
    fields_checked = []

    field_types = OrderedDict([(field, Column(field, Text)) for field in fields_to_check])
    for row in data:
        for field_to_check in fields_to_check:
            value = row.get(field_to_check)
            if value and field_to_check not in fields_checked:
                field_types[field_to_check] = Column(field_to_check, _SQLALCHEMY_MAP_TYPE[type(value)])
                fields_checked.append(field_to_check)
        for field_checked in fields_checked:
            fields_to_check.remove(field_checked)
        fields_checked = []
        if not fields_to_check:
            break

    table = Table(_TEMPORAL_TABLE_NAME, meta_data, *field_types.values(), prefixes=['TEMPORARY'])
    meta_data.create_all(engine)
    return table


def _get_engine_from_api(uri: str) -> Engine:
    try:
        with urllib.request.urlopen(uri) as response:
            if response.getcode() == HTTPStatus.OK:
                split_content_type = response.info()['Content-Type'].split(';')
                mime_type = split_content_type[0]
                if len(split_content_type) > 1:
                    charset = split_content_type[1].split('=')[1]
                else:
                    charset = None

                if mime_type in MimeType.CSV.value:
                    data = _csv_to_dict(response.read(), charset)
                elif mime_type in MimeType.JSON.value:
                    data = json.loads(response.read())
                else:
                    raise MimeTypeError()
            else:
                raise DriverConnectionError('The url could not be reached.')
    except (HTTPError, URLError) as err:
        raise DriverConnectionError('The url could not be reached.') from err
    max_key = max(data, key=len).keys()
    for item in data:
        if len(max_key) > len(item.keys()):
            for k in max_key:
                item.setdefault(k, None)
    engine = create_engine("sqlite:///:memory:", echo=True, future=True)
    metadata = MetaData()
    table = _get_table_from_dict(data, engine, metadata)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    session.execute(table.insert(), data)
    session.commit()
    session.close()
    return engine


def _get_engine(uri: str) -> Engine:
    uri_parsed = urlparse(uri)
    if uri_parsed.scheme in _DATABASE_SCHEMAS:
        return create_engine(uri, max_identifier_length=128)
    if uri_parsed.scheme in _HTTP_SCHEMAS:
        return _get_engine_from_api(uri)
    raise NotImplementedSchemaError(f'Schema: "{uri_parsed.scheme}" is not implemented.')
