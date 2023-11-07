"""Module that deal with external resources."""

import csv
import json
import logging
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
from rest_framework.exceptions import ValidationError
from pydoc import text
import cchardet
import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData, Column, Boolean, Text, Integer, DateTime, Time, REAL
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import class_mapper
from sqlalchemy.types import TypeDecorator, Numeric, Float
from sqlalchemy.dialects import postgresql
from urllib.parse import urlparse
from geoalchemy2 import Geometry,  elements, functions as GeoFunc
from shapely_geojson import dumps, Feature, FeatureCollection
from geoalchemy2.shape import to_shape 
from sqlalchemy import func, select
import datetime
import decimal
import json
import uuid
import re
from pydoc import text
from django.utils.duration import duration_iso_string
from django.utils.functional import Promise
from django.utils.timezone import is_aware


from django.utils.functional import Promise
import math

_DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql+pyodbc', 'oracle', 'sqlite'}
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
                             like="",
                             fields=[],
                             sort=[])

def validate_resource_mssql(*, uri: str, object_location: Optional[str],
                      object_location_schema: Optional[str]) -> Iterable[Dict[str, Any]]:

     """Validate if resource is available . Return data of resource, a iterable of
    dictionaries."""
       
     
     fields=[]
     sort=[]
     
     engine = _get_engine(uri)
     session_maker = sessionmaker(bind=engine)

     model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

     column_dict = {column.name: column for column in model.columns}
     columns = _get_columns(column_dict, fields)
     session = session_maker()
    
     data = session.query(model).order_by(*_get_sort_methods(column_dict, sort)).with_entities(
        *[model.c[col.name].label(col.name) for col in model.columns]).all()
    
     session.close()
     engine.dispose()

     return (dict(zip([column.name for column in columns], row)) for row in data)


def validator_max_excel_allowed(uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      like: str,
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0):
    """Validate if resource  have less rows than allowed."""
   
    data = get_session_data(uri, object_location, object_location_schema, filters, like, fields, sort, limit, offset)
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

#Add feature to sanitize text include control characters
def sanitize_control_charcters(text):
    
    if re.search(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\n]', str(text)):
        text=  re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\n]', "", text)
    try:
        text =  text.decode('utf-8')
    except:
        pass
    return text
def get_GeoJson_resource(uri: str, object_location: Optional[str],
                         object_location_schema: Optional[str]) -> Boolean:

    """From resource return if is GeoJson resource 

    @param cache: if not exists connection save on cache.
    @return: True or False GeoJosn Resource
    """
    engine = _get_engine(uri)
    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)
    geoJson =False
    
    for column in model.columns:
         if str(column.type).startswith("geometry") or str(column.type).startswith("geography") :
            geoJson = True

    engine.dispose()     
    return(geoJson)

def get_resource_data_feature( uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      like: str,
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0):
    """data like GeoJSON .Encoding data a variety of geographic data structures."""
    """Data like Feature_Collection_"""

    """Not posible to implement GeoFunc.ST_AsGeoJSON(rows) with model, postgis version  is < 3.0 """

    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)
    column_dict = {column.name: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    filters_args = _get_filter_by_args(like, model)
    session = session_maker()
    
    parsed = urlparse(uri)
        
    #Get Column Geom - other fields in Properties

    propertiesCol=[]
    propertiesField=[]
    
    for i, col in enumerate(model.columns):
        if str(col.type).startswith("geography") or str(col.type).startswith("geometry"):
            Geom = model.c[col.name].label(col.name) 
        else:
            propertiesField.append(col.name)
            propertiesCol.append(model.c[col.name].label(col.name))
    
    
    #Get A JSon Properties ( not possible json_build_object PostgreSQL 9.2.24 on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-16), 64-bit
    # version of Postgress not allowed _ Postgres and A GeoJson  only _ https://www.postgresql.org/docs/9.2/functions-json.html" 
    # https://www.postgresql.org/docs/current/functions-json.html -> to convert columns to a json in a query 
    if parsed.scheme in ['mssql+pyodbc']:  
        data = session.query(*propertiesCol, (GeoFunc.ST_AsGeoJSON(Geom)).label("geometry")).filter_by(**filters).filter(*filters_args).all()
    else:
         data = session.query(*propertiesCol, (GeoFunc.ST_AsGeoJSON(Geom)).label("geometry")).filter_by(**filters).filter(*filters_args).order_by(*_get_sort_methods(column_dict, sort)). offset(offset).limit(limit).all()

    columnsProperties = _get_columns(column_dict, propertiesField)
  
    #Serializar Feature Collection
    featuresTot = []    
    re_decimal = "\.0*$"  # allow e.g. '1.0000000000' as an int, but not '1.2
    for item in data:
        item = list(item)
        geometry= item.pop()
        print(item)
        for i, column in enumerate(item):
              if isinstance(column, (decimal.Decimal, uuid.UUID, Promise)):
                  parte_decimal, parte_entera = math.modf(column)
                  if (parte_decimal) == 0.0:
                      item[i] = int(column) 
                  else:
                      item[i] = str(column) 
              elif isinstance(column, float):
                 parte_decimal, parte_entera = math.modf(column)
                 if (parte_decimal) == 0.0:
                      item[i] = int(column)  
                 else:
                      item[i] = str(column)
              elif isinstance(column, Numeric):
                 parte_decimal, parte_entera = math.modf(column)
                 if (parte_decimal) == 0.0:
                      item[i] = int(column) 
                 else:
                      item[i] = str(column)
              elif isinstance(column, datetime.datetime):
                    r = column.isoformat()
                    if column.microsecond:
                        r = r[:23] + r[26:]
                    if r.endswith('+00:00'):
                        r = r[:-6] + 'Z'
                    item[i] = r
              elif isinstance(column, datetime.date):
                    item[i] =  column.isoformat()
              elif isinstance(column, datetime.time):
                    r = column.isoformat()
                    if column.microsecond:
                        r = r[:12]
                    item[i] = r
              elif isinstance(column, datetime.timedelta):
                    item[i] =  duration_iso_string(column)
               
              else:
                         item[i] = sanitize_control_charcters(column)
         
        properties = dict(zip([column.name for column in  columnsProperties],  item))
        

        #create feature
        featureType = {
            'type': 'Feature',
            'geometry': json.loads(geometry),
            'properties': properties}
        featuresTot.append(featureType)
    
    wrapped = { "type":"FeatureCollection", "features": featuresTot }
    
    session.close()
    engine.dispose()
       
    return(wrapped)
    

def get_session_data( uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      like: str,
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0):
   
    """sqlalchemy.exc.CompileError: MSSQL requires an order_by when using an OFFSET or a non-simple LIMIT clause """
    """(pyodbc.ProgrammingError) ('42000', '[42000] [FreeTDS][SQL Server]The text, ntext, and image data types cannot be compared or sorted, except when using IS NULL or LIKE operator. (306) (SQLExecDirectW)')"""
    """ mssql no order no limit no offset"""
   
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    column_dict = {column.name: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    filters_args = _get_filter_by_args(like, model)
    
    session = session_maker()
    
    parsed = urlparse(uri)
    
    if parsed.scheme in ['mssql+pyodbc']:  
        data = session.query(model).filter_by(**filters).filter(*filters_args).with_entities(
              *[model.c[col.name].label(col.name) for col in columns]).all()
    else:
         data = session.query(model).filter_by(**filters).filter(*filters_args).order_by(*_get_sort_methods(column_dict, sort)).with_entities(
              *[model.c[col.name].label(col.name) for col in columns]).offset(offset).limit(limit).all()
    
    session.close()
    engine.dispose()
    
    return(data)


def get_resource_data(*,
                      uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      like : str,
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0) -> Iterable[Dict[str, Any]]:
    """Return a iterable of dictionaries with data of resource."""
    
    re_decimal = "\.0\s*$"  # allow e.g. '1.0' as an int, but not '1.2
    data = get_session_data(uri, object_location, object_location_schema, filters, like, fields, sort, limit, offset)
    
    """" When no typing objects are present, as when executing plain SQL strings, adefault "outputtypehandler" is present which will generally return numeric
    values which specify precision and scale as Python ``Decimal`` objects default "outputtypehandler" is present which will generally return numeric
    values which specify precision and scale as Python ``Decimal`` objects.  To disable this coercion to decimal for performance reasons, pass the flag
    ``coerce_to_decimal=False`` to :func:`_sa.create_engine`:: engine = create_engine("oracle+cx_oracle://dsn", coerce_to_decimal=False)
    The ``coerce_to_decimal`` flag only impacts the results of plain string SQL statements that are not otherwise associated with a :class:`.Numeric`
    SQLAlchemy type (or a subclass of such).
    .. versionchanged:: 1.2  The numeric handling system for cx_Oracle has been reworked to take advantage of newer cx_Oracle features as well 
    as better integration of outputtypehandlers. """


    dataTemp = []
    dataTempTuplas= []
  
    for item in data:
        for column in item:
            if (isinstance(column, (decimal.Decimal, uuid.UUID, Promise))) or (isinstance(column, float)) or (isinstance(column, Numeric)):
                    parte_decimal, parte_entera = math.modf(column)
                    if (parte_decimal) == 0.0:
                        dataTempTuplas.append(int(column))
                    else:
                        dataTempTuplas.append(sanitize_control_charcters(column))
                    


            else: 
                    dataTempTuplas.append(sanitize_control_charcters(column))
                    
        dataTemp.append(tuple(dataTempTuplas))
        dataTempTuplas.clear()
    data =dataTemp

    # FIXME:
    #  check https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)
    model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    column_dict = {column.name: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    session = session_maker()
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

    return sort_methods

def _get_filter_by_args(args: str, model: Table):
    """Create constructor of filter like"""  
    filters =[]
    if args and  len(args) != 2:
        try:
            list_args = args.split(",")
            for value in (list_args):
                if re.search(r'[{/}]', (value)):
                    value = re.sub(r'[{/}]', " ", (value))
                if re.search(r'[:]', (value)):
                    value = re.sub(r'[:]', ",", (value))
                key = eval(value)[0]
                filters.append(model.columns[key].ilike(f'%{eval(value)[1]}%'))
        except KeyError as err:
                raise FieldNoExistsError(f'Field: {err.args[0]} not exists.') from err
    return(filters)


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
    
        #charset = cchardet.detect(data)['encoding'] 
        #pasamos a charset 8 porque sino identifica como gb18030 - o ISO15 que no es capaz de decodificar
        charset= 'utf-8'
    print(charset)
    try:
        data = data.decode(charset,'ignore')
    except:
        data = data.decode('utf-8', 'ignore')
    if data:
        dialect = csv.Sniffer().sniff(data)
    else:
        dialect = "excel"
    
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
    if data:
        max_key = max(data, key=len).keys()
        for item in data:
            if len(max_key) > len(item.keys()):
                for k in max_key:
                    item.setdefault(k, None)
    engine = create_engine("sqlite:///:memory:", echo=True, future=True)
    metadata = MetaData()
    if data:
        table = _get_table_from_dict(data, engine, metadata)
    else:
        table = Table(_TEMPORAL_TABLE_NAME, metadata, Column('id', Integer, primary_key = True), prefixes=['TEMPORARY'])
        metadata.create_all(engine)
    if data:
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
