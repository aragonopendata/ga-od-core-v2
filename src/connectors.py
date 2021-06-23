import urllib.request
from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Iterable
from urllib.parse import urlparse

import pandas as pd
import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

_DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite'}
_HTTP_SCHEMAS = {'http', 'https'}
_RESOURCE_MAX_ROWS = 250000
_TEMPORAL_TABLE_NAME = 'temporal_table'


class NotImplementedSchemaError(Exception):
    """Schema of the URI is not implemented. Schema examples: http, https, postgresql, ftp, etc."""
    pass


class DriverConnectionError(Exception):
    """Connection cannot be established."""
    pass


class NoObjectError(Exception):
    """Object is not available. Connection was successfully done but object is not available."""
    pass


class TooManyRowsError(Exception):
    """Object have too many rows to process. There are a hard limit to limit memory usage."""
    pass


class FieldNoExistsError(Exception):
    pass


class SortFieldNoExistsError(Exception):
    pass


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
        return Table(object_location,
                     meta_data,
                     autoload=True,
                     autoload_with=engine,
                     schema=object_location_schema)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))


def get_resource_columns(uri: str,
                         object_location: Optional[str],
                         object_location_schema: Optional[str]) -> Iterable[Dict[str, str]]:
    """From resource get a list of dictionaries with column name and data type.

    @param cache: if not exists connection save on cache.
    @return: list of dictionaries with column name and data type
    """
    engine = _get_engine(uri)
    Model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    data = ({"COLUMN_NAME": column.description, "DATA_TYPE": str(column.type)} for column in Model.columns)
    engine.dispose()
    return data


def validate_resource(*, uri: str, object_location: Optional[str],
                      object_location_schema: Optional[str]) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available and have less rows than allowed. Return data of resource, a iterable of
    dictionaries."""
    _validate_max_rows_allowed(uri, object_location, object_location_schema=object_location_schema)
    return get_resource_data(uri=uri,
                             object_location=object_location,
                             object_location_schema=object_location_schema,
                             filters={},
                             fields=[],
                             sort=[])


def _validate_max_rows_allowed(uri: str, object_location: Optional[str], object_location_schema: Optional[str]):
    """Validate if resource have less rows than allowed."""
    engine = _get_engine(uri)
    session_maker = sessionmaker(bind=engine)

    Model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    session = session_maker()
    if session.query(Model).count() > _RESOURCE_MAX_ROWS:
        raise TooManyRowsError()
    session.close()
    session_maker.close_all()
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

    Model = _get_model(engine=engine, object_location=object_location, object_location_schema=object_location_schema)

    column_dict = {column.name: column for column in Model.columns}
    columns = _get_columns(column_dict, fields)
    sort_methods = _get_sort_methods(column_dict, sort)
    session = session_maker()
    data = session.query(Model).filter_by(**filters).order_by(*sort_methods).with_entities(
        *columns).offset(offset).limit(limit).all()
    # FIXME:
    #  check https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    # FIXME: XML serializer fail if returns a generator :(
    columns_names = [column.name for column in columns]
    data = (dict(zip(columns_names, row)) for row in data)

    session.close()
    session_maker.close_all()
    engine.dispose()

    return data


def _get_columns(columns_dict: Dict[str, Column], column_names: List[str]) -> Iterable[Column]:
    """Get SQLAlchemy column instances from column names."""
    if column_names:
        try:
            return [columns_dict[column_name] for column_name in column_names]
        except KeyError as err:
            raise FieldNoExistsError(f'Field: "{str(err)}" not exists.')
    else:
        return columns_dict.values()


def _get_sort_methods(column_dict: Dict[str, Column], sort: List[OrderBy]):
    """Create a list of SQLAlchemy column instances that represent query sorting."""
    sort_methods = []
    for item in sort:
        try:
            column = column_dict[item.field]
        except KeyError as err:
            raise SortFieldNoExistsError(f'Sort field: "{str(err)} not exists.')
        if item.ascending:
            sort_methods.append(column)
        else:
            sort_methods.append(column.desc())

    return sort_methods


def validate_uri(uri: str) -> None:
    """Validate if URI is available as resource."""
    try:
        with _get_engine(uri).connect() as _:
            pass
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError) as err:
        raise DriverConnectionError(str(err))


def _get_engine(uri: str) -> Engine:
    uri_parsed = urlparse(uri)
    if uri_parsed.scheme in _DATABASE_SCHEMAS:
        try:
            return create_engine(uri)
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError) as err:
            raise DriverConnectionError(str(err))
    elif uri_parsed.scheme in _HTTP_SCHEMAS:
        with urllib.request.urlopen(uri) as f:
            df = pd.read_csv(f)

        engine = create_engine("sqlite:///:memory:", echo=True, future=True)
        df.to_sql(_TEMPORAL_TABLE_NAME, engine)
        return engine
    raise NotImplementedSchemaError(f'Schema: "{uri_parsed.scheme}" is not implemented.')
