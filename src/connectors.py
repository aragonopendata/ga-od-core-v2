from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Iterable
from urllib.parse import urlparse

import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite'}
HTTP_SCHEMAS = {'http', 'https'}
RESOURCE_MAX_ROWS = 250000


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


@dataclass
class ConnectorConf:
    """Defines connection configuration."""
    engine: Engine
    meta_data: MetaData
    session_maker: sessionmaker


connectors_conf: Dict[str, ConnectorConf] = {}


def get_resource_columns(uri: str,
                         object_location: Optional[str],
                         object_location_schema: Optional[str],
                         cache=True) -> Iterable[Dict[str, str]]:
    """From resource get a list of dictionaries with column name and data type.

    @param cache: if not exists connection save on cache.
    @return: list of dictionaries with column name and data type
    """
    connector_conf = _get_connector_conf(uri=uri, cache=cache)
    try:
        Model = Table(object_location,
                      connector_conf.meta_data,
                      autoload=True,
                      autoload_with=connector_conf.engine,
                      schema=object_location_schema)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    return ({"COLUMN_NAME": column.description, "DATA_TYPE": str(column.type)} for column in Model.columns)


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
                             cache=False,
                             sort=[])


def _validate_max_rows_allowed(uri: str, object_location: Optional[str], object_location_schema: Optional[str]):
    """Validate if resource have less rows than allowed."""
    connector_conf = _get_connector_conf(uri=uri, cache=False)
    try:
        Model = Table(object_location,
                      connector_conf.meta_data,
                      autoload=True,
                      autoload_with=connector_conf.engine,
                      schema=object_location_schema)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    session = connector_conf.session_maker()
    if session.query(Model).count() > RESOURCE_MAX_ROWS:
        raise TooManyRowsError()
    session.close()


def get_resource_data(*,
                      uri: str,
                      object_location: Optional[str],
                      object_location_schema: Optional[str],
                      filters: Dict[str, str],
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0,
                      cache: bool = True) -> Iterable[Dict[str, Any]]:
    connector_conf = _get_connector_conf(uri=uri, cache=cache)
    """Return a iterable of dictionaries with data of resource."""
    try:
        Model = Table(object_location,
                      connector_conf.meta_data,
                      autoload=True,
                      autoload_with=connector_conf.engine,
                      schema=object_location_schema)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    column_dict = {column.name: column for column in Model.columns}
    columns = _get_columns(column_dict, fields)
    sort_methods = _get_sort_methods(column_dict, sort)
    session = connector_conf.session_maker()
    data = session.query(Model).filter_by(**filters).order_by(*sort_methods).with_entities(
        *columns).offset(offset).limit(limit).all()
    # FIXME:
    #  check https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    # FIXME: XML serializer fail if returns a generator :(
    session.close()
    columns_names = [column.name for column in columns]
    return (dict(zip(columns_names, row)) for row in data)


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
        with _get_connector_conf(uri, cache=False).engine.connect() as _:
            pass
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError) as err:
        raise DriverConnectionError(str(err))


def _get_connector_conf(uri: str, cache: bool = True) -> ConnectorConf:

    if connector_conf := connectors_conf.get(uri):
        return connector_conf

    engine = _get_engine(uri)
    meta_data = MetaData(bind=engine)
    session_maker = sessionmaker(bind=engine)
    connector_conf = ConnectorConf(engine=engine, meta_data=meta_data, session_maker=session_maker)
    if cache:
        connectors_conf[uri] = connector_conf

    return connector_conf


def _get_engine(uri: str) -> Engine:
    uri_parsed = urlparse(uri)
    if uri_parsed.scheme in DATABASE_SCHEMAS:
        try:
            return create_engine(uri)
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError) as err:
            raise DriverConnectionError(str(err))
    elif uri_parsed.scheme in HTTP_SCHEMAS:
        # TODO: is required to create a loader for each content-type [csv,...] and create a database with a sqlite with
        #  this data. Is required to create a method and attributes in ConnectorConf to clear this temp database.
        pass
    raise NotImplementedSchemaError(f'Schema: "{uri_parsed.scheme}" is not implemented.')
