from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Iterable
from urllib.parse import urlparse

import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql', 'oracle', 'sqlite'}
HTTP_SCHEMAS = {'http', 'https'}
RESOURCE_MAX_ROWS = 20000


class NotImplementedSchemaError(Exception):
    pass


class DriverConnectionError(Exception):
    pass


class NoObjectError(Exception):
    pass


class TooManyRowsError(Exception):
    pass


class FieldNoExistsError(Exception):
    pass


class SortFieldNoExistsError(Exception):
    pass


@dataclass
class OrderBy:
    field: str
    ascending: bool


@dataclass
class ConnectorConf:
    engine: Engine
    meta_data: MetaData
    session_maker: sessionmaker


connectors_conf: Dict[str, ConnectorConf] = {}


def get_resource_columns(uri: str, object_location: Optional[str], cache=True) -> List[Dict[str, str]]:
    connector_conf = _get_connector_conf(uri=uri, cache=cache)
    try:
        Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    return [{"COLUMN_NAME": column.description, "DATA_TYPE": str(column.type)} for column in Model.columns]


def validate_resource(*, uri: str, object_location: Optional[str]) -> Iterable[Dict[str, Any]]:
    _validate_max_rows_allowed(uri, object_location)
    return get_resource_data(uri=uri, object_location=object_location, filters={}, fields=[], cache=False, sort=[])


def _validate_max_rows_allowed(uri: str, object_location: Optional[str]):
    connector_conf = _get_connector_conf(uri=uri, cache=False)
    try:
        Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    session = connector_conf.session_maker()
    if session.query(Model).count() > RESOURCE_MAX_ROWS:
        raise TooManyRowsError()
    session.close()


def get_resource_data(*,
                      uri: str,
                      object_location: Optional[str],
                      filters: Dict[str, str],
                      fields: List[str],
                      sort: List[OrderBy],
                      limit: Optional[int] = None,
                      offset: int = 0,
                      cache: bool = True) -> Iterable[Dict[str, Any]]:
    connector_conf = _get_connector_conf(uri=uri, cache=cache)
    try:
        Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)
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
    if column_names:
        try:
            return [columns_dict[column_name] for column_name in column_names]
        except KeyError as err:
            raise FieldNoExistsError(f'Field: "{str(err)}" not exists.')
    else:
        return columns_dict.values()


def _get_sort_methods(column_dict: Dict[str, Column], sort: List[OrderBy]):
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
    try:
        with _get_connector_conf(uri, cache=False).engine.connect() as _:
            pass
    except sqlalchemy.exc.OperationalError as err:
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
        except sqlalchemy.exc.OperationalError as err:
            raise DriverConnectionError(str(err))
    elif uri_parsed.scheme in HTTP_SCHEMAS:
        # TODO: is required to create a loader for each content-type [csv,...] and create a database with a sqlite with
        #  this data. Is required to create a method and attributes in ConnectorConf to clear this temp database.
        pass
    raise NotImplementedSchemaError(f'Schema: "{uri_parsed.scheme}" is not implemented.')
