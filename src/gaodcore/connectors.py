from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from urllib.parse import urlparse

import sqlalchemy.exc
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

DATABASE_SCHEMAS = {'postgresql', 'mysql', 'mssql', 'oracle'}
HTTP_SCHEMAS = {'http', 'https'}


class NotImplementedSchemaError(Exception):
    pass


class DriverConnectionError(Exception):
    pass


class NoObjectError(Exception):
    pass


@dataclass
class ConnectorConf:
    engine: Engine
    meta_data: MetaData
    session_maker: sessionmaker


connectors_conf: Dict[str, ConnectorConf] = {}


def get_resource_columns(uri: str, object_location: Optional[str], cache=True):
    connector_conf = get_connector_conf(uri=uri, cache=cache)
    Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)
    return [
        {"COLUMN_NAME": column.description, "DATA_TYPE": str(column.type)} for column in Model.columns
    ]


def validate_resource(*, uri: str, object_location: Optional[str]):
    connector_conf = get_connector_conf(uri=uri, cache=False)
    Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)

    try:
        return get_resource_data(uri=uri, object_location=object_location, filter_by={},
                          fields=[], cache=False)
    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError(str(err))
    except sqlalchemy.exc.OperationalError as err:
        raise DriverConnectionError(str(err))


def get_resource_data(*, uri: str, object_location: Optional[str], filters: Dict[str, str], fields: List[str], offset: int = 0,
                      cache: bool = True) -> List[Dict[str, Any]]:
    connector_conf = get_connector_conf(uri=uri, cache=cache)
    Model = Table(object_location, connector_conf.meta_data, autoload=True, autoload_with=connector_conf.engine)
    columns = [column for column in Model.columns if not fields or column.name in fields]
    session = connector_conf.session_maker()
    data = session.query(Model).with_entities(*columns).filter_by(**filters).offset(offset).all()
    # FIXME:
    #  check https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    # FIXME: XML serializer fail if returns a generator :(
    session.close()
    columns_names = [column.name for column in columns]
    return [dict(zip(columns_names, row)) for row in data]


def validate_uri(uri: str) -> None:
    try:
        with get_connector_conf(uri, cache=False).engine.connect() as _:
            pass
    except sqlalchemy.exc.OperationalError as err:
        raise DriverConnectionError(str(err))


def get_connector_conf(uri: str, cache: bool = True) -> ConnectorConf:
    if connector_conf := connectors_conf.get(uri):
        return connector_conf

    engine = get_engine(uri)
    meta_data = MetaData(bind=engine)
    session_maker = sessionmaker(bind=engine)
    connector_conf = ConnectorConf(
        engine=engine,
        meta_data=meta_data,
        session_maker=session_maker
    )
    if cache:
        connectors_conf[uri] = connector_conf

    return connector_conf


def get_engine(uri: str) -> Engine:
    uri_parsed = urlparse(uri)
    if uri_parsed.scheme in DATABASE_SCHEMAS:
        return create_engine(uri)
    elif uri_parsed.scheme in HTTP_SCHEMAS:
        # TODO: is required to create a loader for each content-type [csv,...] and create a database with a sqlite with
        #  this data. Is required to create a method and attributes in ConnectorConf to clear this temp database.
        pass
    raise NotImplementedSchemaError(f'Schema: "{uri_parsed.scheme}" is not implemented.')