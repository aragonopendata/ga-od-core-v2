"""Module that deal with external resources."""

import contextlib
import csv
import decimal
import json
import logging
import ast
import math
import re
import socket
import urllib.request
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from http import HTTPStatus
from io import StringIO
from sqlite3 import Date
from time import monotonic
from typing import Optional, Dict, List, Any, Iterable, Union, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

import sqlalchemy.exc
from sqlalchemy.dialects import mssql as mssql_dialect
from django.utils.duration import duration_iso_string
from django.utils.functional import Promise
from geoalchemy2 import functions as GeoFunc
from rest_framework.exceptions import ValidationError

from exceptions import ServiceUnavailable, ErrorCodes
from log_sanitizer import mask_uri
from phase_timing import PhaseTimer, phase
from sqlalchemy import (
    event,
    create_engine,
    Table,
    MetaData,
    Column,
    Boolean,
    Text,
    Integer,
    DateTime,
    Time,
    REAL,
    text,
    quoted_name,
)
import warnings
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Numeric

from gaodcore.operators import is_datetime
from gaodcore.operators import process_filters_args
from gaodcore_manager.models import ResourceSizeConfig, ResourceConfig

logger = logging.getLogger(__name__)

_DATABASE_SCHEMAS = {
    "postgresql",
    "mysql",
    "mssql",
    "mssql+pyodbc",
    "oracle+oracledb",
    "sqlite",
}
_HTTP_SCHEMAS = {"http", "https"}
_RESOURCE_MAX_ROWS = 1048576
_TEMPORAL_TABLE_NAME = "temporal_table"
_SQLALCHEMY_MAP_TYPE = {
    bool: Boolean,
    str: Text,
    int: Integer,
    float: REAL,
    date: Date,
    datetime: DateTime,
    time: Time,
}

# TODO: move to config.yaml (e.g. common_config.http_connector_timeout_seconds)
DEFAULT_HTTP_TIMEOUT_SECONDS = 10

# TODO: move to config.yaml (e.g. common_config.oracle_call_timeout_seconds)
#
# Oracle has no timeout unless one is set explicitly here: unlike PostgreSQL and
# MySQL, the driver applies no default, so an unresponsive instance blocks the
# worker until gunicorn kills it (--timeout 240 in the Dockerfile).
#
# ORACLE_CALL_TIMEOUT_SECONDS bounds a single round trip to the database, not the
# total query duration. A query streaming many rows performs many round trips and
# is not capped by this value; what it does cap is any individual stall, which is
# the failure it exists to catch (a server that stops answering mid-query, or one
# that never answers at all). It is deliberately below gunicorn's 240s so a stalled
# query surfaces as a clean 503 with room to serialize the response, rather than a
# SIGKILLed worker.
ORACLE_CALL_TIMEOUT_SECONDS = 180

# Fallback when the ORACLE_ARRAYSIZE Django setting is absent.
DEFAULT_ORACLE_ARRAYSIZE = 5000


class MimeType(Enum):
    """Enum with some mimetype and his different values."""

    CSV = ("text/csv",)
    XLSX = (
        "application/xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    JSON = ("application/json",)


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

    message = None

    def __init__(self, message="Sort field not exists."):
        self.message = message


class MimeTypeError(Exception):
    """Type of document is not csv or excel."""


@dataclass
class OrderBy:
    """Defines field and is ascending sort. This is used to generate SQL queries."""

    field: str
    ascending: bool


def _create_table_from_information_schema(
    engine: Engine,
    object_location: str,
    object_location_schema: str,
    meta_data: MetaData,
) -> Table:
    """
    Create a SQLAlchemy Table object by querying information_schema instead of using reflection.

    This fallback approach is used when standard SQLAlchemy reflection fails, commonly
    due to PostgreSQL system catalog issues or custom data types.

    @param engine: SQLAlchemy Engine instance
    @param object_location: Table/view name
    @param object_location_schema: Schema name
    @param meta_data: MetaData object to attach the table to

    @return: SQLAlchemy Table object
    @raises NoObjectError: If table doesn't exist or no columns found
    @raises DriverConnectionError: If connection fails
    """

    schema = object_location_schema or "public"

    # PostgreSQL data type mapping for common types
    pg_type_mapping = {
        "integer": Integer,
        "bigint": Integer,
        "smallint": Integer,
        "numeric": Numeric,
        "real": REAL,
        "double precision": REAL,
        "text": Text,
        "character varying": Text,
        "character": Text,
        "varchar": Text,
        "char": Text,
        "boolean": Boolean,
        "date": Date,
        "timestamp without time zone": DateTime,
        "timestamp with time zone": DateTime,
        "time without time zone": Time,
        "time with time zone": Time,
        # PostGIS and other custom types default to Text
        "geometry": Text,
        "geography": Text,
        "uuid": Text,
        "json": Text,
        "jsonb": Text,
        "xml": Text,
        "inet": Text,
        "cidr": Text,
        "macaddr": Text,
    }

    try:
        with engine.connect() as conn:
            # Check if table/view exists
            exists_query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            UNION ALL
            SELECT 1 FROM information_schema.views
            WHERE table_schema = :schema AND table_name = :table
            """

            result = conn.execute(
                text(exists_query), {"schema": schema, "table": object_location}
            )
            if not result.fetchone():
                raise NoObjectError(
                    f"Table/view {schema}.{object_location} does not exist"
                )

            # Get column information
            columns_query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """

            result = conn.execute(
                text(columns_query), {"schema": schema, "table": object_location}
            )
            columns_info = result.fetchall()

            if not columns_info:
                raise NoObjectError(
                    f"No columns found for table {schema}.{object_location}"
                )

            # Create SQLAlchemy columns
            sqlalchemy_columns = []
            for col_info in columns_info:
                col_name = col_info[0]
                col_type = col_info[1].lower()
                is_nullable = col_info[2] == "YES"

                # Map PostgreSQL type to SQLAlchemy type
                sqlalchemy_type = pg_type_mapping.get(col_type, Text)

                # Create column
                column = Column(col_name, sqlalchemy_type, nullable=is_nullable)
                sqlalchemy_columns.append(column)

            # Create the table
            table = Table(
                object_location,
                meta_data,
                *sqlalchemy_columns,
                schema=object_location_schema,
            )

            logger.info(
                "Successfully created table using information_schema fallback. Table: %s, Schema: %s, Columns: %d",
                object_location,
                object_location_schema,
                len(sqlalchemy_columns),
                extra={
                    "fallback_success": True,
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "column_count": len(sqlalchemy_columns),
                    "column_names": [col.name for col in sqlalchemy_columns],
                },
            )

            return table

    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError("Object not available.") from err
    except (
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.DatabaseError,
        sqlalchemy.exc.ProgrammingError,
    ) as err:
        logger.error(
            "Database connection error in information_schema fallback: %s",
            str(err),
        )
        raise DriverConnectionError("Connection not available.") from err


def _create_table_from_oracle_system_views(
    engine: Engine,
    object_location: str,
    object_location_schema: str,
    meta_data: MetaData,
) -> Table:
    """
    Create a SQLAlchemy Table object by querying Oracle system views instead of using reflection.

    This fallback approach is used when standard SQLAlchemy reflection fails with Oracle views
    or tables that cannot be introspected properly.

    @param engine: SQLAlchemy Engine instance
    @param object_location: Table/view name
    @param object_location_schema: Schema name
    @param meta_data: MetaData object to attach the table to

    @return: SQLAlchemy Table object
    @raises NoObjectError: If table doesn't exist or no columns found
    @raises DriverConnectionError: If connection fails
    """

    # Oracle data type mapping for common types
    oracle_type_mapping = {
        "VARCHAR2": Text,
        "CHAR": Text,
        "NVARCHAR2": Text,
        "NCHAR": Text,
        "CLOB": Text,
        "NCLOB": Text,
        "NUMBER": REAL,
        "BINARY_FLOAT": REAL,
        "BINARY_DOUBLE": REAL,
        "DATE": DateTime,
        "TIMESTAMP": DateTime,
        "TIMESTAMP(6)": DateTime,
        "BLOB": Text,  # For simplicity
        "RAW": Text,
        "LONG": Text,
        "LONG RAW": Text,
        # PostGIS and spatial types
        "SDO_GEOMETRY": Text,
        "GEOMETRY": Text,
        "GEOGRAPHY": Text,
    }

    try:
        with engine.connect() as conn:
            # Check if table/view exists in Oracle system views
            exists_query = """
            SELECT 1 FROM all_tables
            WHERE UPPER(owner) = UPPER(:schema) AND UPPER(table_name) = UPPER(:table)
            UNION ALL
            SELECT 1 FROM all_views
            WHERE UPPER(owner) = UPPER(:schema) AND UPPER(view_name) = UPPER(:table)
            """

            result = conn.execute(
                text(exists_query),
                {"schema": object_location_schema, "table": object_location},
            )
            if not result.fetchone():
                raise NoObjectError(
                    f"Table/view {object_location_schema}.{object_location} does not exist"
                )

            # Get column information from Oracle system views
            columns_query = """
            SELECT
                column_name,
                data_type,
                nullable,
                column_id
            FROM all_tab_columns
            WHERE UPPER(owner) = UPPER(:schema) AND UPPER(table_name) = UPPER(:table)
            ORDER BY column_id
            """

            result = conn.execute(
                text(columns_query),
                {"schema": object_location_schema, "table": object_location},
            )
            columns_info = result.fetchall()

            if not columns_info:
                raise NoObjectError(
                    f"No columns found for table {object_location_schema}.{object_location}"
                )

            # Create SQLAlchemy columns
            sqlalchemy_columns = []
            for col_info in columns_info:
                col_name = col_info[0]
                col_type = col_info[1]
                is_nullable = col_info[2] == "Y"

                # Map Oracle type to SQLAlchemy type
                sqlalchemy_type = oracle_type_mapping.get(col_type, Text)
                if col_type.startswith("NUMBER"):
                    sqlalchemy_type = REAL

                # Quote column name for Oracle compatibility and create lowercase key for API
                quoted_col_name = quoted_name(col_name, quote=True)
                lowercase_name = col_name.lower()

                # Create column with quoted name for Oracle but lowercase key for API
                column = Column(
                    quoted_col_name,
                    sqlalchemy_type,
                    nullable=is_nullable,
                    key=lowercase_name,
                )

                sqlalchemy_columns.append(column)

            # Create the table
            table = Table(
                object_location,
                meta_data,
                *sqlalchemy_columns,
                schema=object_location_schema,
            )

            logger.info(
                "Successfully created Oracle table using ALL_TAB_COLUMNS fallback. Table: %s, Schema: %s, Columns: %d",
                object_location,
                object_location_schema,
                len(sqlalchemy_columns),
                extra={
                    "fallback_success": True,
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "column_count": len(sqlalchemy_columns),
                    "column_names": [col.name for col in sqlalchemy_columns],
                },
            )

            return table

    except sqlalchemy.exc.NoSuchTableError as err:
        raise NoObjectError("Object not available.") from err
    except (
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.DatabaseError,
        sqlalchemy.exc.ProgrammingError,
    ) as err:
        logger.error(
            "Database connection error in Oracle system views fallback: %s",
            str(err),
        )
        raise DriverConnectionError("Connection not available.") from err


def _get_model(
    *, engine: Engine, object_location: str, object_location_schema: str
) -> Table:
    """
    Get SQLAlchemy model from object_location and object_location_schema.

    This function implements a graceful fallback approach to handle PostgreSQL reflection
    issues. It first attempts standard SQLAlchemy reflection, then falls back to manual
    table creation using information_schema queries if reflection fails.

    Common reflection failures include:
    - PostgreSQL "failed to find conversion function from unknown to text" errors
    - Custom data types (PostGIS, enums) that confuse SQLAlchemy
    - Version compatibility issues between SQLAlchemy and PostgreSQL
    - System catalog permission or corruption issues

    @param engine: SQLAlchemy Engine instance to connect to the database.
    @param object_location: The name of the table or object location.
    @param object_location_schema: The schema of the object location.

    @return: SQLAlchemy Table object representing the specified table.

    @raises NoObjectError: If the specified table does not exist.
    @raises DriverConnectionError: If there is an issue with the database connection.
    """

    object_location = object_location or _TEMPORAL_TABLE_NAME
    meta_data = MetaData()

    # Strategy 1: Try standard SQLAlchemy reflection
    try:
        # Suppress SQLAlchemy warnings about unknown types
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Did not recognize type",
                category=sqlalchemy.exc.SAWarning,
            )
            return Table(
                object_location,
                meta_data,
                autoload_with=engine,
                schema=object_location_schema,
            )
    except sqlalchemy.exc.NoSuchTableError as err:
        # Check if this is Oracle and try fallback
        if "oracle" in str(engine.url).lower():
            logger.info(
                "Oracle reflection failed, trying ALL_TAB_COLUMNS fallback. This is expected with some Oracle views. Table: %s, Schema: %s, Error: %s",
                object_location,
                object_location_schema,
                str(err),
                extra={
                    "fallback_reason": "oracle_reflection_error",
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "engine_url": str(engine.url)[:50] + "...",
                    "error_type": type(err).__name__,
                },
            )

            # Strategy 2: Try fallback approach for Oracle
            try:
                return _create_table_from_oracle_system_views(
                    engine, object_location, object_location_schema, meta_data
                )
            except Exception as fallback_err:
                logger.error(
                    "Fallback also failed for Oracle table. Table: %s, Schema: %s, Error: %s",
                    object_location,
                    object_location_schema,
                    str(fallback_err),
                    extra={
                        "fallback_failed": True,
                        "original_error": str(err),
                        "fallback_error": str(fallback_err),
                    },
                )
                raise NoObjectError("Object not available.") from err
        else:
            logger.warning(
                "Table does not exist. Table: %s, Schema: %s, Url: %s",
                object_location,
                object_location_schema,
                engine.url,
            )
            raise NoObjectError("Object not available.") from err
    except (
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.DatabaseError,
        sqlalchemy.exc.ProgrammingError,
    ) as err:
        # Check if this is a connection error (not a reflection error)
        error_str = str(err).lower()
        is_connection_error = any(
            keyword in error_str
            for keyword in [
                "ora-12514",  # TNS:listener does not know of service
                "ora-12541",  # TNS:no listener
                "ora-12170",  # TNS:connect timeout
                "ora-01017",  # invalid username/password
                "connection refused",
                "connection timeout",
                "network error",
                "could not connect",
            ]
        )

        if is_connection_error:
            logger.error(
                "Database connection error (not a reflection issue). Table: %s, Schema: %s, Error: %s",
                object_location,
                object_location_schema,
                str(err),
                extra={
                    "connection_error": True,
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "engine_url": str(engine.url)[:50] + "...",
                    "error_type": type(err).__name__,
                    "original_error": str(err)[:200],
                },
            )
            raise DriverConnectionError(
                f"Connection not available: {str(err)[:200]}"
            ) from err

        # Log reflection failure and attempt fallback for PostgreSQL and Oracle
        if "postgresql" in str(engine.url).lower():
            logger.info(
                "PostgreSQL reflection failed, using information_schema fallback. This is expected with older PostgreSQL versions (9.x). Table: %s, Schema: %s, Error: %s",
                object_location,
                object_location_schema,
                str(err),
                extra={
                    "fallback_reason": "postgresql_reflection_error",
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "engine_url": str(engine.url)[:50] + "...",
                    "error_type": type(err).__name__,
                },
            )

            # Strategy 2: Try fallback approach for PostgreSQL
            try:
                return _create_table_from_information_schema(
                    engine, object_location, object_location_schema, meta_data
                )
            except Exception as fallback_err:
                logger.error(
                    "Fallback also failed for PostgreSQL table. Table: %s, Schema: %s, Error: %s",
                    object_location,
                    object_location_schema,
                    str(fallback_err),
                    extra={
                        "fallback_failed": True,
                        "original_error": str(err),
                        "fallback_error": str(fallback_err),
                    },
                )
                raise DriverConnectionError("Connection not available.") from err
        elif "oracle" in str(engine.url).lower():
            logger.info(
                "Oracle reflection failed, using ALL_TAB_COLUMNS fallback. This is expected with some Oracle views and tables. Table: %s, Schema: %s, Error: %s",
                object_location,
                object_location_schema,
                str(err),
                extra={
                    "fallback_reason": "oracle_reflection_error",
                    "object_location": object_location,
                    "object_location_schema": object_location_schema,
                    "engine_url": str(engine.url)[:50] + "...",
                    "error_type": type(err).__name__,
                },
            )

            # Strategy 2: Try fallback approach for Oracle
            try:
                return _create_table_from_oracle_system_views(
                    engine, object_location, object_location_schema, meta_data
                )
            except Exception as fallback_err:
                logger.error(
                    "Fallback also failed for Oracle table. Table: %s, Schema: %s, Original error: %s, Fallback error: %s",
                    object_location,
                    object_location_schema,
                    str(err),
                    str(fallback_err),
                    extra={
                        "fallback_failed": True,
                        "original_error": str(err),
                        "fallback_error": str(fallback_err),
                    },
                )
                raise DriverConnectionError("Connection not available.") from err
        else:
            logger.error(
                "Database connection error. Url: %s, Error: %s", engine.url, err
            )
            raise DriverConnectionError("Connection not available.") from err


@dataclass
class OpenResource:
    """An engine and the reflected table of a resource, shared across the steps of a request."""

    engine: Engine
    model: Table


def _create_engine_and_model(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int],
    timer: Optional[PhaseTimer],
) -> OpenResource:
    """Create the engine and reflect the table inside the ``connect`` phase.

    The engine is disposed here only if the reflection fails; otherwise the caller owns it.
    """
    with phase(timer, "connect"):
        engine = _get_engine(uri, timeout=timeout)
        try:
            model = _get_model(
                engine=engine,
                object_location=object_location,
                object_location_schema=object_location_schema,
            )
        except BaseException:
            engine.dispose()
            raise
    return OpenResource(engine=engine, model=model)


@contextlib.contextmanager
def open_resource(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
):
    """Create the engine and reflect the table once, and dispose the engine once on exit.

    Pass the yielded object as ``resource=`` to the data functions so a request makes a single connection
    (a single download for HTTP connectors) and a single reflection. The engine must not be disposed between
    steps: the in-memory SQLite engine used for HTTP resources would lose its data.
    """
    resource = _create_engine_and_model(
        uri, object_location, object_location_schema, timeout, timer
    )
    try:
        yield resource
    finally:
        resource.engine.dispose()


def _acquire_resource(
    resource: Optional[OpenResource],
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int],
    timer: Optional[PhaseTimer],
) -> Tuple[OpenResource, bool]:
    """Return ``(resource, owned)``: the shared one, or a new one the caller must dispose."""
    if resource is not None:
        return resource, False
    return (
        _create_engine_and_model(
            uri, object_location, object_location_schema, timeout, timer
        ),
        True,
    )


@contextlib.contextmanager
def _owned_resource(
    resource: Optional[OpenResource],
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int],
    timer: Optional[PhaseTimer],
):
    """Yield a resource and dispose its engine on exit only when it was created here.

    A shared resource passed in by the caller is never disposed: its owner (``open_resource``) does it.
    """
    resource, owned = _acquire_resource(
        resource, uri, object_location, object_location_schema, timeout, timer
    )
    try:
        yield resource
    finally:
        if owned:
            resource.engine.dispose()


def get_resource_columns(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int] = None,
) -> Iterable[Dict[str, str]]:
    """From resource get a list of dictionaries with column name and data type.

    @param cache: if not exists connection save on cache.
    @return: list of dictionaries with column name and data type
    """
    engine = _get_engine(uri, timeout=timeout)
    try:
        model = _get_model(
            engine=engine,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )
    finally:
        engine.dispose()

    # Convert Oracle column names to lowercase for consistency
    from urllib.parse import urlparse

    uri_parsed = urlparse(uri)
    is_oracle = uri_parsed.scheme == "oracle+oracledb" or "oracle" in uri_parsed.scheme

    data = []
    for column in model.columns:
        column_name = column.name
        # For Oracle databases, convert column names to lowercase
        if is_oracle:
            column_name = column_name.lower()

        data.append({"COLUMN_NAME": column_name, "DATA_TYPE": str(column.type)})

    return data


def validate_resource(
    *,
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    limit: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available . Return data of resource, a iterable of
    dictionaries.

    @param limit: maximum number of rows to fetch. The manual manager check passes
        1, since proving the resource is queryable does not need the whole result.
    """

    def _validate_resource_internal():
        _validate_max_rows_allowed(
            uri,
            object_location,
            object_location_schema=object_location_schema,
            timeout=timeout,
        )
        return get_resource_data(
            uri=uri,
            object_location=object_location,
            object_location_schema=object_location_schema,
            filters={},
            like="",
            fields=[],
            sort=[],
            limit=limit,
            timeout=timeout,
        )

    # For Oracle connections, timeout is handled at the connection level
    return _validate_resource_internal()


def validate_resource_mssql(
    *, uri: str, object_location: Optional[str], object_location_schema: Optional[str]
) -> Iterable[Dict[str, Any]]:
    """Validate if resource is available . Return data of resource, a iterable of
    dictionaries."""
    engine = _get_engine(uri)
    try:
        session_maker = sessionmaker(bind=engine)

        model = _get_model(
            engine=engine,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )

        column_dict = {column.key: column for column in model.columns}
        columns = _get_columns(column_dict, [])
        with session_maker() as session:
            data = (
                session.query(model)
                .order_by(*_get_sort_methods(column_dict, []))
                .with_entities(
                    *[model.c[col.key].label(col.key) for col in model.columns]
                )
                .all()
            )
    finally:
        engine.dispose()

    return (dict(zip([column.key for column in columns], row)) for row in data)


def _validate_max_rows_allowed(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int] = None,
):
    """Validate if resource is aviable."""
    engine = _get_engine(uri, timeout=timeout)
    try:
        session_maker = sessionmaker(bind=engine)

        model = _get_model(
            engine=engine,
            object_location=object_location,
            object_location_schema=object_location_schema,
        )

        with session_maker() as session:
            try:
                session.query(
                    *[model.c[col.key].label(col.key) for col in model.columns]
                ).count()
            except sqlalchemy.exc.ProgrammingError as err:
                logging.exception("Object not available.")
                raise NoObjectError("Object not available.") from err
    finally:
        engine.dispose()


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\n]")


# Add feature to sanitize text include control characters
def sanitize_control_charcters(text):
    if _CONTROL_CHARS_RE.search(str(text)):
        text = _CONTROL_CHARS_RE.sub("", text)
    try:
        text = text.decode("utf-8")
    except Exception:
        pass
    if isinstance(text, str):
        text = text.strip()
    return text


def _normalize_value(column):
    """Generic normalization of one driver value (reference behaviour for any type)."""
    if isinstance(column, uuid.UUID):
        # Handle UUID objects by converting to string
        return str(column)
    if (
        isinstance(column, (decimal.Decimal, Promise))
        or isinstance(column, float)
        or isinstance(column, Numeric)
    ):
        parte_decimal, _ = math.modf(column)
        if parte_decimal == 0.0:
            try:
                return int(column)
            except OverflowError:  # inf / -inf have no integer value: they are null, as NaN is in the output
                return None
        return sanitize_control_charcters(column)
    return sanitize_control_charcters(column)


def _normalize_identity(value):
    return value


def _normalize_str(value):
    if _CONTROL_CHARS_RE.search(value):
        value = _CONTROL_CHARS_RE.sub("", value)
    return value.strip()


def _normalize_bytes(value):
    # str(bytes) escapes every control character, so the generic cleanup never removes anything from bytes.
    try:
        return value.decode("utf-8").strip()
    except UnicodeDecodeError:
        return value


def _normalize_number(value):
    # float and Decimal: integral values become int, the rest is returned as is (nan is kept, inf becomes None).
    parte_decimal, _ = math.modf(value)
    if parte_decimal == 0.0:
        try:
            return int(value)
        except OverflowError:  # inf / -inf have no integer value: they are null, as NaN is in the output
            return None
    return value


# Exact type -> conversion. Subclasses and any other type go through ``_normalize_value``.
_NORMALIZERS = {
    str: _normalize_str,
    int: _normalize_identity,
    bool: _normalize_identity,
    type(None): _normalize_identity,
    float: _normalize_number,
    decimal.Decimal: _normalize_number,
    bytes: _normalize_bytes,
    uuid.UUID: str,
    datetime: _normalize_identity,
    date: _normalize_identity,
    time: _normalize_identity,
    timedelta: _normalize_identity,
}


def get_GeoJson_resource(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
    resource: Optional[OpenResource] = None,
) -> Boolean:
    """From resource return if is GeoJson resource

    @param resource: shared engine and model; when omitted one is created and disposed here.
    @return: True or False GeoJosn Resource
    """
    geoJson = False

    with _owned_resource(
        resource, uri, object_location, object_location_schema, timeout, timer
    ) as resource:
        for column in resource.model.columns:
            if str(column.type).startswith("geometry") or str(column.type).startswith(
                "geography"
            ):
                geoJson = True
    return geoJson


def get_resource_data_feature(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    filters: Dict[str, str],
    like: Union[str, Dict],
    fields: List[str],
    sort: List[OrderBy],
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
    resource: Optional[OpenResource] = None,
):
    """data like GeoJSON .Encoding data a variety of geographic data structures."""
    """Data like Feature_Collection_"""

    """Not posible to implement GeoFunc.ST_AsGeoJSON(rows) with model, postgis version  is < 3.0 """

    with _owned_resource(
        resource, uri, object_location, object_location_schema, timeout, timer
    ) as resource:
        return _build_feature_collection(
            resource, uri, filters, like, sort, limit, offset, timer
        )


def _build_feature_collection(
    resource: OpenResource,
    uri: str,
    filters: Dict[str, str],
    like: Union[str, Dict],
    sort: List[OrderBy],
    limit: Optional[int],
    offset: int,
    timer: Optional[PhaseTimer],
):
    """Query and normalize the rows as a FeatureCollection. The engine is not disposed here."""
    engine, model = resource.engine, resource.model
    session_maker = sessionmaker(bind=engine)
    column_dict = {column.key: column for column in model.columns}
    # columns = _get_columns(column_dict, fields)
    filters_args = _process_like_filter(like, model)
    # The session is closed on every path, including a failed query or normalization.
    session = session_maker()
    try:
        return _query_feature_collection(
            session, model, uri, column_dict, filters, filters_args, sort, limit, offset, timer
        )
    finally:
        session.close()


def _query_feature_collection(
    session, model, uri, column_dict, filters, filters_args, sort, limit, offset, timer
):
    parsed = urlparse(uri)

    # Get Column Geom - other fields in Properties

    propertiesCol = []
    propertiesField = []

    for i, col in enumerate(model.columns):
        if str(col.type).startswith("geography") or str(col.type).startswith(
            "geometry"
        ):
            Geom = model.c[col.name].label(col.name)
        else:
            propertiesField.append(col.name)
            propertiesCol.append(model.c[col.name].label(col.name))

    # Get A JSon Properties ( not possible json_build_object PostgreSQL 9.2.24 on x86_64-unknown-linux-gnu,
    # compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-16), 64-bit
    # version of Postgress not allowed _ Postgres and A GeoJson  only _
    # https://www.postgresql.org/docs/9.2/functions-json.html"
    # https://www.postgresql.org/docs/current/functions-json.html -> to convert columns to a json in a query
    query_started = monotonic()
    try:
        if parsed.scheme in ["mssql+pyodbc"]:
            data = (
                session.query(
                    *propertiesCol, (GeoFunc.ST_AsGeoJSON(Geom)).label("geometry")
                )
                .filter_by(**filters)
                .filter(*filters_args)
                .all()
            )
        else:
            data = (
                session.query(
                    *propertiesCol, (GeoFunc.ST_AsGeoJSON(Geom)).label("geometry")
                )
                .filter_by(**filters)
                .filter(*filters_args)
                .order_by(*_get_sort_methods(column_dict, sort))
                .offset(offset)
                .limit(limit)
                .all()
            )
    except sqlalchemy.exc.ProgrammingError as err:
        raise NoObjectError("Object not available.") from err
    finally:
        if timer is not None:
            timer.add("query", monotonic() - query_started)
    normalize_started = monotonic()
    columnsProperties = _get_columns(column_dict, propertiesField)

    # Serializar Feature Collection
    featuresTot = []
    for item in data:
        item = list(item)
        geometry = item.pop()
        for i, column in enumerate(item):
            if isinstance(column, uuid.UUID):
                item[i] = str(column)
            elif isinstance(column, (decimal.Decimal, Promise, float, Numeric)):
                parte_decimal, parte_entera = math.modf(column)
                if (parte_decimal) == 0.0:
                    try:
                        item[i] = int(column)
                    except OverflowError:  # inf / -inf are null
                        item[i] = None
                else:
                    item[i] = str(column)
            elif isinstance(column, datetime):
                r = column.isoformat()
                if column.microsecond:
                    r = r[:23] + r[26:]
                if r.endswith("+00:00"):
                    r = r[:-6] + "Z"
                item[i] = r
            elif isinstance(column, date):
                item[i] = column.isoformat()
            elif isinstance(column, time):
                r = column.isoformat()
                if column.microsecond:
                    r = r[:12] + r[15:]  # milliseconds only; keep the UTC offset of an aware time, if any
                item[i] = r
            elif isinstance(column, timedelta):
                item[i] = duration_iso_string(column)

            else:
                item[i] = sanitize_control_charcters(column)

        properties = dict(zip([column.name for column in columnsProperties], item))

        # create feature. ST_AsGeoJSON(NULL) is NULL: RFC 7946 §3.2 allows an
        # unlocated feature with a null geometry, so keep the row.
        featureType = {
            "type": "Feature",
            "geometry": json.loads(geometry) if geometry is not None else None,
            "properties": properties,
        }
        featuresTot.append(featureType)

    wrapped = {"type": "FeatureCollection", "features": featuresTot}
    if timer is not None:
        timer.add("normalize", monotonic() - normalize_started)

    return wrapped


def get_session_data(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    filters: Dict[str, Union[str, dict]],
    like: Union[str, Dict],
    fields: List[str],
    sort: List[OrderBy],
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
    resource: Optional[OpenResource] = None,
):
    """Retrieve the rows of a resource. See ``_get_session_data_and_columns``."""
    data, _column_names = _get_session_data_and_columns(
        uri,
        object_location,
        object_location_schema,
        filters,
        like,
        fields,
        sort,
        limit,
        offset,
        timeout,
        timer,
        resource,
    )
    return data


def _get_session_data_and_columns(
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    filters: Dict[str, Union[str, dict]],
    like: Union[str, Dict],
    fields: List[str],
    sort: List[OrderBy],
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
    resource: Optional[OpenResource] = None,
):
    """
    Retrieve data from a resource based on the provided parameters.

    This function connects to a database or API specified by the URI and retrieves data from the specified
    table or object location. It applies filters, LIKE-based filtering, field selection, sorting, and pagination
    to the query.

    @param uri: The URI of the resource.
    @param object_location: The name of the table or object location.
    @param object_location_schema: The schema of the object location.
    @param filters: A dictionary of filters to apply to the query. Each key is a field name, and the value can be
                   a string or a dictionary specifying an operator and value.
    @param like: A string for LIKE-based filtering.
    @param fields: A list of field names to include in the result.
    @param sort: A list of OrderBy objects to sort the result.
    @param limit: An optional limit on the number of rows to return.
    @param offset: The number of rows to skip before starting to return rows.
    @param resource: Shared engine and model. When omitted they are created here and the engine is disposed here;
                     a shared engine is never disposed by this function.

    @return: A tuple ``(data, column_names)``: the rows of the resource and the names of the selected columns, in
             the order of the rows' values. The names are known even when there are no rows.

    @raises sqlalchemy.exc.CompileError: If the query compilation fails.
    @raises sqlalchemy.exc.ProgrammingError: If there is a programming error in the query.
    """
    # sqlalchemy.exc.CompileError: MSSQL requires an order_by when using an OFFSET or a non-simple LIMIT clause
    # (pyodbc.ProgrammingError) ('42000', '[42000] [FreeTDS][SQL Server]The text, ntext, and image data types
    # cannot be compared or sorted, except when using IS NULL or LIKE operator. (306) (SQLExecDirectW)')
    # mssql no order no limit no offset

    with _owned_resource(
        resource, uri, object_location, object_location_schema, timeout, timer
    ) as resource:
        return _query_session_data(
            resource, uri, object_location, object_location_schema, filters, like, fields, sort, limit, offset, timer
        )


# MSSQL cannot compare or sort these legacy types ("The text, ntext, and image data types cannot be compared or
# sorted"), so they are never used in an ORDER BY.
_MSSQL_UNSORTABLE_TYPES = (mssql_dialect.TEXT, mssql_dialect.NTEXT, mssql_dialect.IMAGE)


def _mssql_is_sortable(column: Column) -> bool:
    return not isinstance(column.type, _MSSQL_UNSORTABLE_TYPES)


def _mssql_sort_methods(model: Table, column_dict: Dict[str, Column], sort: List[OrderBy], paginated: bool) -> list:
    """ORDER BY elements for a MSSQL query.

    The requested sort is applied, skipping columns MSSQL cannot sort. When the query is paginated (limit or offset)
    and nothing is left, fall back to the primary key and then to the first sortable column, because OFFSET requires
    an ORDER BY. An empty list means no ORDER BY.
    """
    methods = [
        method
        for item, method in zip(sort, _get_sort_methods(column_dict, sort))
        if _mssql_is_sortable(column_dict[item.field])
    ]
    if methods or not paginated:
        return methods
    pk_columns = list(model.primary_key.columns)
    if pk_columns and all(_mssql_is_sortable(column) for column in pk_columns):
        return pk_columns
    for column in model.columns:
        if _mssql_is_sortable(column):
            return [column]
    return []


def _build_mssql_query(session, model, column_dict, columns, filters, filters_args, sort, limit, offset):
    """Build the MSSQL query. Without limit and offset it is the plain query (ORDER BY only if sort is requested)."""
    paginated = limit is not None or bool(offset)
    order_by = _mssql_sort_methods(model, column_dict, sort, paginated)
    query = (
        session.query(model)
        .filter_by(**filters)
        .filter(*filters_args)
        .order_by(*order_by)
        .with_entities(*[model.c[col.key].label(col.key) for col in columns])
    )
    if order_by:
        if offset:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
    elif limit is not None:
        # Nothing sortable: TOP n (including the rows an offset would skip, see the caller).
        query = query.limit(limit + (offset or 0))
    return query


def _query_session_data(
    resource: OpenResource,
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    filters,
    like,
    fields,
    sort,
    limit,
    offset,
    timer,
):
    """Run the query on a shared resource, closing the session on every path. The engine is not disposed here."""
    engine, model = resource.engine, resource.model
    session_maker = sessionmaker(bind=engine)
    parsed = urlparse(uri)

    column_dict = {column.key: column for column in model.columns}
    columns = _get_columns(column_dict, fields)
    filters_args = []
    like_filters = _process_like_filter(like, model)
    filters, filters_args = _get_filter_operators(filters, filters_args)
    if "oracle" in parsed.scheme:
        filters = _process_filters_oracle_dates(filters)
    filters_args = process_filters_args(filters_args, parsed.scheme, frozenset(column_dict.keys()))
    filters_args.extend(like_filters)

    session = session_maker()

    if parsed.scheme in ["mssql+pyodbc"]:
        mssql_query_started = monotonic()
        try:
            query = _build_mssql_query(
                session, model, column_dict, columns, filters, filters_args, sort, limit, offset
            )
            data = query.all()
            if offset and not _mssql_sort_methods(model, column_dict, sort, paginated=True):
                # No orderable column: OFFSET is impossible, so TOP (limit + offset) was used and
                # the skipped rows are dropped here.
                data = data[offset:]
        except sqlalchemy.exc.ProgrammingError as err:
            raise NoObjectError("Object not available.") from err
        except SortFieldNoExistsError as err:
            logger.warning("Sort Field No Exists Error. - %s ", err)
            raise ValidationError(err.message, ErrorCodes.INVALID_SORT) from err
        finally:
            if timer is not None:
                timer.add("query", monotonic() - mssql_query_started)
            session.close()
    else:
        query_started_at = monotonic()
        try:
            data = (
                session.query(model)
                .filter_by(**filters)
                .filter(*filters_args)
                .order_by(*_get_sort_methods(column_dict, sort))
                .with_entities(*[model.c[col.key].label(col.key) for col in columns])
                .offset(offset)
                .limit(limit)
                .all()
            )
        except sqlalchemy.exc.ProgrammingError as err:
            logger.warning("Object not available. - %s ", err)
            raise ServiceUnavailable(
                "Object not available.", code=ErrorCodes.OBJECT_UNAVAILABLE
            ) from err
        except sqlalchemy.exc.InvalidRequestError as err:
            logger.warning("Invalid Request Error. - %s ", err)
            raise ServiceUnavailable(
                "Invalid Request Error.", code=ErrorCodes.QUERY_ERROR
            ) from err
        except SortFieldNoExistsError as err:
            logger.warning("Sort Field No Exists Error. - %s ", err)
            raise ValidationError(err.message, ErrorCodes.INVALID_SORT) from err
        except sqlalchemy.exc.OperationalError as err:
            # Driver-level failure, which is where a timeout surfaces: oracledb
            # raises DPY-4011/DPY-4024 on call_timeout and SQLAlchemy wraps it here.
            # Logged at ERROR with the object location and elapsed time so the
            # offending resource is identifiable from the logs alone - a timeout
            # kills the request before any access-log line is written, so this is
            # the only record of which resource was responsible.
            logger.error(
                "Resource query failed on %s (object_location=%r, schema=%r) "
                "after %.1fs: %s",
                mask_uri(uri),
                object_location,
                object_location_schema,
                monotonic() - query_started_at,
                err,
            )
            raise ServiceUnavailable(
                "Query error", code=ErrorCodes.QUERY_ERROR
            ) from err
        except Exception as err:
            logger.warning("Problem in resource query: %s", err)
            raise ServiceUnavailable(
                "Query error", code=ErrorCodes.QUERY_ERROR
            ) from err
        finally:
            if timer is not None:
                timer.add("query", monotonic() - query_started_at)
            session.close()

    return data, [column.key for column in columns]


def _process_filters_oracle_dates(filters):
    """Oracle usually expects dates in the format 'dd-Mon-YYYY HH24:MI:SS'. This function processes the filters to
    convert the dates to this format."""
    logger.info("Process filters for oracle dates")

    for key in filters:
        if is_datetime(filters[key]):
            the_date = datetime.fromisoformat(filters[key])
            filters[key] = f"{the_date.strftime('%d-%b-%Y %H:%M:%S')}"
            # filters[key] = f"TO_DATE('{the_date.isoformat()}', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
    return filters


def _get_filter_operators(
    filters: Dict[str, Union[str, dict]], filters_args: list
) -> Tuple[dict, list]:
    """Takes operator clauses from filters. If it has a dict or a list, it adds it to result and removes it from
    filters."""
    changed_filters = []
    for field in filters:
        if isinstance(filters[field], dict) or isinstance(filters[field], list):
            filters_args.append({field: filters[field]})
            changed_filters.append(field)
    for field in changed_filters:
        filters.pop(field)
    return filters, filters_args


def get_resource_data(
    *,
    uri: str,
    object_location: Optional[str],
    object_location_schema: Optional[str],
    filters: Dict[str, Union[str, dict]],
    like: Union[str, Dict],
    fields: List[str],
    sort: List[OrderBy],
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: Optional[int] = None,
    timer: Optional[PhaseTimer] = None,
    resource: Optional[OpenResource] = None,
) -> Iterable[Dict[str, Any]]:
    """Return a iterable of dictionaries with data of resource."""

    data, column_names = _get_session_data_and_columns(
        uri,
        object_location,
        object_location_schema,
        filters,
        like,
        fields,
        sort,
        limit,
        offset,
        timeout,
        timer=timer,
        resource=resource,
    )

    normalize_started = monotonic()

    """ When no typing objects are present, as when executing plain SQL strings, adefault "outputtypehandler" is
    present which will generally return numeric values which specify precision and scale as Python ``Decimal`` objects
    default "outputtypehandler" is present which will generally return numeric values which specify precision and scale
    as Python ``Decimal`` objects.  To disable this coercion to decimal for performance reasons, pass the flag
    ``coerce_to_decimal=False`` to :func:`_sa.create_engine`:: engine = create_engine("oracle+cx_oracle://dsn",
    coerce_to_decimal=False)
    The ``coerce_to_decimal`` flag only impacts the results of plain string SQL statements that are not otherwise
    associated with a :class:`.Numeric` SQLAlchemy type (or a subclass of such).
    .. versionchanged:: 1.2  The numeric handling system for Oracle has been reworked to take advantage of newer
    oracledb features as well as better integration of outputtypehandlers. """

    # Eager normalization to tuples (errors raise here). The conversion is picked per value by its exact Python type
    # (not by the reflected column type, which drivers do not honour: Oracle NUMBER, NullType, views, SQLite).
    # Types without a fast path use the generic logic, so the result is the same for any value.
    dispatch = _NORMALIZERS
    generic = _normalize_value
    rows = []
    append = rows.append
    for item in data:
        append(tuple([dispatch.get(type(value), generic)(value) for value in item]))
    data = rows
    if timer is not None:
        timer.add("normalize", monotonic() - normalize_started)

    return (dict(zip(column_names, row)) for row in data)


def update_resource_size(resource_id, registries, size):
    try:
        resource = ResourceConfig.objects.select_related().get(
            id=resource_id, enabled=True, connector_config__enabled=True
        )
    except ResourceConfig.DoesNotExist as err:
        raise ValidationError(
            "Resource not exists or is not available", ErrorCodes.RESOURCE_UNAVAILABLE
        ) from err

    rsc = ResourceSizeConfig(registries=registries, size=size)
    rsc.resource_id = resource
    rsc.save()


def _get_columns(
    columns_dict: Dict[str, Column], column_names: List[str]
) -> Iterable[Column]:
    """Get SQLAlchemy column instances from column names."""
    if column_names:
        try:
            return [columns_dict[column_name] for column_name in column_names]
        except KeyError as err:
            raise FieldNoExistsError(f"Field: {err.args[0]} not exists.") from err
    else:
        return columns_dict.values()


def _get_sort_methods(column_dict: Dict[str, Column], sort: List[OrderBy]):
    """Create a list of SQLAlchemy column instances that represent query sorting."""
    sort_methods = []
    for item in sort:
        try:
            column = column_dict[item.field]
        except KeyError as err:
            raise SortFieldNoExistsError(
                message=f"Sort field: {err.args[0]} not exists."
            ) from err
        if item.ascending:
            sort_methods.append(column)
        else:
            sort_methods.append(column.desc())

    return sort_methods


def _process_like_filter(args: Union[str, Dict], model: Table) -> list:
    """Create constructor of filter like.

    Accepts either a dict (preferred, from validated _get_like()) or a string
    (legacy fallback). Dict input is processed directly without parsing.
    String input uses ast.literal_eval() instead of eval() to prevent RCE.
    """
    filters = []
    if not args:
        return filters
    if isinstance(args, dict):
        try:
            for key, value in args.items():
                filters.append(model.columns[key].ilike(f"%{value}%"))
        except KeyError as err:
            raise FieldNoExistsError(f"Field: {err.args[0]} not exists.") from err
    else:
        if len(args) != 2:
            try:
                list_args = args.split(",")
                for value in list_args:
                    if re.search(r"[{/}]", (value)):
                        value = re.sub(r"[{/}]", " ", (value))
                    if re.search(r"[:]", (value)):
                        value = re.sub(r"[:]", ",", (value))
                    parsed = ast.literal_eval(value)
                    key = parsed[0]
                    filters.append(model.columns[key].ilike(f"%{parsed[1]}%"))
            except (ValueError, SyntaxError) as err:
                raise ValidationError(
                    f"Invalid like filter format: {err}", ErrorCodes.INVALID_FILTER
                ) from err
            except KeyError as err:
                raise FieldNoExistsError(f"Field: {err.args[0]} not exists.") from err
    return filters


class NetworkConnectionError(Exception):
    """Network connectivity check failed."""


def check_network_connectivity(uri: str, timeout: Optional[int] = None) -> bool:
    """
    Perform a fast TCP connectivity check before attempting database connection.

    Args:
        uri: The database URI to check
        timeout: Timeout in seconds for network check (uses config default if None)

    Returns:
        bool: True if network is reachable, False otherwise

    Raises:
        NetworkConnectionError: If network connectivity fails
    """
    if timeout is None:
        # Get timeout from config if available
        try:
            from gaodcore_project.config import Config

            config = Config.get_config()
            timeout = config.common_config.health_monitoring.network_timeout_seconds
        except (ImportError, AttributeError):
            timeout = 3  # Fallback default

    uri_parsed = urlparse(uri)

    # Handle different URI schemes
    if uri_parsed.scheme in _HTTP_SCHEMAS:
        # For HTTP/HTTPS APIs, the network check is essentially the same as the full check
        # so we skip the network pre-check for APIs
        return True

    if uri_parsed.scheme not in _DATABASE_SCHEMAS:
        # Unknown scheme, skip network check
        return True

    # Extract host and port from database URI
    host = uri_parsed.hostname
    port = uri_parsed.port

    if not host:
        # No host specified (e.g., SQLite), skip network check
        return True

    # Set default ports for database schemes
    if port is None:
        port_map = {
            "postgresql": 5432,
            "mysql": 3306,
            "mssql": 1433,
            "mssql+pyodbc": 1433,
            "oracle+oracledb": 1521,
            "oracle": 1521,
        }
        port = port_map.get(uri_parsed.scheme, 5432)

    try:
        # Attempt TCP connection
        with socket.create_connection((host, port), timeout=timeout):
            logger.debug("Network connectivity check passed for %s:%s", host, port)
            return True
    except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError) as e:
        logger.warning(
            "Network connectivity check failed for %s:%s - %s", host, port, str(e)
        )
        raise NetworkConnectionError(
            f"Network connectivity failed for {host}:{port}: {str(e)}"
        ) from e


def validate_uri(uri: str, timeout: Optional[int] = None) -> None:
    """Validate if URI is available as resource."""
    # Perform network connectivity pre-check for database URIs
    try:
        check_network_connectivity(uri)
    except NetworkConnectionError as err:
        # Network connectivity failed - don't attempt database connection
        logger.warning(
            "Network connectivity check failed for URI: %s - %s", mask_uri(uri), str(err)
        )
        raise DriverConnectionError("Network connectivity failed.") from err

    engine = _get_engine(uri, timeout=timeout)
    try:
        with engine.connect() as _:
            pass
    except sqlalchemy.exc.DatabaseError as err:
        # Log connection failure as warning - expected in health checks
        logger.warning(
            "Connection not available for URI: %s - %s", mask_uri(uri), str(err)
        )
        raise DriverConnectionError("Connection not available.") from err


def _csv_to_dict(data: bytes, charset: str) -> List[Dict[str, Any]]:
    if not charset:
        # charset = cchardet.detect(data)['encoding']
        # pasamos a charset 8 porque sino identifica como gb18030 - o ISO15 que no es capaz de decodificar
        charset = "utf-8"
    print(charset)
    try:
        data = data.decode(charset, "ignore")
    except UnicodeDecodeError:
        data = data.decode("utf-8", "ignore")
    if data:
        dialect = csv.Sniffer().sniff(data)
    else:
        dialect = "excel"

    return list(csv.DictReader(StringIO(data), dialect=dialect))


def _get_table_from_dict(
    data: List[Dict[str, Any]], engine: Engine, meta_data: MetaData
) -> Table:
    fields_to_check = list(data[0].keys())
    fields_checked = []

    field_types = OrderedDict(
        [(field, Column(field, Text)) for field in fields_to_check]
    )
    for row in data:
        for field_to_check in fields_to_check:
            value = row.get(field_to_check)
            if value and field_to_check not in fields_checked:
                field_types[field_to_check] = Column(
                    field_to_check, _SQLALCHEMY_MAP_TYPE[type(value)]
                )
                fields_checked.append(field_to_check)
        for field_checked in fields_checked:
            fields_to_check.remove(field_checked)
        fields_checked = []
        if not fields_to_check:
            break

    table = Table(
        _TEMPORAL_TABLE_NAME, meta_data, *field_types.values(), prefixes=["TEMPORARY"]
    )
    meta_data.create_all(engine)
    return table


def _get_engine_from_api(uri: str, timeout: Optional[int] = None) -> Engine:
    """Fetch a resource served over HTTP(S) and load it into an in-memory SQLite
    engine so the rest of the connector code can treat it like a database.

    Timeout semantics: `timeout` bounds each individual blocking socket
    operation performed by urlopen() - the connect, and separately each read()
    call while downloading the response body - not the total request duration.
    A slow origin that keeps trickling bytes (or a client reading a very large
    body) could still take much longer than `timeout` in wall-clock time. This
    is why `timeout` is never left as None on the ordinary call path (see
    DEFAULT_HTTP_TIMEOUT_SECONDS below): a None timeout means urlopen() blocks
    indefinitely on both connect and each read.
    """
    if timeout is None:
        timeout = DEFAULT_HTTP_TIMEOUT_SECONDS
    try:
        with urllib.request.urlopen(uri, timeout=timeout) as response:
            if response.getcode() == HTTPStatus.OK:
                split_content_type = response.info()["Content-Type"].split(";")
                mime_type = split_content_type[0]
                if len(split_content_type) > 1:
                    charset = split_content_type[1].split("=")[1]
                else:
                    charset = None

                if mime_type in MimeType.CSV.value:
                    data = _csv_to_dict(response.read(), charset)
                elif mime_type in MimeType.JSON.value:
                    data = json.loads(response.read())
                else:
                    raise MimeTypeError()
            else:
                raise DriverConnectionError("The url could not be reached.")
    except HTTPError as err:
        # Raised immediately once the status line/headers are read, before any
        # body bytes are fetched - a non-2xx origin response is rejected without
        # waiting on a slow/large error body.
        logger.warning(
            "HTTP connector received error status %s from %s", err.code, mask_uri(uri)
        )
        err.close()
        raise DriverConnectionError("The url could not be reached.") from err
    except URLError as err:
        # Connection-level failures (DNS, refused, TLS, connect-phase timeout).
        logger.warning("HTTP connector failed to reach %s: %s", mask_uri(uri), err.reason)
        raise DriverConnectionError("The url could not be reached.") from err
    except TimeoutError as err:
        # A 200 response whose body then stalls mid-read raises a bare
        # TimeoutError (aliased to socket.timeout on Python 3.10+), which
        # urllib does NOT wrap as URLError - it must be caught separately.
        logger.warning(
            "HTTP connector timed out after %ss while reading response: %s",
            timeout,
            mask_uri(uri),
        )
        raise DriverConnectionError("The url could not be reached.") from err
    if data:
        max_key = max(data, key=len).keys()
        for item in data:
            if len(max_key) > len(item.keys()):
                for k in max_key:
                    item.setdefault(k, None)
    engine = create_engine("sqlite:///:memory:", echo=False, future=True)
    metadata = MetaData()
    if data:
        table = _get_table_from_dict(data, engine, metadata)
    else:
        table = Table(
            _TEMPORAL_TABLE_NAME,
            metadata,
            Column("id", Integer, primary_key=True),
            prefixes=["TEMPORARY"],
        )
        metadata.create_all(engine)
    if data:
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        session.execute(table.insert(), data)
        session.commit()
        session.close()
    return engine


# Global flag to track Oracle client initialization
_oracle_client_initialized = False


def _get_engine(uri: str, timeout: Optional[int] = None) -> Engine:
    global _oracle_client_initialized
    uri_parsed = urlparse(uri)

    # Handle Oracle URI conversion from oracle:// to oracle+oracledb://
    if uri_parsed.scheme == "oracle":
        # Convert oracle:// to oracle+oracledb:// for the new oracledb package
        uri = uri.replace("oracle://", "oracle+oracledb://", 1)
        uri_parsed = urlparse(uri)

    # Handle MSSQL URI conversion from mssql:// to mssql+pyodbc://
    if uri_parsed.scheme == "mssql":
        # Convert mssql:// to mssql+pyodbc:// for pyodbc driver compatibility
        uri = uri.replace("mssql://", "mssql+pyodbc://", 1)
        uri_parsed = urlparse(uri)

    if uri_parsed.scheme in _DATABASE_SCHEMAS:
        # Special handling for Oracle to enable thick mode for older password verifiers
        if uri_parsed.scheme == "oracle+oracledb" and not _oracle_client_initialized:
            try:
                import oracledb

                # Initialize thick mode for compatibility with older Oracle versions
                oracledb.init_oracle_client()
                _oracle_client_initialized = True
                logger.info("Oracle thick mode initialized successfully")
            except Exception as e:
                logger.warning(f"Could not initialize Oracle thick mode: {e}")
                # Fall back to thin mode (default behavior)

        # Configure database-specific connection timeout parameters.
        # connect_timeout only governs TCP handshake + auth, not query execution time,
        # so a short default is safe even for long-running queries.
        _DEFAULT_CONNECT_TIMEOUT = 10
        connect_timeout = timeout if timeout is not None else _DEFAULT_CONNECT_TIMEOUT
        connect_args = {}
        engine_kwargs = {}
        oracle_call_timeout = None
        if uri_parsed.scheme in ["postgresql", "mysql"]:
            connect_args["connect_timeout"] = connect_timeout
        elif uri_parsed.scheme in ["mssql+pyodbc", "mssql"]:
            connect_args["timeout"] = connect_timeout
        elif uri_parsed.scheme == "oracle+oracledb":
            # Bound both phases explicitly. A previous version of this branch
            # deferred to "TNS/freetds level" configuration, but no sqlnet.ora or
            # TNS_ADMIN exists in this deployment (the freetds.conf in the
            # Dockerfile governs MSSQL only), so Oracle ran with no timeout at all.
            connect_args["tcp_connect_timeout"] = connect_timeout
            # Validate pooled connections before use: a connection dropped by the
            # server is otherwise only discovered mid-query, surfacing as the
            # DPY-1001 "not connected to database" errors seen during worker teardown.
            engine_kwargs["pool_pre_ping"] = True
            oracle_call_timeout = ORACLE_CALL_TIMEOUT_SECONDS * 1000
            # The oracledb default (100 rows per round trip) makes large tables
            # cost thousands of network trips; the dialect applies this value to
            # every cursor it creates. Read lazily to avoid importing settings
            # at module import time.
            from django.conf import settings

            engine_kwargs["arraysize"] = getattr(
                settings, "ORACLE_ARRAYSIZE", DEFAULT_ORACLE_ARRAYSIZE
            )
        # sqlite: file operations are local, no network timeout needed

        engine = create_engine(
            uri, max_identifier_length=128, connect_args=connect_args, **engine_kwargs
        )
        if oracle_call_timeout is not None:
            # call_timeout is a Connection attribute in oracledb, not a connect()
            # argument (passing it through connect_args raises TypeError), so it is
            # applied to each pooled connection as it is opened. Milliseconds.
            @event.listens_for(engine, "connect")
            def _set_oracle_call_timeout(dbapi_connection, _connection_record):
                dbapi_connection.call_timeout = oracle_call_timeout

        return engine
    if uri_parsed.scheme in _HTTP_SCHEMAS:
        return _get_engine_from_api(uri, timeout=timeout)
    raise NotImplementedSchemaError(
        f'Schema: "{uri_parsed.scheme}" is not implemented.'
    )
