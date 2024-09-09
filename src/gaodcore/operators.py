from datetime import date
from typing import Callable

from rest_framework.exceptions import ValidationError
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def get_function_for_operator(operator: str) -> Callable:
    """ Return the operator function based on the filter type. """
    filter_operators = {
        "$gt": filter_gt,
        "$lt": filter_lt,
        "$eq": filter_eq,
        "$ne": filter_ne,
        "$gte": filter_gte,
        "$lte": filter_lte
    }
    result = filter_operators.get(operator)
    if result is None:
        logger.warning(f"Operator {operator} not implemented")
        raise ValidationError(f"Operator {operator} not implemented")
    return result


def format_type(value):
    if isinstance(value, str):
        value = f"'{value}'"
    elif isinstance(value, date):
        value = f"'{value.isoformat()}'"
    return value


def filter_gt(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gt"]
    value = format_type(value)

    return text(f"{field} > {value}")


def filter_lt(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lt"]
    value = format_type(value)

    return text(f"{field} < {value}")


def filter_eq(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$eq"]
    value = format_type(value)

    return text(f"{field} = {value}")


def filter_ne(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$ne"]
    value = format_type(value)

    return text(f"{field} != {value}")


def filter_gte(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gte"]
    value = format_type(value)

    return text(f"{field} >= {value}")


def filter_lte(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lte"]
    value = format_type(value)

    return text(f"{field} <= {value}")
