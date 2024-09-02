from typing import Callable

from rest_framework.exceptions import ValidationError
from sqlalchemy import text


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
    result =  filter_operators.get(operator)
    if result is None:
        raise ValidationError(f"Operator {operator} not implemented")
    return result


def filter_gt(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gt"]
    return text(f"{field} > {value}")

def filter_lt(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lt"]
    return text(f"{field} < {value}")

def filter_eq(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$eq"]
    return text(f"{field} = {value}")

def filter_ne(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$ne"]
    return text(f"{field} != {value}")

def filter_gte(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gte"]
    return text(f"{field} >= {value}")

def filter_lte(field: str, filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lte"]
    return text(f"{field} <= {value}")

