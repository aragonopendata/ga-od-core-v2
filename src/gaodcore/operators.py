from datetime import date, datetime
from typing import Callable, Any

from rest_framework.exceptions import ValidationError
from sqlalchemy import text, not_, and_, or_
import logging

logger = logging.getLogger(__name__)

def is_datetime(value):
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False

# def process_filters_args(filters: list[dict]) -> list:
#     """Process filters and return a list of SQLAlchemy clauses."""
#     result = []
#     logger.info("Processing filters: %s", filters)
#     for filter in filters:
#         for key in filter:
#             if isinstance(filter[key], dict):
#                 if key == "$not":
#                     not_function = get_function_for_operator(key)
#                     result.append(not_function(filter[key]))
#                 else:
#                     for field in filter[key]:
#                         filter_function = get_function_for_operator(field)
#                         result.append(filter_function(key, filter[key]))
#             elif isinstance(filter[key], list):
#                 clause_list = []
#                 for item in filter[key]:
#                     clause = process_filters_args([item])
#                     clause_list.extend(clause)
#                 if key == "$and":
#                     result.append(and_(*clause_list))
#                 elif key == "$or":
#                     result.append(or_(*clause_list))
#                 else:
#                     logger.warning("Filter not valid: %s", filter)
#                     raise ValidationError("Filter not valid: %s", filter)
#
#             else:
#                 logger.warning("Filter not valid: %s", filter)
#                 raise ValidationError("Filter not valid")
#
#     return result


def process_filters_args(filters: list[dict], scheme: str ="") -> list:
    """Process filters and return a list of SQLAlchemy clauses."""
    result = []
    logger.info("Processing filters: %s", filters)
    for filter in filters:
        for key, value in filter.items():
            if isinstance(value, dict):
                result.extend(process_dict_filter(key, value, scheme))
            elif isinstance(value, list):
                result.append(process_list_filter(key, value))
            else:
                result.append(process_simple_filter(key, value))
    return result


def process_dict_filter(key: str, value: dict, schema: str) -> list:
    """Process dictionary filters."""
    result = []
    if key == "$not":
        not_function = get_function_for_operator(key)
        result.append(not_function(value))
    else:
        for field, field_value in value.items():
            filter_function = get_function_for_operator(field)
            result.append(filter_function(key, {field: field_value}, schema))
    return result


def process_list_filter(key: str, value: list) -> text:
    """Process list filters."""
    clause_list = []
    for item in value:
        clause_list.extend(process_filters_args([item]))
    if key == "$and":
        return and_(*clause_list)
    elif key == "$or":
        return or_(*clause_list)
    else:
        logger.warning("Filter not valid: %s", {key: value})
        raise ValidationError("Filter not valid: %s" % {key: value})


def process_simple_filter(key: str, value: Any) -> text:
    """Process simple filters."""
    logger.warning("Filter not valid: %s", {key: value})
    raise ValidationError("Filter not valid: %s" % {key: value})


def get_function_for_operator(operator: str) -> Callable:
    """ Return the operator function based on the filter type. """
    filter_operators = {
        "$gt": filter_gt,
        "$lt": filter_lt,
        "$eq": filter_eq,
        "$ne": filter_ne,
        "$gte": filter_gte,
        "$lte": filter_lte,
        "$not": filter_not,
    }
    result = filter_operators.get(operator)
    if result is None:
        logger.warning(f"Operator {operator} not implemented")
        raise ValidationError(f"Operator {operator} not implemented")
    return result


def format_type(value, schema: str = "") -> str:
    if isinstance(value, str):
        if "oracle" in schema and is_datetime(value):
            the_date = datetime.fromisoformat(value)
            value = f"TO_DATE('{the_date.isoformat()}', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        else:
            value = f"'{value}'"
    elif isinstance(value, date):
        if "oracle" in schema:
            value = f"TO_DATE('{value.isoformat()}', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        else:
            value = f"'{value.isoformat()}'"
    return value

def filter_gt(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gt"]

    value = format_type(value, schema)

    return text(f"{field} > {value}")


def filter_lt(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lt"]
    value = format_type(value, schema)

    return text(f"{field} < {value}")


def filter_eq(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$eq"]
    value = format_type(value, schema)

    return text(f"{field} = {value}")


def filter_ne(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$ne"]
    value = format_type(value, schema)

    return text(f"{field} != {value}")


def filter_gte(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$gte"]
    value = format_type(value, schema)

    return text(f"{field} >= {value}")


def filter_lte(field: str, filter: dict, schema: str) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """
    value = filter["$lte"]
    value = format_type(value, schema)

    return text(f"{field} <= {value}")


def filter_not(filter: dict) -> text:
    """Translate a filter string to a SQL clause.
        @param field: Field name
        @param filter: Filter dictionary
        @return: SQL clause
    """

    clauses = process_filters_args([filter])
    return not_(*clauses)
