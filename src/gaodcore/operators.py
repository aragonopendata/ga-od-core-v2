from datetime import date, datetime
import uuid
from typing import Callable, Any

from rest_framework.exceptions import ValidationError
from sqlalchemy import text, not_, and_, or_
import logging

logger = logging.getLogger(__name__)

def _next_bind_name() -> str:
    return f"val_{uuid.uuid4().hex[:8]}"


def is_datetime(value):
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def _validate_field(field: str, column_names: frozenset):
    if column_names and field not in column_names:
        raise ValidationError(f"Unknown field: {field}")


def process_filters_args(filters: list[dict], scheme: str = "", column_names: frozenset = frozenset()) -> list:
    result = []
    logger.info("Processing filters: %s", filters)
    for filter in filters:
        for key, value in filter.items():
            if isinstance(value, dict):
                result.extend(process_dict_filter(key, value, scheme, column_names))
            elif isinstance(value, list):
                result.append(process_list_filter(key, value, column_names))
            else:
                result.append(process_simple_filter(key, value))
    return result


def process_dict_filter(key: str, value: dict, schema: str, column_names: frozenset = frozenset()) -> list:
    result = []
    if key == "$not":
        not_function = get_function_for_operator(key)
        result.append(not_function(value, column_names=column_names))
    else:
        _validate_field(key, column_names)
        for field, field_value in value.items():
            filter_function = get_function_for_operator(field)
            result.append(filter_function(key, {field: field_value}, schema, column_names=column_names))
    return result


def process_list_filter(key: str, value: list, column_names: frozenset = frozenset()) -> text:
    clause_list = []
    for item in value:
        clause_list.extend(process_filters_args([item], column_names=column_names))
    if key == "$and":
        return and_(*clause_list)
    elif key == "$or":
        return or_(*clause_list)
    else:
        logger.warning("Filter not valid: %s", {key: value})
        raise ValidationError("Filter not valid: %s" % {key: value})


def process_simple_filter(key: str, value: Any) -> text:
    logger.warning("Filter not valid: %s", {key: value})
    raise ValidationError("Filter not valid: %s" % {key: value})


def get_function_for_operator(operator: str) -> Callable:
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


def _build_bind_clause(field: str, op: str, value, schema: str) -> text:
    bind_name = _next_bind_name()
    if isinstance(value, str) and "oracle" in schema and is_datetime(value):
        the_date = datetime.fromisoformat(value)
        return text(
            f"{field} {op} TO_DATE(:{bind_name}, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        ).bindparams(**{bind_name: the_date.isoformat()})
    elif isinstance(value, date) and "oracle" in schema:
        return text(
            f"{field} {op} TO_DATE(:{bind_name}, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        ).bindparams(**{bind_name: value.isoformat()})
    elif isinstance(value, date):
        return text(f"{field} {op} :{bind_name}").bindparams(
            **{bind_name: value.isoformat()}
        )
    else:
        return text(f"{field} {op} :{bind_name}").bindparams(**{bind_name: value})


def filter_gt(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, ">", filter["$gt"], schema)


def filter_lt(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, "<", filter["$lt"], schema)


def filter_eq(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, "=", filter["$eq"], schema)


def filter_ne(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, "!=", filter["$ne"], schema)


def filter_gte(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, ">=", filter["$gte"], schema)


def filter_lte(field: str, filter: dict, schema: str, column_names: frozenset = frozenset()) -> text:
    return _build_bind_clause(field, "<=", filter["$lte"], schema)


def filter_not(filter: dict, column_names: frozenset = frozenset()) -> text:
    clauses = process_filters_args([filter], column_names=column_names)
    if len(clauses) == 1:
        return not_(clauses[0])
    return not_(and_(*clauses))
