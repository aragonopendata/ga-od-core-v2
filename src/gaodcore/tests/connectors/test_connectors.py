from collections.abc import Iterable
from datetime import date

import pytest
from rest_framework.exceptions import ValidationError

from connectors import get_resource_data, _get_filter_operators

from .conftest import Car
from gaodcore.operators import process_filters_args


@pytest.fixture
def car_table():
    Car.create_table()
    Car.add_car("Model S", "Tesla", 2020, 34000,
                date(2020, 7, 13))
    Car.add_car("Model 3", "Tesla", 2021, 40000,
                date(2021, 4, 28))
    Car.add_car("Corsa", "Opel", 2019, 20000,
                date(2019, 2, 21))
    Car.add_car("Astra", "Opel", 2018, 25000,
                date(2018, 11, 15))
    Car.add_car("Clio", "Renault", 2017, 15000,
                date(2017, 8, 10))
    yield
    Car.delete_all()


def get_resource_with_filters(configs, filters):
    return list(get_resource_data(
        uri=configs["uri"],
        object_location=configs["location"],
        object_location_schema=configs["schema"],
        filters=filters,
        like='',
        fields=[],
        sort=[],
        limit=None,
    ))


def try_filters(configs: dict, filters: dict, expected_number: int, expected_names: Iterable[str]):
    result = get_resource_with_filters(configs, filters)
    assert len(result) == expected_number
    for r in result:
        assert r["name"] in expected_names


@pytest.mark.django_db
def test_create_table(configs, car_table):
    test_filters = {}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 5


@pytest.mark.django_db
def test_get_resource_invalid_filter_fails(configs, car_table):
    test_filters = {'year': {"$invalid": 2020}}

    with pytest.raises(ValidationError):
        get_resource_with_filters(configs, test_filters)


@pytest.mark.django_db
def test_get_resource_str_filter(configs, car_table):
    test_filters = {'name': "Model S"}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Model S"


@pytest.mark.django_db
def test_get_resource_gt_int(configs, car_table):
    test_filters = {'year': {"$gt": 2020}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Model 3"


@pytest.mark.django_db
def test_get_resource_gte_int(configs, car_table):
    test_filters = {'year': {"$gte": 2020}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 2
    for r in result:
        assert r["name"] in ["Model 3", "Model S"]


@pytest.mark.django_db
def test_get_resource_lt_int(configs, car_table):
    test_filters = {'year': {"$lt": 2018}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Clio"


@pytest.mark.django_db
def test_get_resource_lte_int(configs, car_table):
    test_filters = {'year': {"$lte": 2018}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 2
    for r in result:
        assert r["name"] in ["Clio", "Astra"]


@pytest.mark.django_db
def test_get_resource_gt_date(configs, car_table):
    test_filters = {'purchase_date': {"$gt": date(2020, 12, 1)}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Model 3"


@pytest.mark.django_db
def test_get_resource_ne_int(configs, car_table):
    test_filters = {'year': {"$ne": 2019}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 4
    expected_names = ["Model S", "Model 3", "Clio", "Astra"]
    for r in result:
        assert r["name"] in expected_names


@pytest.mark.django_db
def test_get_resource_eq_int(configs, car_table):
    test_filters = {'year': {"$eq": 2019}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Corsa"


@pytest.mark.django_db
def test_get_resource_eq_str(configs, car_table):
    test_filters = {'name': {"$eq": "Model S"}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Model S"


@pytest.mark.django_db
def test_get_resource_eq_date(configs, car_table):
    test_filters = {'purchase_date': {"$eq": date(2020, 7, 13)}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Model S"


@pytest.mark.django_db
def test_get_resource_gt_lt_int(configs, car_table):
    test_filters = {'year': {"$gt": 2018}, 'price': {"$lt": 30000}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Corsa"


@pytest.mark.django_db
def test_get_resource_gt_and_lt_int(configs, car_table):
    test_filters = {'$and': [{'year': {"$gt": 2018}}, {'price': {"$lt": 30000}}]}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    assert result[0]["name"] == "Corsa"


@pytest.mark.django_db
def test_get_resource_or(configs, car_table):
    test_filters = {"$or": [{'name': {"$eq": "Model 3"}}, {'name': {"$eq": "Model S"}}]}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 2
    for r in result:
        assert r["name"] in ["Model 3", "Model S"]


@pytest.mark.parametrize("filters, expected", [
    ({'year': {"$lt": 2018}}, ({}, [{'year': {"$lt": 2018}}])),
    ({'year': {"$lte": 2018}}, ({}, [{'year': {"$lte": 2018}}])),
    ({'year': {"$gt": 2018}}, ({}, [{'year': {"$gt": 2018}}])),
    ({'year': {"$gte": 2018}}, ({}, [{'year': {"$gte": 2018}}])),
    ({'year': {"$eq": 2018}}, ({}, [{'year': {"$eq": 2018}}])),
    ({'year': {"$ne": 2018}}, ({}, [{'year': {"$ne": 2018}}])),
    ({'year': {"$gt": 2018, "$lt": 2020}}, ({}, [{'year': {"$gt": 2018, "$lt": 2020}}])),
])
def test_get_filter_operators(filters, expected):
    filters_args = []
    result_filters, result_filters_args = _get_filter_operators(filters, filters_args)
    assert result_filters == expected[0]
    assert len(result_filters_args) == len(expected[1])
    for fargs in result_filters_args:
        assert fargs in expected[1]


@pytest.mark.parametrize("filters, filters_args, expected", [
    ({"id": {"$gt": 1}}, [], ({}, [{"id": {"$gt": 1}}])),
    ({"id": {"$gt": 1}, "name": 3}, [], ({"name": 3}, [{"id": {"$gt": 1}}])),
    ({"id": {"$gt": 1}, "name": 3}, [{"age": {"$lt": "20"}}],
     ({"name": 3}, [{"age": {"$lt": "20"}}, {"id": {"$gt": 1}}])),
    ({"$and": [{"age": 1}, {"name": "john"}]}, [], ({}, [{"$and": [{"age": 1}, {"name": "john"}]}])),
])
def test_get_filter_operators2(filters, filters_args, expected):
    result_filters, result_filters_args = _get_filter_operators(filters, filters_args)
    assert result_filters == expected[0]
    assert result_filters_args == expected[1]


@pytest.mark.parametrize("filters_args, expected", [
    ([], []),
    ([{"id": {"$gt": 1}}], ["id > 1"]),
    ([{"id": {"$gt": 1}, "name": {"$gt": "john"}}], ["id > 1", "name > 'john'"]),
    ([{"$and": [{"id": {"$gt": 1}}, {"name": {"$gt": "john"}}]}], ["id > 1 AND name > 'john'"]),
    ([{"$or": [{"id": {"$gt": 1}}, {"name": {"$gt": "john"}}]}], ["id > 1 OR name > 'john'"]),
])
def test_process_filters_args(filters_args, expected):
    result = process_filters_args(filters_args)
    result = [str(r) for r in result]
    assert result == expected

@pytest.mark.django_db
def test_get_resource_and_simple_conditions(configs, car_table):
    test_filters = {"$and": [{'brand': {"$eq": "Tesla"}}, {'year': {"$eq": 2020}}]}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 1
    for r in result:
        assert r["name"] in ["Model S"]

@pytest.mark.this
@pytest.mark.django_db
def test_get_resource_not_simple(configs, car_table):
    test_filters = {"$not": {'brand': {"$eq": "Tesla"}}}
    result = get_resource_with_filters(configs, test_filters)

    assert len(result) == 3
    for r in result:
        assert r["name"] in ["Corsa", "Astra", "Clio"]