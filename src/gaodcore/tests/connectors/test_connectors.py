from collections.abc import Iterable
from datetime import date

import pytest
from rest_framework.exceptions import ValidationError

from connectors import get_resource_data
from .conftest import Car


@pytest.fixture
def car_table():
    Car.create_table()
    Car.add_car("Model S", "Tesla", 2020, 34000,
                date(2020, 7, 13))
    Car.add_car("Model 3", "Tesla", 2021,   40000,
                date(2021, 4, 28))
    Car.add_car("Corsa", "Opel", 2019,  20000,
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