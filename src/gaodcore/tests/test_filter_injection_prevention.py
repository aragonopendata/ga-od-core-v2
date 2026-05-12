import re
import pytest
from datetime import date
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import TextClause

from gaodcore.operators import (
    process_filters_args,
    _build_bind_clause,
)
from rest_framework.exceptions import ValidationError


CAR_COLUMNS = frozenset({"id", "name", "brand", "year", "price", "purchase_date"})


class TestValueInjection:
    def test_sql_metacharacters_in_string_value_bound_safely(self):
        malicious = "' OR 1=1 --"
        clause = _build_bind_clause("name", "=", malicious, "")
        assert isinstance(clause, TextClause)
        assert ":val_" in clause.text
        assert malicious not in clause.text
        assert "OR 1=1" not in clause.text

    def test_sql_metacharacters_in_gt_value_bound_safely(self):
        malicious = "'; DROP TABLE users;--"
        clause = _build_bind_clause("name", ">", malicious, "")
        assert isinstance(clause, TextClause)
        assert ":val_" in clause.text
        assert malicious not in clause.text

    def test_numeric_value_bound_safely(self):
        clause = _build_bind_clause("id", ">", 1, "")
        assert isinstance(clause, TextClause)
        assert re.match(r"id > :val_[0-9a-f]+", clause.text)

    def test_date_value_bound_safely(self):
        clause = _build_bind_clause("purchase_date", ">", date(2020, 1, 1), "")
        assert isinstance(clause, TextClause)
        assert ":val_" in clause.text

    def test_oracle_datetime_value_uses_bindparam(self):
        dt_str = "2020-07-13T00:00:00"
        clause = _build_bind_clause("purchase_date", ">", dt_str, "oracle+oracledb")
        assert isinstance(clause, TextClause)
        assert re.search(r"TO_DATE\(:val_[0-9a-f]+", clause.text)
        assert dt_str not in clause.text


class TestFieldNameWhitelisting:
    def test_unknown_field_raises_validation_error(self):
        with pytest.raises(ValidationError, match="Unknown field"):
            process_filters_args(
                [{"nonexistent": {"$gt": 1}}],
                column_names=CAR_COLUMNS,
            )

    def test_sql_injection_in_field_name_raises_validation_error(self):
        with pytest.raises(ValidationError, match="Unknown field"):
            process_filters_args(
                [{"1=1; DROP TABLE users--": {"$gt": 1}}],
                column_names=CAR_COLUMNS,
            )

    def test_valid_field_passes_validation(self):
        result = process_filters_args(
            [{"year": {"$gt": 2020}}],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 1
        assert isinstance(result[0], TextClause)

    def test_empty_column_names_skips_validation(self):
        result = process_filters_args(
            [{"anything": {"$gt": 1}}],
            column_names=frozenset(),
        )
        assert len(result) == 1

    def test_default_column_names_skips_validation(self):
        result = process_filters_args(
            [{"anything": {"$gt": 1}}],
        )
        assert len(result) == 1


class TestBindParameterUniqueness:
    def test_multiple_same_operator_no_collision(self):
        result = process_filters_args(
            [
                {"year": {"$gt": 2018}},
                {"year": {"$gt": 2020}},
            ],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 2
        texts = [r.text for r in result]
        bind_names = set()
        for t in texts:
            parts = t.split(":val_")
            for part in parts[1:]:
                name = part.split()[0] if " " in part else part
                bind_names.add(f"val_{name}")
        assert len(bind_names) == 2, f"Expected 2 unique bind names, got {bind_names}"

    def test_multiple_filters_in_one_dict_no_collision(self):
        result = process_filters_args(
            [{"year": {"$gt": 2018}, "price": {"$lt": 30000}}],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 2
        texts = [r.text for r in result]
        bind_names = set()
        for t in texts:
            parts = t.split(":val_")
            for part in parts[1:]:
                name = part.split()[0] if " " in part else part
                bind_names.add(f"val_{name}")
        assert len(bind_names) == 2, f"Expected 2 unique bind names, got {bind_names}"


class TestNotFilterPropagation:
    def test_not_filter_validates_field_names(self):
        result = process_filters_args(
            [{"$not": {"brand": {"$eq": "Tesla"}}}],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 1
        clause = result[0]
        assert "NOT" in str(clause)

    def test_not_filter_rejects_unknown_field(self):
        with pytest.raises(ValidationError, match="Unknown field"):
            process_filters_args(
                [{"$not": {"nonexistent": {"$eq": "test"}}}],
                column_names=CAR_COLUMNS,
            )


class TestAndOrFilterPropagation:
    def test_and_filter_validates_all_nested_fields(self):
        result = process_filters_args(
            [{"$and": [{"brand": {"$eq": "Tesla"}}, {"year": {"$eq": 2020}}]}],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 1
        combined = str(result[0])
        assert "AND" in combined

    def test_or_filter_validates_all_nested_fields(self):
        result = process_filters_args(
            [{"$or": [{"brand": {"$eq": "Tesla"}}, {"brand": {"$eq": "Opel"}}]}],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 1
        combined = str(result[0])
        assert "OR" in combined

    def test_and_filter_rejects_unknown_field(self):
        with pytest.raises(ValidationError, match="Unknown field"):
            process_filters_args(
                [{"$and": [{"brand": {"$eq": "Tesla"}}, {"hack": {"$eq": 1}}]}],
                column_names=CAR_COLUMNS,
            )

    def test_or_filter_rejects_unknown_field(self):
        with pytest.raises(ValidationError, match="Unknown field"):
            process_filters_args(
                [{"$or": [{"hack": {"$eq": 1}}]}],
                column_names=CAR_COLUMNS,
            )

    def test_nested_and_or_validates_all_fields(self):
        result = process_filters_args(
            [
                {
                    "$and": [
                        {"$or": [{"brand": {"$eq": "Tesla"}}, {"brand": {"$eq": "Opel"}}]},
                        {"$or": [{"year": {"$eq": 2020}}, {"year": {"$eq": 2019}}]},
                    ]
                }
            ],
            column_names=CAR_COLUMNS,
        )
        assert len(result) == 1


class TestSqliteIntegration:
    @pytest.fixture
    def sqlite_session(self):
        engine = create_engine("sqlite:///:memory:", echo=False, future=True)
        metadata = MetaData()
        table = Table(
            "cars",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("brand", String),
            Column("year", Integer),
            Column("price", Integer),
            Column("purchase_date", String),
        )
        metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute(
            insert(table),
            [
                {"name": "Model S", "brand": "Tesla", "year": 2020, "price": 34000, "purchase_date": "2020-07-13"},
                {"name": "Model 3", "brand": "Tesla", "year": 2021, "price": 40000, "purchase_date": "2021-04-28"},
                {"name": "Corsa", "brand": "Opel", "year": 2019, "price": 20000, "purchase_date": "2019-02-21"},
            ],
        )
        session.commit()
        column_names = frozenset({"id", "name", "brand", "year", "price", "purchase_date"})
        yield session, table, column_names
        session.close()
        engine.dispose()

    def test_gt_filter_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"year": {"$gt": 2019}}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 2
        assert {r.name for r in rows} == {"Model S", "Model 3"}

    def test_eq_filter_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"brand": {"$eq": "Tesla"}}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 2
        assert {r.name for r in rows} == {"Model S", "Model 3"}

    def test_and_filter_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"$and": [{"brand": {"$eq": "Tesla"}}, {"year": {"$eq": 2020}}]}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 1
        assert rows[0].name == "Model S"

    def test_or_filter_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"$or": [{"brand": {"$eq": "Tesla"}}, {"brand": {"$eq": "Opel"}}]}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 3

    def test_not_filter_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"$not": {"brand": {"$eq": "Tesla"}}}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 1
        assert rows[0].name == "Corsa"

    def test_string_value_with_special_chars_bound_safely(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"brand": {"$eq": "Tesla'; DROP TABLE cars;--"}}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 0

        all_rows = session.query(table).all()
        assert len(all_rows) == 3

    def test_combined_gt_lt_returns_correct_rows(self, sqlite_session):
        session, table, column_names = sqlite_session
        clauses = process_filters_args(
            [{"year": {"$gt": 2019}, "price": {"$lt": 35000}}],
            column_names=column_names,
        )
        rows = session.query(table).filter(*clauses).all()
        assert len(rows) == 1
        assert rows[0].name == "Model S"
