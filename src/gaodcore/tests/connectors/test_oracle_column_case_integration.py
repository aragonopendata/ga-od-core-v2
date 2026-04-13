"""Integration tests for Oracle column name case preservation with real Oracle DB.

Uses testcontainers to spin up an Oracle XE container and verifies that column names
flow correctly through the entire pipeline:
  DDL -> all_tab_columns -> _create_table_from_oracle_system_views -> get_resource_columns -> get_resource_data

Covers all 5 normalization scenarios from SQLALCHEMY_NAME_NORMALIZATION_EXPLAINED.md:
1. Unquoted ASCII uppercase (PRODUCTID)
2. Unquoted uppercase with special chars (AÑO)
3. Quoted with spaces (CONTRACT STATUS)
4. Quoted lowercase ("order_id")
5. Quoted mixed case ("UserId")

Requires Docker to run.

Oracle fixtures (oracle_container, oracle_engine, oracle_uri, setup_oracle_tables)
are defined in the shared conftest.py alongside the PostgreSQL fixtures.
"""

from sqlalchemy import MetaData

from connectors import (
    _create_table_from_oracle_system_views,
    get_resource_columns,
    get_resource_data,
)

from .conftest import ORACLE_TEST_TABLE, ORACLE_SCHEMA


EXPECTED_COLUMN_NAMES = {
    "PRODUCTID",
    "AÑO",
    "CONTRACT STATUS",
    "order_id",
    "UserId",
}


class TestOracleColumnCaseIntegration:
    """Integration tests using real Oracle XE container.

    Tests the ALL_TAB_COLUMNS fallback path that is always used for Oracle
    to preserve exact column casing from the database.

    Note: We force Oracle to use the fallback path (_create_table_from_oracle_system_views)
    instead of standard SQLAlchemy reflection because standard reflection normalizes
    table names to lowercase, which causes issues with case-sensitive Oracle identifiers.
    """

    def test_create_table_preserves_column_keys(self, oracle_engine, setup_oracle_tables):
        metadata = MetaData()
        table = _create_table_from_oracle_system_views(
            engine=oracle_engine,
            object_location=ORACLE_TEST_TABLE,
            object_location_schema=ORACLE_SCHEMA,
            meta_data=metadata,
        )

        column_keys = {col.key for col in table.columns}
        assert column_keys == EXPECTED_COLUMN_NAMES

    def test_create_table_column_names_match_oracle(self, oracle_engine, setup_oracle_tables):
        metadata = MetaData()
        table = _create_table_from_oracle_system_views(
            engine=oracle_engine,
            object_location=ORACLE_TEST_TABLE,
            object_location_schema=ORACLE_SCHEMA,
            meta_data=metadata,
        )

        column_names = {col.name for col in table.columns}
        assert column_names == EXPECTED_COLUMN_NAMES

    def test_get_resource_columns_preserves_case(self, oracle_uri, setup_oracle_tables):
        columns = get_resource_columns(
            uri=oracle_uri,
            object_location=ORACLE_TEST_TABLE,
            object_location_schema=ORACLE_SCHEMA,
        )

        column_names = {col["COLUMN_NAME"] for col in columns}
        assert column_names == EXPECTED_COLUMN_NAMES

    def test_get_resource_columns_count(self, oracle_uri, setup_oracle_tables):
        columns = get_resource_columns(
            uri=oracle_uri,
            object_location=ORACLE_TEST_TABLE,
            object_location_schema=ORACLE_SCHEMA,
        )

        assert len(list(columns)) == 5

    def test_get_resource_data_keys_match_oracle(self, oracle_uri, setup_oracle_tables):
        rows = list(
            get_resource_data(
                uri=oracle_uri,
                object_location=ORACLE_TEST_TABLE,
                object_location_schema=ORACLE_SCHEMA,
                filters={},
                like="",
                fields=[],
                sort=[],
                limit=10,
            )
        )

        assert len(rows) >= 1
        row_keys = set(rows[0].keys())
        assert row_keys == EXPECTED_COLUMN_NAMES

    def test_get_resource_data_values_accessible_with_exact_case(self, oracle_uri, setup_oracle_tables):
        rows = list(
            get_resource_data(
                uri=oracle_uri,
                object_location=ORACLE_TEST_TABLE,
                object_location_schema=ORACLE_SCHEMA,
                filters={},
                like="",
                fields=[],
                sort=[],
                limit=10,
            )
        )

        row = rows[0]

        assert row["PRODUCTID"] == 1
        assert row["AÑO"] == 2024
        assert row["CONTRACT STATUS"] == "Active"
        assert row["order_id"] == 100
        assert row["UserId"] == "user_001"

    def test_no_lowercase_column_keys_in_created_table(self, oracle_engine, setup_oracle_tables):
        metadata = MetaData()
        table = _create_table_from_oracle_system_views(
            engine=oracle_engine,
            object_location=ORACLE_TEST_TABLE,
            object_location_schema=ORACLE_SCHEMA,
            meta_data=metadata,
        )

        column_keys = [col.key for col in table.columns]

        assert "productid" not in column_keys
        assert "año" not in column_keys
        assert "contract status" not in column_keys
        assert "userid" not in column_keys
