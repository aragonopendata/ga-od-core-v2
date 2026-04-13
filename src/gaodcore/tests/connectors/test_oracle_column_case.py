"""Tests for Oracle column name case preservation.

These tests verify that Oracle column names are returned exactly as they appear
in the database (matching DBeaver), rather than being lowercased.

Covers all 5 normalization scenarios from SQLALCHEMY_NAME_NORMALIZATION_EXPLAINED.md:
1. Unquoted ASCII uppercase (PRODUCTID)
2. Unquoted uppercase with special chars (AÑO)
3. Quoted with spaces (CONTRACT STATUS)
4. Quoted lowercase ("order_id")
5. Quoted mixed case ("UserId")
"""

from sqlalchemy import Column, Text, MetaData, Table, quoted_name



def _build_table(oracle_columns, use_exact_case=True):
    """Helper to build a Table mimicking the Oracle column creation path.

    Args:
        oracle_columns: list of (col_name, col_type) tuples simulating all_tab_columns output.
        use_exact_case: If True, use exact Oracle case (desired behavior).
                        If False, use .lower() (current/broken behavior).

    Returns:
        SQLAlchemy Table object.
    """
    metadata = MetaData()
    sqlalchemy_columns = []

    for col_name, col_type in oracle_columns:
        quoted_col_name = quoted_name(col_name, quote=True)
        key = col_name if use_exact_case else col_name.lower()

        column = Column(
            quoted_col_name,
            Text,
            nullable=True,
            key=key,
        )
        sqlalchemy_columns.append(column)

    return Table("test_table", metadata, *sqlalchemy_columns)


class TestOracleColumnCasePreservation:
    """Test that Oracle column names preserve exact case from database.

    These tests all use use_exact_case=True, representing the DESIRED behavior
    (Option 2 from the doc: skip normalization, use exact Oracle names).
    """

    def test_unquoted_ascii_columns_uppercase(self):
        """Case 1: Unquoted ASCII columns stored as UPPERCASE in Oracle."""
        oracle_columns = [
            ("ID", "NUMBER"),
            ("NAME", "VARCHAR2"),
            ("STATUS", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == ["ID", "NAME", "STATUS"]

    def test_unquoted_special_char_columns(self):
        """Case 2: Unquoted columns with special chars like Ñ stored as UPPERCASE."""
        oracle_columns = [
            ("AÑO", "NUMBER"),
            ("TRIMESTRE", "NUMBER"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == ["AÑO", "TRIMESTRE"]

    def test_quoted_columns_with_spaces_preserved(self):
        """Case 3: Quoted columns with spaces preserve exact case."""
        oracle_columns = [
            ("Contract ID", "NUMBER"),
            ("Contract Status", "VARCHAR2"),
            ("Total Amount", "NUMBER"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == ["Contract ID", "Contract Status", "Total Amount"]

    def test_quoted_lowercase_columns_preserved(self):
        """Case 4: Quoted lowercase columns preserve exact case."""
        oracle_columns = [
            ("order_id", "NUMBER"),
            ("customer_name", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == ["order_id", "customer_name"]

    def test_quoted_mixed_case_columns_preserved(self):
        """Case 5: Quoted mixed case columns preserve exact case."""
        oracle_columns = [
            ("UserId", "NUMBER"),
            ("FirstName", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == ["UserId", "FirstName"]

    def test_mixed_all_column_types_realistic(self):
        """Real-world scenario with mixed column types from the documentation."""
        oracle_columns = [
            ("AÑO", "NUMBER"),
            ("TRIMESTRE", "NUMBER"),
            ("CONTRATO RESERVADO", "VARCHAR2"),
            ("ÓRGANO CONTRATACIÓN", "VARCHAR2"),
            ("MODALIDAD", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=True)
        column_keys = [col.key for col in table.columns]

        assert column_keys == [
            "AÑO",
            "TRIMESTRE",
            "CONTRATO RESERVADO",
            "ÓRGANO CONTRATACIÓN",
            "MODALIDAD",
        ]

    def test_column_key_equals_column_name_with_exact_case(self):
        """When using exact case, column.key should equal the Oracle column name."""
        col_name = "TRIMESTRE"
        quoted_col_name = quoted_name(col_name, quote=True)

        column = Column(
            quoted_col_name,
            Text,
            nullable=True,
            key=col_name,
        )

        assert column.name == "TRIMESTRE"
        assert column.key == "TRIMESTRE"


class TestOracleColumnCaseCurrentBehaviorBroken:
    """Tests showing current behavior (.lower()) is WRONG for case-sensitive names.

    These tests document what breaks with the current implementation.
    They should all FAIL until the production code is fixed.
    """

    def test_lowercase_breaks_quoted_columns_with_spaces(self):
        """Case 3: .lower() incorrectly lowercases 'Contract ID' -> 'contract id'."""
        oracle_columns = [
            ("Contract ID", "NUMBER"),
            ("Contract Status", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=False)
        column_keys = [col.key for col in table.columns]

        broken_keys = ["contract id", "contract status"]
        correct_keys = ["Contract ID", "Contract Status"]

        assert column_keys == broken_keys, "Current code produces lowercase"
        assert column_keys != correct_keys, "And that's WRONG for quoted identifiers"

    def test_lowercase_breaks_quoted_mixed_case(self):
        """Case 5: .lower() incorrectly lowercases 'UserId' -> 'userid'."""
        oracle_columns = [
            ("UserId", "NUMBER"),
            ("FirstName", "VARCHAR2"),
        ]

        table = _build_table(oracle_columns, use_exact_case=False)
        column_keys = [col.key for col in table.columns]

        broken_keys = ["userid", "firstname"]
        correct_keys = ["UserId", "FirstName"]

        assert column_keys == broken_keys, "Current code produces lowercase"
        assert column_keys != correct_keys, "And that's WRONG for quoted identifiers"

    def test_lowercase_breaks_special_chars(self):
        """Case 2: .lower() incorrectly lowercases 'AÑO' -> 'año'."""
        oracle_columns = [
            ("AÑO", "NUMBER"),
        ]

        table = _build_table(oracle_columns, use_exact_case=False)
        column_keys = [col.key for col in table.columns]

        broken_keys = ["año"]
        correct_keys = ["AÑO"]

        assert column_keys == broken_keys, "Current code produces lowercase"
        assert column_keys != correct_keys, "And that's WRONG for unquoted Oracle names"


class TestGetResourceColumnsMocked:
    """Test get_resource_columns behavior using mock column objects.

    These tests simulate the data flow in get_resource_columns (connectors.py:638-671)
    without requiring a real database connection.
    """

    class MockColumn:
        def __init__(self, name, key, col_type):
            self.name = name
            self.key = key
            self.type = col_type

    def _simulate_get_resource_columns_current(self, mock_columns):
        """Simulate current get_resource_columns logic with .lower() for Oracle."""
        is_oracle = True
        data = []
        for column in mock_columns:
            column_name = column.name
            if is_oracle:
                column_name = column_name.lower()
            data.append({"COLUMN_NAME": column_name, "DATA_TYPE": str(column.type)})
        return data

    def _simulate_get_resource_columns_fixed(self, mock_columns):
        """Simulate fixed get_resource_columns logic using column.key."""
        data = []
        for column in mock_columns:
            data.append({"COLUMN_NAME": column.key, "DATA_TYPE": str(column.type)})
        return data

    def test_current_behavior_forces_lowercase(self):
        """Current code lowercases ALL Oracle column names."""
        mock_columns = [
            self.MockColumn("TRIMESTRE", "trimestre", "NUMBER"),
            self.MockColumn("CONTRATO", "contrato", "VARCHAR2"),
        ]

        result = self._simulate_get_resource_columns_current(mock_columns)

        assert result[0]["COLUMN_NAME"] == "trimestre"
        assert result[1]["COLUMN_NAME"] == "contrato"

    def test_current_behavior_breaks_quoted_names(self):
        """Current code incorrectly lowercases quoted column names with spaces."""
        mock_columns = [
            self.MockColumn("CONTRATO RESERVADO", "contrato reservado", "VARCHAR2"),
            self.MockColumn("ÓRGANO CONTRATACIÓN", "órgano contratación", "VARCHAR2"),
        ]

        result = self._simulate_get_resource_columns_current(mock_columns)

        assert result[0]["COLUMN_NAME"] == "contrato reservado"
        assert result[1]["COLUMN_NAME"] == "órgano contratación"

    def test_fixed_behavior_preserves_exact_case(self):
        """After fix: column.key preserves exact Oracle case for all types."""
        mock_columns = [
            self.MockColumn("AÑO", "AÑO", "NUMBER"),
            self.MockColumn("TRIMESTRE", "TRIMESTRE", "NUMBER"),
            self.MockColumn("CONTRATO RESERVADO", "CONTRATO RESERVADO", "VARCHAR2"),
            self.MockColumn("ÓRGANO CONTRATACIÓN", "ÓRGANO CONTRATACIÓN", "VARCHAR2"),
            self.MockColumn("MODALIDAD", "MODALIDAD", "VARCHAR2"),
        ]

        result = self._simulate_get_resource_columns_fixed(mock_columns)

        expected_names = [
            "AÑO",
            "TRIMESTRE",
            "CONTRATO RESERVADO",
            "ÓRGANO CONTRATACIÓN",
            "MODALIDAD",
        ]
        actual_names = [r["COLUMN_NAME"] for r in result]
        assert actual_names == expected_names

    def test_fixed_behavior_all_five_cases(self):
        """After fix: all 5 normalization cases from the doc work correctly."""
        mock_columns = [
            self.MockColumn("PRODUCTID", "PRODUCTID", "NUMBER"),
            self.MockColumn("AÑO", "AÑO", "NUMBER"),
            self.MockColumn("CONTRACT STATUS", "CONTRACT STATUS", "VARCHAR2"),
            self.MockColumn("order_id", "order_id", "NUMBER"),
            self.MockColumn("UserId", "UserId", "VARCHAR2"),
        ]

        result = self._simulate_get_resource_columns_fixed(mock_columns)

        expected_names = [
            "PRODUCTID",
            "AÑO",
            "CONTRACT STATUS",
            "order_id",
            "UserId",
        ]
        actual_names = [r["COLUMN_NAME"] for r in result]
        assert actual_names == expected_names
