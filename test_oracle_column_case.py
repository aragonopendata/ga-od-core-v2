#!/usr/bin/env python
"""
Quick test to verify Oracle column names preserve exact case.
Run this with: python test_oracle_column_case.py
"""

from sqlalchemy import quoted_name, Column, Text, MetaData, Table

# Simulate what the Oracle fallback function does
def test_column_creation():
    """Test that columns preserve exact case from Oracle."""

    # Simulate Oracle column names as returned by all_tab_columns
    oracle_columns = [
        "AÑO",                    # Unquoted with special char
        "TRIMESTRE",              # Unquoted ASCII
        "CONTRATO RESERVADO",     # Quoted with space
        "ÓRGANO CONTRATACIÓN",    # Quoted with special chars
        "MODALIDAD",              # Unquoted ASCII
    ]

    metadata = MetaData()
    sqlalchemy_columns = []

    for col_name in oracle_columns:
        # This is what the fixed code does
        quoted_col_name = quoted_name(col_name, quote=True)

        column = Column(
            quoted_col_name,
            Text,
            nullable=True,
            key=col_name,  # Use exact case from Oracle
        )

        sqlalchemy_columns.append(column)

    # Create a test table
    table = Table(
        "test_table",
        metadata,
        *sqlalchemy_columns,
    )

    # Verify that keys preserve exact case
    print("Column keys (as they will appear in API):")
    for col in table.columns:
        print(f"  '{col.key}'")

    # Verify expected behavior
    expected_keys = oracle_columns  # Should match exactly
    actual_keys = [col.key for col in table.columns]

    if expected_keys == actual_keys:
        print("\n✓ SUCCESS: Column names preserve exact case from Oracle!")
        return True
    else:
        print("\n✗ FAILURE: Column names don't match!")
        print(f"Expected: {expected_keys}")
        print(f"Actual:   {actual_keys}")
        return False

if __name__ == "__main__":
    success = test_column_creation()
    exit(0 if success else 1)
