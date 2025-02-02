"""
common.py
=========

This module contains expected test results for unit tests.
"""

from datetime import date, datetime
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from database_inspector.infrastructure.models import DbColumn
from database_inspector.infrastructure.types import DbSchema

type SupportedContainerClasses = PostgresContainer | MySqlContainer | SqlServerContainer

expected_test_table_columns: list[DbColumn] = [
    DbColumn(name="id", datatype=int, is_nullable=False),
    DbColumn(name="name", datatype=str, is_nullable=False),
    DbColumn(name="email", datatype=str, is_nullable=False),
    DbColumn(name="date_of_birth", datatype=date, is_nullable=True),
    DbColumn(name="timestamp", datatype=datetime, is_nullable=False),
    DbColumn(name="description", datatype=str, is_nullable=True),
]

expected_test_table_2_columns: list[DbColumn] = [
    DbColumn(name="id", datatype=int, is_nullable=False),
    DbColumn(name="product_name", datatype=str, is_nullable=False),
    DbColumn(name="product_description", datatype=str, is_nullable=False),
    DbColumn(name="product_price", datatype=float, is_nullable=False),
    DbColumn(name="timestamp", datatype=datetime, is_nullable=False),
]

expected_test_schema: DbSchema = {
    "test_table": expected_test_table_columns,
    "test_table_2": expected_test_table_2_columns,
}

expected_test_schema_no_tables: DbSchema = {}
