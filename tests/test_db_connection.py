"""
test_db_connection.py
=====================

This module provides unit tests for database types.

Classes
-------

- **DbTestConfig**: A dataclass to encapsulate test configuration. Intended to be
used to assist in setting up fixtures.
- **TestDbConnection**: A class containing tests methods to test database connections.

Functions
---------

- **working_db(request)**: A fixture providing correctly instantiated and configured database
connections to be used with unit tests.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import cast, Generator, LiteralString, Type
import pytest
from pytest import FixtureRequest
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from database_inspector.db.db_base import DbBase
from database_inspector.db.mssql_db_connection import MSSqlDbConnection
from database_inspector.db.mysql_db_connection import MySqlDbConnection
from database_inspector.db.postgres_db_connection import PostgresDbConnection
from database_inspector.infrastructure.enums import ConnectionStatus
from database_inspector.infrastructure.models import ConnectionParams, DbColumn
from .common import (
    SupportedContainerClasses,
    expected_test_table_results,
    expected_test_table_2_results,
)


@dataclass(slots=True, frozen=True)
class DbTestConfig:
    """Represents test configuration for database tests."""

    db_type: str
    """The type of the database."""

    db_class: Type[DbBase]
    """The name of the database implementation class."""

    container_class: Type[SupportedContainerClasses]
    """The name of the `testcontainers` class to be used."""

    container_version: str
    """The version of the `testcontainers` container to be used."""

    container_port: int
    """The port that the `testcontainers` container exposes."""


db_test_configs: list[DbTestConfig] = [
    DbTestConfig(
        "postgres",
        PostgresDbConnection,
        PostgresContainer,
        "postgres:16-alpine",
        5432,
    ),
    DbTestConfig(
        "mysql",
        MySqlDbConnection,
        MySqlContainer,
        "mysql:9.1.0",
        3306,
    ),
    DbTestConfig(
        "mssql",
        MSSqlDbConnection,
        SqlServerContainer,
        "mcr.microsoft.com/mssql/server:2022-CU13-ubuntu-22.04",
        1433,
    ),
]
"""Test configurations to use in setting up test fixtures."""


@pytest.fixture(name="working_db", scope="module", params=db_test_configs)
def fixture_working_db(request: FixtureRequest) -> Generator[DbBase, None, None]:
    """
    A test fixture to provide a properly connected and configured database.

    :param request: The `request` fixture from `pytest`.
    :type request: FixtureRequest
    :return: A generator that yields `DbBase` instances.
    :rtype: Generator[DbBase, None, None]
    """

    config = request.param
    db_container = config.container_class(config.container_version)
    db_container.start()

    def remove_container() -> None:
        db_container.stop()

    request.addfinalizer(remove_container)

    db_conn_params = ConnectionParams(
        host=db_container.get_container_host_ip(),
        port=db_container.get_exposed_port(config.container_port),
        user=db_container.username,
        password=db_container.password,
        database=db_container.dbname,
    )

    path_to_sql = Path(__file__).parent / "sql" / f"{config.db_type}_test_table.sql"

    with (
        config.db_class(db_conn_params) as db,
        open(path_to_sql, "r", encoding="utf-8") as f,
    ):
        sql_script = cast(LiteralString, f.read())
        queries = [
            query.strip() for query in sql_script.split(";") if query.strip() != ""
        ]

        if (
            db.connection is None
            or db.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise ConnectionError("Connection to database was not established")

        with db.connection.cursor() as cursor:
            for q in queries:
                cursor.execute(q)
            if db.get_connection_status() == ConnectionStatus.DISCONNECTED:
                raise ConnectionError("Connection to database was lost")
            yield db


class TestDbConnection:
    """A test class for testing database connections."""

    def test_working_db_connection(self, working_db: DbBase) -> None:
        """
        Test whether a working database shows the proper connection status.

        :param working_db: The working database fixture.
        :type working_db: DbBase
        """

        assert working_db.connection is not None
        assert working_db.get_connection_status() == ConnectionStatus.CONNECTED

    def test_db_invalid_credentials(self) -> None:
        """Test whether improper credentials raise the appropriate exception."""

        ...

    def test_working_db_get_tables(self, working_db: DbBase) -> None:
        """
        Test whether the `get_tables` method works correctly.

        :param working_db: The working database fixture.
        :type working_db: DbBase
        """

        tables = working_db.get_tables()
        assert len(tables) == 2
        assert tables == ["test_table", "test_table_2"]

    @pytest.mark.parametrize(
        "table_name, expected_columns",
        [
            pytest.param(
                "test_table",
                expected_test_table_results,
            ),
            pytest.param(
                "test_table_2",
                expected_test_table_2_results,
            ),
        ],
    )
    def test_working_db_get_columns(
        self,
        working_db: DbBase,
        table_name: str,
        expected_columns: list[DbColumn],
    ) -> None:
        """
        Test whether the `get_columns` method works correctly.

        :param working_db: The working database fixture.
        :type working_db: DbBase
        :param table_name: The name of the table to test.
        :type table_name: str
        :param expected_columns: The expected column information.
        :type expected_columns: list[DbColumn]
        """

        columns = working_db.get_columns(table_name)
        expected_sorted = sorted(expected_columns, key=lambda column: column.name)
        actual_sorted = sorted(columns, key=lambda column: column.name)
        assert len(columns) == len(expected_columns)
        assert actual_sorted == expected_sorted
