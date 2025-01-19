"""
test_sqlite_db_connection.py
=====================

This module provides unit tests specifically for SQLite database types.

Classes
-------

- **TestSqliteDBConnection**: A class containing tests methods to test SQLite database connections.

Functions
---------

- **db_path(tmp_path)**: A fixture providing a path to a SQLite database file.
- **sqlite_connection()**: A fixture providing a SQLite in-memory connection.
"""

from collections.abc import Generator
from pathlib import Path
import pytest

from database_inspector.db.db_base import DbBase
from database_inspector.infrastructure.enums import ConnectionStatus
from database_inspector.infrastructure.models import DbColumn
from database_inspector.db.sqlite_db_connection import SqliteDbConnection
from .common import expected_test_table_results, expected_test_table_2_results


@pytest.fixture(name="db_path")
def fixture_db_path(tmp_path: Path) -> Path:
    """
    A test fixture providing a path to a SQLite database file.

    :param tmp_path: The `tmp_path` fixture from `pytest`.
    :type tmp_path: `pathlib.Path`
    :return: The path to the SQLite database file.
    :rtype: `pathlib.Path`
    """

    return tmp_path / "test.db"


@pytest.fixture(name="sqlite_connection")
def fixture_sqlite_connection() -> Generator[DbBase, None, None]:
    """
    A test fixture to provide a properly connected and configured SQLite in-memory
    database.

    :return: A generator that yields `DbBase` instances.
    :rtype: Generator[DbBase, None, None]
    """

    with SqliteDbConnection(":memory:") as connection:
        path_to_sql = Path(__file__).parent / "sql" / "sqlite_test_table.sql"

        with open(path_to_sql, "r", encoding="utf-8") as sql_file:
            create_tables_sql = sql_file.read()

            if (
                connection.connection is None
                or connection.get_connection_status() == ConnectionStatus.DISCONNECTED
            ):
                raise ConnectionError(
                    "Connection to SQLite database was not established"
                )

            connection.connection.executescript(create_tables_sql)

            yield connection


class TestSqliteDBConnection:
    """A test class for testing SQLite database connections."""

    def test_sqlite_db_connects_to_db(self, db_path: Path) -> None:
        """
        A test method to verify that a SQLite file is properly created during initialization.

        :param db_path: The expected path to a SQLite database file.
        :type db_path: `pathlib.Path`
        """

        with SqliteDbConnection(db_path):
            assert db_path.exists()

    def test_sqlite_db_disconnects_from_db(self, db_path: Path) -> None:
        """
        A test method to verify that a SQLite connection is properly disconnected.

        :param db_path: The expected path to a SQLite database file.
        :type db_path: `pathlib.Path`
        """

        with SqliteDbConnection(db_path) as connection:
            pass

        assert connection.get_connection_status() == ConnectionStatus.DISCONNECTED

    def test_sqlite_get_tables(self, sqlite_connection: SqliteDbConnection) -> None:
        """
        Test whether the `get_tables` method works correctly.

        :param sqlite_connection: The working database fixture.
        :type sqlite_connection: SqliteDbConnection
        """

        tables = sqlite_connection.get_tables()
        assert len(tables) == 2
        assert "test_table" in tables
        assert "test_table_2" in tables

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
    def test_sqlite_get_columns(
        self,
        sqlite_connection: SqliteDbConnection,
        table_name: str,
        expected_columns: list[DbColumn],
    ) -> None:
        """
        Test whether the `get_columns` method works correctly.

        :param sqlite_connection: The working database fixture.
        :type sqlite_connection: SqliteDbConnection
        :param table_name: The name of the table to test.
        :type table_name: str
        :param expected_columns: The expected column information.
        :type expected_columns: list[DbColumn]
        """

        columns = sqlite_connection.get_columns(table_name)
        assert len(columns) == len(expected_columns)
        assert columns == expected_columns
