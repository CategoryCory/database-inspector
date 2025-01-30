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
- **fixture_empty_sqlite_connection()**: A fixture providing an empty SQLite in-memory connection.
- **fixture_populated_sqlite_connection()**: A fixture providing a populated SQLite in-memory
connection.
"""

from collections.abc import Generator
from contextlib import nullcontext
from pathlib import Path
import pytest
from pytest_lazy_fixtures import lf

from database_inspector.db.db_base import DbBase
from database_inspector.db.sqlite_db_connection import SqliteDbConnection
from database_inspector.infrastructure.enums import ConnectionStatus
from database_inspector.infrastructure.errors import DatabaseTableNotFoundError
from .common import (
    expected_test_table_columns,
    expected_test_table_2_columns,
    expected_test_schema,
)


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


@pytest.fixture(name="empty_sqlite_connection")
def fixture_empty_sqlite_connection() -> Generator[DbBase, None, None]:
    """
    A test fixture to provide an empty SQLite database connection.

    :return: A generator that yields an empty `DbBase` instance.
    :rtype: Generator[DbBase, None, None]
    """

    with SqliteDbConnection(":memory:") as connection:
        yield connection


@pytest.fixture(name="populated_sqlite_connection")
def fixture_populated_sqlite_connection() -> Generator[DbBase, None, None]:
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
        A test method to verify that a SQLite connection works properly.

        :param db_path: The expected path to a SQLite database file.
        :type db_path: `pathlib.Path`
        """

        with SqliteDbConnection(db_path) as db:
            assert db_path.exists()
            assert db.get_connection_status() == ConnectionStatus.CONNECTED
            assert db.connection is not None

        assert db.get_connection_status() == ConnectionStatus.DISCONNECTED
        assert db.connection is None

    @pytest.mark.parametrize(
        "db, expected",
        [
            pytest.param(lf("empty_sqlite_connection"), []),
            pytest.param(
                lf("populated_sqlite_connection"), ["test_table", "test_table_2"]
            ),
        ],
    )
    def test_sqlite_get_tables(self, db: DbBase, expected: list[str]) -> None:
        """
        Test whether the `get_tables` method works correctly.

        :param db: The working database fixture.
        :type db: DbBase
        """

        tables = db.get_tables()
        assert len(tables) == len(expected)
        assert tables == expected

    @pytest.mark.parametrize(
        "table_name, expected",
        [
            pytest.param(
                "test_table",
                nullcontext(expected_test_table_columns),
            ),
            pytest.param(
                "test_table_2",
                nullcontext(expected_test_table_2_columns),
            ),
            pytest.param(
                "fake_table",
                pytest.raises(DatabaseTableNotFoundError),
            ),
        ],
    )
    def test_sqlite_get_columns(
        self,
        populated_sqlite_connection: SqliteDbConnection,
        table_name: str,
        expected: nullcontext,
    ) -> None:
        """
        Test whether the `get_columns` method works correctly.

        :param populated_sqlite_connection: The working database fixture.
        :type populated_sqlite_connection: SqliteDbConnection
        :param table_name: The name of the table to test.
        :type table_name: str
        :param expected: The expected column information.
        :type expected: list[DbColumn]
        """

        with expected as e:
            columns = populated_sqlite_connection.get_columns(table_name)
            assert len(columns) == len(e)
            assert columns == e

    def test_db_extract_schema(self, populated_sqlite_connection: DbBase) -> None:
        """
        Test whether the `extract_schema` method works correctly.

        :param populated_sqlite_connection: The database fixture.
        :type populated_sqlite_connection: DbBase
        """

        schema = populated_sqlite_connection.extract_schema()
        assert isinstance(schema, dict)
        assert schema == expected_test_schema
