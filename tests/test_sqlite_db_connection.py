import pytest
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

from src.database.db_base import DbBase, DbColumn
from src.database.sqlite_db_connection import SqliteDbConnection


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def sqlite_connection() -> Generator[DbBase, None]:
    with SqliteDbConnection() as connection:
        path_to_sql = Path(__file__).parent / "sql" / "sqlite_test_table.sql"

        with open(path_to_sql, "r") as sql_file:
            create_tables_sql = sql_file.read()

            connection.connect(":memory:")

            if connection.connection is None:
                raise ConnectionError(
                    "Connection to SQLite database was not established"
                )

            connection.connection.executescript(create_tables_sql)

            yield connection


class TestSqliteDBConnection:
    def test_sqlite_db_connects_to_db(self, db_path: Path) -> None:
        with SqliteDbConnection() as connection:
            connection.connect(db_path)
            assert db_path.exists()

    def test_sqlite_db_disconnects_from_db(self, db_path: Path) -> None:
        with SqliteDbConnection() as connection:
            connection.connect(db_path)

        assert connection.connection is None

    def test_sqlite_get_tables(self, sqlite_connection: SqliteDbConnection) -> None:
        tables = sqlite_connection.get_tables()
        assert len(tables) == 2
        assert "test_table" in tables
        assert "logs" in tables

    @pytest.mark.parametrize(
        "table_name, expected_columns",
        [
            pytest.param(
                "test_table",
                [
                    DbColumn(name="id", datatype=int, is_nullable=True),
                    DbColumn(name="name", datatype=str, is_nullable=False),
                    DbColumn(name="email", datatype=str, is_nullable=False),
                    DbColumn(name="date_of_birth", datatype=datetime, is_nullable=True),
                ],
            ),
            pytest.param(
                "logs",
                [
                    DbColumn(name="id", datatype=int, is_nullable=True),
                    DbColumn(name="timestamp", datatype=datetime, is_nullable=False),
                    DbColumn(name="level", datatype=str, is_nullable=False),
                    DbColumn(name="source", datatype=str, is_nullable=False),
                    DbColumn(name="message", datatype=str, is_nullable=True),
                ],
            ),
        ],
    )
    def test_sqlite_get_columns(
        self,
        sqlite_connection: SqliteDbConnection,
        table_name: str,
        expected_columns: list[DbColumn],
    ) -> None:
        columns = sqlite_connection.get_columns(table_name)
        assert len(columns) == len(expected_columns)
        assert columns == expected_columns
