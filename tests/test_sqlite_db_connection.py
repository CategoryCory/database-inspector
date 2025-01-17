import pytest
from collections.abc import Generator
from pathlib import Path

from .common import expected_test_table_results, expected_test_table_2_results
from src import DbBase
from src.database_inspector.infrastructure.enums import ConnectionStatus
from src import DbColumn
from src import SqliteDbConnection


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def sqlite_connection() -> Generator[DbBase, None]:
    with SqliteDbConnection(":memory:") as connection:
        path_to_sql = Path(__file__).parent / "sql" / "sqlite_test_table.sql"

        with open(path_to_sql, "r") as sql_file:
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
    def test_sqlite_db_connects_to_db(self, db_path: Path) -> None:
        with SqliteDbConnection(db_path):
            assert db_path.exists()

    def test_sqlite_db_disconnects_from_db(self, db_path: Path) -> None:
        with SqliteDbConnection(db_path) as connection:
            pass

        assert connection.get_connection_status() == ConnectionStatus.DISCONNECTED

    def test_sqlite_get_tables(self, sqlite_connection: SqliteDbConnection) -> None:
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
        columns = sqlite_connection.get_columns(table_name)
        assert len(columns) == len(expected_columns)
        assert columns == expected_columns
