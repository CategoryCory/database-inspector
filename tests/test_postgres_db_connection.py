import pytest
from datetime import datetime
from pathlib import Path
from pytest import FixtureRequest
from testcontainers.postgres import PostgresContainer
from typing import cast, Generator, LiteralString

from src.database.db_base import DbBase, DbColumn
from src.database.postgres_db_connection import PostgresDbConnection, PostgresConnParams

postgres: PostgresContainer = PostgresContainer("postgres:16-alpine")


@pytest.fixture(scope="module", autouse=True)
def postgres_db(request: FixtureRequest) -> Generator[DbBase, None]:
    postgres.start()

    def remove_container() -> None:
        postgres.stop()

    request.addfinalizer(remove_container)

    pg_conn_params: PostgresConnParams = PostgresConnParams(
        host=postgres.get_container_host_ip(),
        port=postgres.get_exposed_port(5432),
        user=postgres.username,
        password=postgres.password,
        dbname=postgres.dbname,
    )

    with PostgresDbConnection() as db:
        path_to_sql = Path(__file__).parent / "sql" / "postgres_test_table.sql"

        with open(path_to_sql, "r") as f:
            sql_script = f.read()
            db.connect(pg_conn_params)

            if db.connection is None:
                raise ConnectionError(
                    "Connection to PostgreSQL database was not established"
                )

            db.connection.execute(cast(LiteralString, sql_script))

            yield db


class TestPostgresDbConnection:
    def test_postgres_db_connection(self, postgres_db: PostgresDbConnection) -> None:
        assert postgres_db.connection is not None

    def test_postgres_db_get_tables(self, postgres_db: PostgresDbConnection) -> None:
        tables = postgres_db.get_tables()
        assert len(tables) == 2
        assert "test_table" in tables
        assert "logs" in tables

    @pytest.mark.parametrize(
        "table_name, expected_columns",
        [
            pytest.param(
                "test_table",
                [
                    DbColumn(name="id", datatype=int, is_nullable=False),
                    DbColumn(name="name", datatype=str, is_nullable=False),
                    DbColumn(name="email", datatype=str, is_nullable=False),
                    DbColumn(name="date_of_birth", datatype=datetime, is_nullable=True),
                ],
            ),
            pytest.param(
                "logs",
                [
                    DbColumn(name="id", datatype=int, is_nullable=False),
                    DbColumn(name="timestamp", datatype=datetime, is_nullable=False),
                    DbColumn(name="level", datatype=str, is_nullable=False),
                    DbColumn(name="source", datatype=str, is_nullable=False),
                    DbColumn(name="message", datatype=str, is_nullable=True),
                ],
            ),
        ],
    )
    def test_postgres_get_columns(
        self,
        postgres_db: PostgresDbConnection,
        table_name: str,
        expected_columns: list[DbColumn],
    ) -> None:
        columns = postgres_db.get_columns(table_name)
        expected_sorted = sorted(expected_columns, key=lambda column: column.name)
        actual_sorted = sorted(columns, key=lambda column: column.name)
        assert len(columns) == len(expected_columns)
        assert actual_sorted == expected_sorted
