import pytest
from dataclasses import dataclass
from pathlib import Path
from pytest import FixtureRequest
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer
from typing import cast, Generator, LiteralString, Type

from .common import (
    SupportedContainerClasses,
    expected_test_table_results,
    expected_test_table_2_results,
)
from src import DbBase
from src.database_inspector.infrastructure.enums import ConnectionStatus
from src import ConnectionParams, DbColumn
from src import MSSqlDbConnection
from src import MySqlDbConnection
from src import PostgresDbConnection


@dataclass(slots=True, frozen=True)
class DbTestConfig:
    db_type: str
    db_class: Type[DbBase]
    container_class: Type[SupportedContainerClasses]
    container_version: str
    container_port: int


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


@pytest.fixture(scope="module", params=db_test_configs)
def working_db(request: FixtureRequest) -> Generator[DbBase, None]:
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

    with config.db_class(db_conn_params) as db, open(path_to_sql, "r") as f:
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
    def test_working_db_connection(self, working_db: DbBase) -> None:
        assert working_db.connection is not None
        assert working_db.get_connection_status() == ConnectionStatus.CONNECTED

    def test_db_invalid_credentials(self) -> None: ...

    def test_working_db_get_tables(self, working_db: DbBase) -> None:
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
        columns = working_db.get_columns(table_name)
        expected_sorted = sorted(expected_columns, key=lambda column: column.name)
        actual_sorted = sorted(columns, key=lambda column: column.name)
        assert len(columns) == len(expected_columns)
        assert actual_sorted == expected_sorted
