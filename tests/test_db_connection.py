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

- **db_test_configs()**: Retrieves a list of test configurations.
- **fixture_container_setup(request)**: A fixture that sets up and starts a Docker container
running an instance of a database.
- **fixture_empty_db(container_setup)**: A fixture that opens a connection to an empty database.
- **fixture_populated_db(container_setup)**: A fixture that opens a connection to a database and
populates it with test tables.

"""

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import cast, Generator, LiteralString, Type
from unittest.mock import MagicMock, patch
import pytest
from pytest import FixtureRequest
from pytest_lazy_fixtures import lf
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from database_inspector.db.db_base import DbBase
from database_inspector.db.mssql_db_connection import MSSqlDbConnection
from database_inspector.db.mysql_db_connection import MySqlDbConnection
from database_inspector.db.postgres_db_connection import PostgresDbConnection
from database_inspector.infrastructure.enums import ConnectionStatus, DatabaseType
from database_inspector.infrastructure.errors import (
    DatabaseConnectionError,
    DatabaseTableNotFoundError,
)
from database_inspector.infrastructure.models import ConnectionParams
from .common import (
    SupportedContainerClasses,
    expected_test_table_columns,
    expected_test_table_2_columns,
    expected_test_schema,
)


@dataclass(slots=True, frozen=True)
class DbTestConfig:
    """Represents test configuration for database tests."""

    db_type: DatabaseType
    """The type of the database."""

    db_class: Type[DbBase]
    """The name of the database implementation class."""

    container_class: Type[SupportedContainerClasses]
    """The name of the `testcontainers` class to be used."""

    container_version: str
    """The version of the `testcontainers` container to be used."""

    container_port: int
    """The port that the `testcontainers` container exposes."""


@dataclass(slots=True, frozen=True)
class DbTestContainerInfo:
    """Represents information about a database test container."""

    container: SupportedContainerClasses
    """The database container."""

    config: DbTestConfig
    """The test configuration for the container."""


def db_test_configs() -> list[DbTestConfig]:
    """
    Returns a list of database test configurations.

    :return: The list of database test configurations.
    :rtype: list[DbTestConfig]
    """

    return [
        DbTestConfig(
            DatabaseType.POSTGRES,
            PostgresDbConnection,
            PostgresContainer,
            "postgres:16-alpine",
            5432,
        ),
        DbTestConfig(
            DatabaseType.MYSQL,
            MySqlDbConnection,
            MySqlContainer,
            "mysql:9.1.0",
            3306,
        ),
        DbTestConfig(
            DatabaseType.MSSQL,
            MSSqlDbConnection,
            SqlServerContainer,
            "mcr.microsoft.com/mssql/server:2022-CU13-ubuntu-22.04",
            1433,
        ),
    ]


@pytest.fixture(
    name="mock_database",
    params=[PostgresDbConnection, MySqlDbConnection, MSSqlDbConnection],
)
def fixture_mock_database(request: FixtureRequest) -> Generator[DbBase, None, None]:
    """
    A test fixture that yields a mock database connection.

    :param request: The `pytest` request fixture.
    :type request: FixtureRequest
    :return: A generator that yields mock `DbBase` instances.
    :rtype: Generator[DbBase, None, None]
    """

    with (
        patch.object(request.param, "__init__", lambda self, conn_params: None),
        patch.object(
            request.param, "get_tables", return_value=["test_table", "test_table_2"]
        ),
        patch.object(
            request.param,
            "get_columns",
            side_effect=lambda table: expected_test_table_columns
            if table == "test_table"
            else expected_test_table_2_columns,
        ),
    ):
        db = request.param(None)
        yield db


@pytest.fixture(name="container_setup", params=db_test_configs())
def fixture_container_setup(
    request: FixtureRequest,
) -> Generator[DbTestContainerInfo, None, None]:
    """
    A test fixture that yields a database test container along with its configuration.

    :param request: The `pytest` request fixture.
    :type request: FixtureRequest
    :return: A generator that yields `DbTestContainerInfo` instances.
    :rtype: Generator[DbTestContainerInfo, None, None]
    """

    config = request.param
    container = config.container_class(config.container_version)
    container.start()

    def remove_container() -> None:
        container.stop()

    request.addfinalizer(remove_container)

    yield DbTestContainerInfo(container, config)


@pytest.fixture(name="empty_db")
def fixture_empty_db(
    container_setup: DbTestContainerInfo,
) -> Generator[DbBase, None, None]:
    """
    A test fixture that yields a connection to an empty database.

    :param container_setup: The database test container info.
    :type container_setup: DbTestContainerInfo
    :return: A generator that yields `DbBase` instances.
    :rtype: Generator[DbBase, None, None]
    """

    db_conn_params = ConnectionParams(
        host=container_setup.container.get_container_host_ip(),
        port=container_setup.container.get_exposed_port(
            container_setup.config.container_port
        ),
        user=container_setup.container.username,
        password=container_setup.container.password,
        database=container_setup.container.dbname,
    )

    with container_setup.config.db_class(db_conn_params) as db:
        yield db


@pytest.fixture(name="populated_db")
def fixture_populated_db(
    container_setup: DbTestContainerInfo,
) -> Generator[DbBase, None, None]:
    """
    A test fixture to provide a properly connected and configured database.

    :param container_setup: The database test container info.
    :type container_setup: DbTestContainerInfo
    :return: A generator that yields `DbBase` instances.
    :rtype: Generator[DbBase, None, None]
    """

    db_conn_params = ConnectionParams(
        host=container_setup.container.get_container_host_ip(),
        port=container_setup.container.get_exposed_port(
            container_setup.config.container_port
        ),
        user=container_setup.container.username,
        password=container_setup.container.password,
        database=container_setup.container.dbname,
    )

    path_to_sql = (
        Path(__file__).parent
        / "sql"
        / f"{container_setup.config.db_type.name.lower()}_test_table.sql"
    )

    with (
        container_setup.config.db_class(db_conn_params) as db,
        open(path_to_sql, "r", encoding="utf-8") as f,
    ):
        sql_script = cast(LiteralString, f.read())
        queries = [
            query.strip() for query in sql_script.split(";") if query.strip() != ""
        ]

        with db.connection.cursor() as cursor:  # type: ignore
            for q in queries:
                cursor.execute(q)

        yield db


class TestDbConnection:
    """A test class for testing database connections."""

    def test_db_connection(self, container_setup: DbTestContainerInfo) -> None:
        """
        Test whether a connection to the database works as expected.

        :param container_setup: Data related to a database test container.
        :type container_setup: DbTestContainerInfo
        """

        db_conn_params = ConnectionParams(
            host=container_setup.container.get_container_host_ip(),
            port=container_setup.container.get_exposed_port(
                container_setup.config.container_port
            ),
            user=container_setup.container.username,
            password=container_setup.container.password,
            database=container_setup.container.dbname,
        )

        with container_setup.config.db_class(db_conn_params) as db:
            assert db.connection is not None
            assert db.get_connection_status() == ConnectionStatus.CONNECTED

        assert db.connection is None
        assert db.get_connection_status() == ConnectionStatus.DISCONNECTED

    def test_db_connection_bad_creds(
        self, container_setup: DbTestContainerInfo
    ) -> None:
        """
        Test whether an exception is raised when attempting to connect to a database with
        invalid credentials.

        :param container_setup: Data related to a database test container.
        :type container_setup: DbTestContainerInfo
        """

        db_conn_params = ConnectionParams(
            host=container_setup.container.get_container_host_ip(),
            port=container_setup.container.get_exposed_port(
                container_setup.config.container_port
            ),
            user="FAKE_USER",
            password="FAKE_PASSWORD",
            database=container_setup.container.dbname,
        )

        with pytest.raises(DatabaseConnectionError) as exc_info:
            with container_setup.config.db_class(db_conn_params):
                pass

        assert exc_info.type is DatabaseConnectionError
        assert str(exc_info.value).startswith("Connection to database failed:")
        assert exc_info.value.db_type == container_setup.config.db_type

    @pytest.mark.parametrize(
        "db, expected_tables",
        [
            pytest.param(lf("empty_db"), []),
            pytest.param(
                lf("populated_db"),
                [
                    "test_table",
                    "test_table_2",
                ],
            ),
        ],
    )
    def test_working_db_get_tables(
        self, db: DbBase, expected_tables: list[str]
    ) -> None:
        """
        Test whether the `get_tables` method works correctly.

        :param db: The database container fixture.
        :type db: DbBase
        :param expected_tables: The expected list of table names.
        :type expected_tables: list[str]
        """

        tables = db.get_tables()
        assert len(tables) == len(expected_tables)
        assert tables == expected_tables

    @pytest.mark.parametrize(
        "table_name, expected_columns",
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
    def test_populated_db_get_columns(
        self,
        populated_db: DbBase,
        table_name: str,
        expected_columns: nullcontext,
    ) -> None:
        """
        Test whether the `get_columns` method works correctly.

        :param populated_db: The populated database fixture.
        :type populated_db: DbBase
        :param table_name: The name of the table to test.
        :type table_name: str
        :param expected_columns: The expected column information.
        :type expected_columns: list[DbColumn]
        """

        with expected_columns as e:
            columns = populated_db.get_columns(table_name)
            expected_sorted = sorted(e, key=lambda column: column.name)
            actual_sorted = sorted(columns, key=lambda column: column.name)
            assert len(columns) == len(e)
            assert actual_sorted == expected_sorted

    def test_db_extract_schema(self, mock_database: DbBase) -> None:
        """
        Test whether the `extract_schema` method works correctly.

        :param mock_database: The database fixture.
        :type mock_database: DbBase
        """

        schema = mock_database.extract_schema()
        assert isinstance(schema, dict)
        assert isinstance(mock_database.get_tables, MagicMock)
        assert isinstance(mock_database.get_columns, MagicMock)
        assert schema == expected_test_schema
        mock_database.get_tables.assert_called_once()
        mock_database.get_columns.assert_any_call("test_table")
        mock_database.get_columns.assert_any_call("test_table_2")
