"""
postgres_db_connection.py
=========================

This module provides an implementation of the :class:`database_inspector.db.DbBase` abstract
base class for inspecting PostgreSQL databases.

Classes
-------

- **PostgresDbConnection**: Implements :class:`database_inspector.db.DbBase` for inspecting
PostgreSQL databases.

Usage Example
-------------

.. code-block:: python

    from database_inspector.db.postgres_db_connection import PostgresDbConnection

    with PostgresDbConnection(connection_params) as db:
        db.get_tables()
"""

from typing import cast, LiteralString
import psycopg
from psycopg import Connection as PostgresConnection
from psycopg.rows import dict_row

from database_inspector.db.db_base import DbBase
from database_inspector.infrastructure.enums import ConnectionStatus, DatabaseType
from database_inspector.infrastructure.errors import (
    DatabaseConnectionError,
    DatabaseTableNotFoundError,
)
from database_inspector.infrastructure.models import ConnectionParams, DbColumn


class PostgresDbConnection(DbBase[PostgresConnection, ConnectionParams]):
    """A class for inspecting PostgreSQL databases."""

    def __init__(self, connection_params: ConnectionParams) -> None:
        """
        Initialize an instance of PostgresDbConnection and connect to a PostgreSQL database.

        :param connection_params: The connection parameters for the PostgreSQL database.
        :type connection_params: ConnectionParams
        """

        super().__init__(connection_params)
        self._connection: PostgresConnection | None = None
        self._connect()

    def _connect(self) -> None:
        """
        Open a connection to the PostgreSQL database.

        :raises DatabaseConnectionError: If the connection is unsuccessful.
        """

        try:
            conn_str = (
                f"postgresql://{self._connection_params.user}:{self._connection_params.password}"
                f"@{self._connection_params.host}:{self._connection_params.port}/"
                f"{self._connection_params.database}"
            )
            self._connection = psycopg.connect(conn_str)
        except psycopg.Error as error:
            raise DatabaseConnectionError(
                f"Connection to database failed: {error}", DatabaseType.POSTGRES
            ) from error

    def get_connection_status(self) -> ConnectionStatus:
        """
        Retrieve the current database connection status.

        :return: The status of the database connection.
        :rtype: ConnectionStatus
        """

        if self._connection is not None and not self._connection.closed:  # pylint: disable=E1101
            return ConnectionStatus.CONNECTED
        return ConnectionStatus.DISCONNECTED

    def get_tables(self) -> list[str]:
        """
        Retrieve a list of tables in the database.

        :return: A list of the tables in the database.
        :rtype: list[str]
        :raises DatabaseConnectionError: If the connection is closed.
        """

        if (
            self._connection is None
            or self.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise DatabaseConnectionError(
                "Connection to PostgreSQL database was unexpectedly closed.",
                DatabaseType.POSTGRES,
            )

        query: LiteralString = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """

        with self._connection.cursor(row_factory=dict_row) as cursor:  # pylint: disable=E1101
            cursor.execute(query)
            return [row["table_name"] for row in cursor.fetchall()]

    def get_columns(self, table_name: str) -> list[DbColumn]:
        """
        Retrieve a list of columns in the database.

        :param table_name: The table to retrieve column information from.
        :type table_name: str
        :return: A list of column information in `table_name`.
        :rtype: list[DbColumn]
        :raises DatabaseConnectionError: If the connection is closed.
        :raises DatabaseTableNotFoundError: If the table is not found.
        """

        if (
            self._connection is None
            or self.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise DatabaseConnectionError(
                "Connection to PostgreSQL database was unexpectedly closed.",
                DatabaseType.POSTGRES,
            )

        with self._connection.cursor(row_factory=dict_row) as cursor:  # pylint: disable=E1101
            get_current_schema_query: LiteralString = "SELECT CURRENT_SCHEMA;"
            result = cursor.execute(get_current_schema_query).fetchall()
            current_schema = result[0]["current_schema"]

            check_table_exists_query: LiteralString = cast(
                LiteralString, f"SELECT to_regclass('{current_schema}.{table_name}');"
            )
            result = cursor.execute(check_table_exists_query).fetchall()
            table_regclass = result[0]["to_regclass"]

            if table_regclass is None:
                raise DatabaseTableNotFoundError(
                    f"The specified table {table_name} does not exist.",
                    DatabaseType.POSTGRES,
                )

            get_columns_query: LiteralString = cast(
                LiteralString,
                (
                    f"SELECT column_name, data_type, is_nullable "
                    f"FROM information_schema.columns "
                    f"WHERE table_schema = '{current_schema}' "
                    f"AND table_name = '{table_name}';"
                ),
            )
            table_cols: list[DbColumn] = []

            cursor.execute(get_columns_query)
            columns = cursor.fetchall()
            for c in columns:
                table_cols.append(
                    DbColumn(
                        name=c["column_name"],
                        datatype=self._get_python_type(c["data_type"]),
                        is_nullable=c["is_nullable"].lower() == "yes",
                    )
                )

            return table_cols
