import sqlite3
from sqlite3 import Connection as SqliteConnection
from typing import cast, LiteralString

from database_inspector.db.db_base import DbBase
from database_inspector.infrastructure.enums import ConnectionStatus, DatabaseType
from database_inspector.infrastructure.errors import DbConnectionError
from database_inspector.infrastructure.models import DbColumn
from database_inspector.infrastructure.types import SqliteConnParams


class SqliteDbConnection(DbBase[SqliteConnection, SqliteConnParams]):
    """A class for inspecting SQLite databases."""

    def __init__(self, connection_params: SqliteConnParams) -> None:
        """
        Initialize an instance of SqliteDbConnection and connect to a SQLite database.

        :param connection_params: The connection parameters for the SQLite database.
        :type connection_params: SqliteConnParams
        """

        super().__init__(connection_params)
        self._connection: SqliteConnection | None = None
        self._connect()
        if self._connection is not None:
            self._connection.row_factory = sqlite3.Row

    def _connect(self) -> None:
        """
        Open a connection to the SQLite database.

        :raises DbConnectionError: If the connection is unsuccessful.
        """

        try:
            self._connection = sqlite3.connect(self._connection_params)
        except sqlite3.Error as error:
            raise DbConnectionError(
                f"Connection to database failed: {error}", DatabaseType.SQLITE
            ) from error

    def close(self) -> None:
        """Close the connection to the SQLite database and set the connection to None."""

        if (
            self._connection is not None
            and self.get_connection_status() == ConnectionStatus.CONNECTED
        ):
            self._connection.close()
            self._connection = None

    def get_connection_status(self) -> ConnectionStatus:
        """
        Retrieve the current database connection status.

        :return: The status of the current database connection.
        :rtype: ConnectionStatus
        """

        try:
            if self._connection is not None:
                self._connection.cursor()
                return ConnectionStatus.CONNECTED
            return ConnectionStatus.DISCONNECTED
        except sqlite3.ProgrammingError:
            return ConnectionStatus.DISCONNECTED

    def get_tables(self) -> list[str]:
        """
        Retrieve a list of tables in the database.

        :return: A list of tables in the database.
        :rtype: list[str]
        :raises DbConnectionError: If the connection is closed.
        """

        if (
            self._connection is None
            or self.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise DbConnectionError(
                "Connection to SQLite database was unexpectedly closed.",
                DatabaseType.SQLITE,
            )

        query: LiteralString = cast(
            LiteralString,
            (
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            ),
        )
        cursor = self._connection.execute(query)
        return [row["name"] for row in cursor.fetchall()]

    def get_columns(self, table_name: str) -> list[DbColumn]:
        """
        Retrieve a list of columns in the database.

        :param table_name: The table to retrieve column information from.
        :type table_name: str
        :return: A list of column information in `table_name`.
        :rtype: list[DbColumn]
        :raises DbConnectionError: If the connection is closed.
        """

        if (
            self._connection is None
            or self.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise DbConnectionError(
                "Connection to SQLite database was unexpectedly closed.",
                DatabaseType.SQLITE,
            )

        query: LiteralString = cast(
            LiteralString, f"PRAGMA table_info('{table_name}');"
        )
        table_cols: list[DbColumn] = []

        cursor = self._connection.execute(query)
        columns = cursor.fetchall()
        for c in columns:
            table_cols.append(
                DbColumn(
                    name=c["name"],
                    datatype=self._get_python_type(c["type"]),
                    is_nullable=c["notnull"] == 0,
                )
            )

        return table_cols
