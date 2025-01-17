import mysql.connector
from dataclasses import asdict
from typing import cast, LiteralString

from database_inspector.db.db_base import DbBase
from database_inspector.infrastructure.enums import ConnectionStatus, DatabaseType
from database_inspector.infrastructure.errors import DbConnectionError
from database_inspector.infrastructure.models import ConnectionParams, DbColumn
from database_inspector.infrastructure.types import MySQLConnectionTypes


class MySqlDbConnection(DbBase[MySQLConnectionTypes, ConnectionParams]):
    """A class for inspecting MySQL databases."""

    def __init__(self, connection_params: ConnectionParams) -> None:
        """
        Initialize an instance of MySqlDbConnection and connect to a MySQL database.

        :param connection_params: The connection parameters for the MySQL database.
        :type connection_params: ConnectionParams
        """

        super().__init__(connection_params)
        self._connection: MySQLConnectionTypes | None = None
        self._connect()

    def _connect(self) -> None:
        """
        Open a connection to the MySQL database.

        :raises DbConnectionError: If the connection is unsuccessful.
        """

        try:
            # TODO: Remove `noqa` after PyCharm update resolves "expected DataclassInstance" warning
            self._connection = mysql.connector.connect(
                **asdict(self._connection_params),  # noqa
                autocommit=True,
            )
        except mysql.connector.Error as error:
            raise DbConnectionError(
                f"Connection to database failed: {error}", DatabaseType.MYSQL
            ) from error

    def close(self) -> None:
        """Close the connection to the MySQL database and set the connection to None."""

        if (
            self._connection is not None
            and self.get_connection_status() == ConnectionStatus.CONNECTED
        ):
            self._connection.close()
            self._connection = None

    def get_connection_status(self) -> ConnectionStatus:
        """
        Retrieve the current database connection status.

        :return: The status of the database connection.
        :rtype: ConnectionStatus
        """

        if self._connection is not None and self._connection.is_connected():
            return ConnectionStatus.CONNECTED
        else:
            return ConnectionStatus.DISCONNECTED

    def get_tables(self) -> list[str]:
        """
        Retrieve a list of tables in the database.

        :return: A list of the tables in the database.
        :rtype: list[str]
        :raises DbConnectionError: If the connection is closed.
        """

        if (
            self._connection is None
            or self.get_connection_status() == ConnectionStatus.DISCONNECTED
        ):
            raise DbConnectionError(
                "Connection to MySQL database was unexpectedly closed.",
                DatabaseType.MYSQL,
            )

        query: LiteralString = "SHOW TABLES;"
        tables_results_col_header = f"Tables_in_{self._connection_params.database}"

        with self._connection.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            results = cast(list[dict], cursor.fetchall())
            return [row[tables_results_col_header] for row in results]

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
                "Connection to MySQL database was unexpectedly closed.",
                DatabaseType.MYSQL,
            )

        query: LiteralString = cast(LiteralString, f"DESCRIBE {table_name};")
        table_cols: list[DbColumn] = []

        with self._connection.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            columns = cast(list[dict], cursor.fetchall())
            for c in columns:
                table_cols.append(
                    DbColumn(
                        name=c["Field"],
                        datatype=self._get_python_type(c["Type"]),
                        is_nullable=c["Null"].lower() == "yes",
                    )
                )

        return table_cols
