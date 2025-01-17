import pyodbc  # type: ignore
from pyodbc import Connection as MSSQLConnection, Cursor as MSSQLCursor
from typing import cast, LiteralString

from database_inspector.db.db_base import DbBase
from database_inspector.infrastructure.enums import ConnectionStatus, DatabaseType
from database_inspector.infrastructure.errors import DbConnectionError
from database_inspector.infrastructure.models import ConnectionParams, DbColumn


def db_results_to_dict(cursor: MSSQLCursor) -> list[dict[str, str]]:
    """
    Convert the result of a query to a dictionary.

    :param cursor: A MSSQLCursor instance.
    :type cursor: MSSQLCursor
    :return: A dictionary representation of the result.
    :rtype: list[dict[str, str]]
    """
    column_names = [col[0] for col in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(column_names, row)))
    return results


class MSSqlDbConnection(DbBase[MSSQLConnection, ConnectionParams]):
    """A class for inspecting Microsoft SQL databases."""

    def __init__(self, connection_params: ConnectionParams):
        """
        Initialize an instance of MSSqlDbConnection and connect to a Microsoft SQL database.

        :param connection_params: The connection parameters for the Microsoft SQL database.
        :type connection_params: ConnectionParams
        :raises DbConnectionError: If the connection is unsuccessful.
        """

        super().__init__(connection_params)
        self._connection: MSSQLConnection | None = None
        self._connect()

    def _connect(self) -> None:
        """
        Open a connection to the Microsoft SQL database.

        :raises DbConnectionError: If the connection is unsuccessful.
        """

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self._connection_params.host},{self._connection_params.port};"
            f"DATABASE={self._connection_params.database};UID={self._connection_params.user};"
            f"PWD={self._connection_params.password};TrustServerCertificate=Yes"
        )
        try:
            self._connection = pyodbc.connect(conn_str)
        except pyodbc.Error as error:
            raise DbConnectionError(
                f"Connection to database failed: {error}", DatabaseType.MSSQL
            ) from error

    def close(self) -> None:
        """Close the connection to the Microsoft SQL database and set the connection to None."""

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

        if self._connection is not None and self._connection.closed:
            return ConnectionStatus.DISCONNECTED
        else:
            return ConnectionStatus.CONNECTED

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
                "Connection to SQL Server database was unexpectedly closed.",
                DatabaseType.MSSQL,
            )

        query: LiteralString = cast(
            LiteralString,
            f"""
        SELECT table_name
        FROM {self._connection_params.database}.information_schema.tables
        WHERE table_type = 'BASE TABLE';
        """,
        )

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            results = db_results_to_dict(cursor)
            return [r["table_name"] for r in results]

    def get_columns(self, table_name: str) -> list[DbColumn]:
        """
        Retrieve a list of columns in the table.

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
                "Connection to SQL Server database was unexpectedly closed.",
                DatabaseType.MSSQL,
            )

        query: LiteralString = cast(
            LiteralString,
            f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = N'{table_name}';
        """,
        )
        table_cols: list[DbColumn] = []

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            columns = db_results_to_dict(cursor)
            print(columns)
            for c in columns:
                table_cols.append(
                    DbColumn(
                        name=c["column_name"],
                        datatype=self._get_python_type(c["data_type"]),
                        is_nullable=c["is_nullable"].lower() == "yes",
                    )
                )

        return table_cols
