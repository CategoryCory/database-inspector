import re
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Self

from database_inspector.infrastructure.enums import ConnectionStatus
from database_inspector.infrastructure.models import DbColumn, python_type_map


class DbBase[ConnectionT, ConnParamsT](ABC):
    """
    An abstract base class for database connections.

    This class defines an interface for interacting with and inspecting
    databases. Concrete implementations should inherit from this class.
    """

    _connection: ConnectionT | None

    _db_type_to_python_type: dict[str, type] = {
        db_type: python_type
        for python_type, db_types in python_type_map.items()
        for db_type in db_types
    }

    def __init__(self, conn_params: ConnParamsT) -> None:
        """
        Initialize a database connection.

        This constructor is meant to be called by subclasses using
        :func: `super().__init__()`.

        :param conn_params: The connection parameters.
        :type conn_params: ConnParamsT
        """

        self._connection_params = conn_params

    def __enter__(self) -> Self:
        """
        Open a database connection and return the instance.

        :return: A DbBase instance.
        :rtype: DbBase
        """

        return self

    def __exit__(
        self,
        exc_type: type | Any,
        exc_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Close the database connection and handle any exceptions.

        :param exc_type: The exception type, if an exception occurred.
        :type exc_type: type | Any
        :param exc_value: The exception instance, if an exception occurred.
        :type exc_value: Exception | None
        :param traceback: The traceback object, if an exception occurred.
        :type traceback: TracebackType | None
        """

        self.close()

    @property
    def connection(self) -> ConnectionT | None:
        """
        Retrieve the database connection instance. If the connection is
        closed, the connection will be None.

        :return: The DbBase connection, or None if the connection is closed.
        :rtype: ConnectionT | None
        """

        return self._connection

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""

    @abstractmethod
    def get_connection_status(self) -> ConnectionStatus:
        """
        Retrieve the current database connection status.

        :return: The status of the current database connection.
        :rtype: ConnectionStatus
        """

    @abstractmethod
    def get_tables(self) -> list[str]:
        """
        Retrieve a list of tables in the database.

        :return: A list of tables in the database.
        :rtype: list[str]
        """

    @abstractmethod
    def get_columns(self, table_name: str) -> list[DbColumn]:
        """
        Retrieve a list of columns in a table.

        :param table_name: The table to retrieve column information from.
        :type table_name: str
        :return: A list of column information in `table_name`.
        :rtype: list[DbColumn]
        """

    def _get_python_type(self, db_type: str) -> type | None:
        """
        Retrieve the corresponding python type for a given database type.

        :param db_type: The database type.
        :type db_type: str
        :return: The corresponding python type, or None if the type is not found.
        :rtype: type | None
        """

        # Convert db_type to a normalized value
        normalized_db_type = re.sub(r"\(.*\)", "", db_type).strip()
        normalized_db_type = re.sub(
            r"\sUNSIGNED|\sZEROFILL", "", normalized_db_type, flags=re.IGNORECASE
        )

        return self._db_type_to_python_type.get(normalized_db_type.upper(), None)
