from src.database_inspector.infrastructure.enums import DatabaseType


class DbConnectionError(ConnectionError):
    """Custom exception raised when database connection fails."""

    def __init__(self, message: str, db_type: DatabaseType = None) -> None:
        """
        Initialize an instance of the DbConnectionError class.

        :param message: The error message.
        :type message: str
        :param db_type: The type of the database connection (MySQL, PostgreSQL, etc.).
        :type db_type: DatabaseType
        """

        self._message = message
        self._db_type = db_type
        super().__init__(self._message)

    def __str__(self) -> str:
        """
        Retrieve the string representation of the exception.

        :return: The string representation of the exception.
        :rtype: str
        """

        return f"{self._message} ({self._db_type})"
