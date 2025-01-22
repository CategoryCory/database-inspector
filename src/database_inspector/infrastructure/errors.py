"""
errors.py
=========

This module implements custom error types used throughout the application.

Exceptions
----------

- **DatabaseError**: The base class for database errors.
- **DatabaseConnectionError**: Custom exception for errors related to the database connection.
- **DatabaseQueryError**: Custom exception for errors related to the database query.
"""

from database_inspector.infrastructure.enums import DatabaseType


class DatabaseError(Exception):
    """Base class for database errors."""

    def __init__(self, message: str, db_type: DatabaseType) -> None:
        """
        Initialize an instance of the DatabaseError class.

        :param message: The error message.
        :type message: str
        :param db_type: The type of the database connection (MySQL, PostgreSQL, etc.).
        :type db_type: DatabaseType
        """

        self.message = message
        self.db_type = db_type
        super().__init__(self.message)

    def __str__(self) -> str:
        """
        Retrieve the string representation of the exception.

        :return: The string representation of the exception.
        :rtype: str
        """

        return f"{self.message} ({self.db_type.name})"


class DatabaseConnectionError(DatabaseError):
    """Custom exception raised when database connection fails."""


class DatabaseTableNotFoundError(DatabaseError):
    """Custom exception raised when a database table does not exist."""
