"""
enums.py
========

This module provides enum types that are used throughout the application.

Classes
-------

- **DatabaseType**: Provides values to indicate the type of database being used.
- **ConnectionStatus**: Provides values to indicate the status of the database connection.
"""

from enum import auto, Enum


class DatabaseType(Enum):
    """
    An enumeration representing the database type.

    This enum is used to indicate the type of database being used.
    """

    SQLITE = 0
    """A SQLite database type."""

    POSTGRES = auto()
    """A PostgreSQL database type."""

    MYSQL = auto()
    """A MySQL database type."""

    MSSQL = auto()
    """A Microsoft SQL server database type."""


class ConnectionStatus(Enum):
    """
    An enumeration representing the status of the connection to the database.

    This enum is used to indicate the current state of a database connection. Commonly
    used in functions to ensure that the database connection is still open before performing
    SQL queries.
    """

    CONNECTED = 0
    """The connection is currently open and _connection is not None."""

    DISCONNECTED = auto()
    """The connection is currently closed and _connection is None."""

    UNKNOWN = auto()
    """
    Typically used to represent a situation where the connection is currently
    open but _connection is None, or vice versa. 
    """
