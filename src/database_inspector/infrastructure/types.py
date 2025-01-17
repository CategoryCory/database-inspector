"""This module defines common type aliases used throughout the application."""

from mysql.connector import MySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from os import PathLike

from database_inspector.infrastructure.models import DbColumn


type SqliteConnParams = str | bytes | PathLike[str] | PathLike[bytes]
"""Represents the connection parameters for a SQLite database connection."""

type MySQLConnectionTypes = (
    MySQLConnection | PooledMySQLConnection | MySQLConnectionAbstract
)
"""Represents the MySQL connection type returned by `mysql.connector.connect()`."""

type DbSchema = dict[str, list[DbColumn]]
"""
Represents the schema of a given database. The keys represent the table names,
and the values represent a list of columns found in that table.
"""
