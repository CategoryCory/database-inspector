from dataclasses import dataclass
from datetime import date, datetime


python_type_map: dict[type, set[str]] = {
    str: {
        "TEXT",
        "CHAR",
        "VARCHAR",
        "NVARCHAR",
        "CLOB",
        "CHARACTER VARYING",
    },
    int: {
        "INT",
        "INTEGER",
        "SMALLINT",
        "BIGINT",
        "SERIAL",
    },
    float: {
        "REAL",
        "DOUBLE PRECISION",
        "NUMERIC",
        "DECIMAL",
    },
    bool: {
        "BOOLEAN",
    },
    date: {
        "DATE",
    },
    datetime: {
        "DATETIME",
        "DATETIME2",
        "TIMESTAMP WITHOUT TIME ZONE",
    },
    bytes: {
        "BLOB",
        "BYTEA",
        "VARBINARY",
    },
}
"""Dictionary mapping used to map between database types and Python types."""


@dataclass(frozen=True, slots=True)
class DbColumn:
    """Represents information about a database column."""

    name: str
    """The name of the column."""

    datatype: type | None
    """The data type of the column."""

    is_nullable: bool
    """Indicates whether the column is nullable."""


@dataclass(frozen=True, slots=True)
class ConnectionParams:
    """Represents information about a database's connection parameters."""

    user: str
    """The database connection's username."""

    password: str
    """The database connection's password.'"""

    host: str
    """The IP address or domain name of the database server."""

    database: str
    """The name of the database to use for the connection."""

    port: int
    """The port number of the database server."""
