import sqlite3
from os import PathLike
from sqlite3 import Connection

from src.database.db_base import DbBase
from src.database.models import DbColumn, python_type_map


type SqliteConnParams = str | bytes | PathLike[str] | PathLike[bytes]


class SqliteDbConnection(DbBase[SqliteConnParams, Connection]):
    def __init__(self) -> None:
        self._connection: Connection | None = None

    def connect(self, connection_params: SqliteConnParams) -> None:
        if not self._connection:
            self._connection = sqlite3.connect(connection_params)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    @property
    def connection(self) -> Connection | None:
        return self._connection

    def get_tables(self) -> list[str]:
        if self._connection is None:
            raise ConnectionError("Connection to SQLite database was not established")

        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"

        cursor = self._connection.execute(query)
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table_name: str) -> list[DbColumn]:
        if self._connection is None:
            raise ConnectionError("Connection to SQLite database was not established")

        query = f"PRAGMA table_info('{table_name}');"

        table_cols: list[DbColumn] = []

        db_type_to_python_type: dict[str, type] = {
            db_type: python_type
            for python_type, db_types in python_type_map.items()
            for db_type in db_types
        }

        cursor = self._connection.execute(query)
        columns = cursor.fetchall()
        for c in columns:
            table_cols.append(
                DbColumn(
                    name=c[1],
                    datatype=db_type_to_python_type.get(c[2].upper(), None),
                    is_nullable=not c[3],
                )
            )

        return table_cols
