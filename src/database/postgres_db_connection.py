import psycopg
from psycopg import Connection
from typing import TypedDict, LiteralString

from src.database.db_base import DbBase
from src.database.models import DbColumn, python_type_map


class PostgresConnParams(TypedDict, total=False):
    dbname: str
    user: str
    password: str
    port: int
    host: str


class PostgresDbConnection(DbBase[PostgresConnParams, Connection]):
    def __init__(self) -> None:
        self._connection: Connection | None = None

    def connect(self, connection_params: PostgresConnParams) -> None:
        if not self._connection:
            self._connection = psycopg.connect(**connection_params)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    @property
    def connection(self) -> Connection | None:
        return self._connection

    def get_tables(self) -> list[str]:
        query: LiteralString = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """

        if self._connection is None:
            raise ConnectionError(
                "Connection to PostgreSQL database was not established"
            )

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table_name: str) -> list[DbColumn]:
        query: LiteralString = """
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns
        WHERE table_name = %s
        """

        table_cols: list[DbColumn] = []

        db_type_to_python_type: dict[str, type] = {
            db_type: python_type
            for python_type, db_types in python_type_map.items()
            for db_type in db_types
        }

        if self._connection is None:
            raise ConnectionError(
                "Connection to PostgreSQL database was not established"
            )

        with self._connection.cursor() as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            for c in columns:
                table_cols.append(
                    DbColumn(
                        name=c[0],
                        datatype=db_type_to_python_type.get(c[1].upper(), None),
                        is_nullable=False if c[2] == "NO" else True,
                    )
                )

        return table_cols
