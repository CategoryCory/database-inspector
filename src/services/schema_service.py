from src.database.db_base import DbBase
from src.database.models import DbColumn
from src.database.postgres_db_connection import PostgresConnParams
from src.database.sqlite_db_connection import SqliteConnParams


type ConnParams = PostgresConnParams | SqliteConnParams


# TODO: Refactor the connection_params type annotation
def extract_db_schema[ConnParams](
    db_connection: DbBase, connection_params: ConnParams
) -> dict[str, list[DbColumn]]:
    schema: dict[str, list[DbColumn]] = {}

    with db_connection:
        db_connection.connect(connection_params)

        tables = db_connection.get_tables()
        for t in tables:
            columns = db_connection.get_columns(t)
            schema[t] = columns

    return schema
