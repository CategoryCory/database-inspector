from src.database_inspector.database.db_base import DbBase
from src.database_inspector.infrastructure.types import DbSchema


def extract_db_schema(db_connection: DbBase) -> DbSchema:
    schema: DbSchema = {}

    tables = db_connection.get_tables()
    for t in tables:
        columns = db_connection.get_columns(t)
        schema[t] = columns

    return schema
