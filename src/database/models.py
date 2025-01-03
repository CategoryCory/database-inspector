from dataclasses import dataclass
from datetime import datetime


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
    datetime: {
        "DATETIME",
        "TIMESTAMP WITHOUT TIME ZONE",
    },
    bytes: {
        "BLOB",
        "BYTEA",
        "VARBINARY",
    },
}


@dataclass(frozen=True)
class DbColumn:
    name: str
    datatype: type | None
    is_nullable: bool
