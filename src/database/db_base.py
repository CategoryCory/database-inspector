from abc import ABC, abstractmethod
from typing import Any

from src.database.models import DbColumn


class DbBase[TConnParams, TConnection](ABC):
    def __enter__(self) -> "DbBase":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    @property
    @abstractmethod
    def connection(self) -> TConnection | None:
        pass

    @abstractmethod
    def connect(self, connection_params: TConnParams) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def get_tables(self) -> list[str]:
        pass

    @abstractmethod
    def get_columns(self, table: str) -> list[DbColumn]:
        pass
