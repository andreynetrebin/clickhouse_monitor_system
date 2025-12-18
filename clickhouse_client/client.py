import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from clickhouse_driver import Client as ClickhouseDriver
from clickhouse_driver.errors import Error as ClickhouseError

from .config import ClickHouseConfig
from .exceptions import ClickHouseConnectionError

logger = logging.getLogger(__name__)


@dataclass
class ClickHouseQueryResult:
    """Результат выполнения запроса"""
    data: List[Tuple]
    columns: List[str]
    execution_time: float
    rows_read: int = 0
    bytes_read: int = 0
    error: Optional[str] = None


class ClickHouseClient:
    """
    Потокобезопасный клиент для работы с ClickHouse.
    Каждый экземпляр создаёт **отдельное подключение**.
    """

    def __init__(self, instance_name: str = 'default', max_retries: int = 3, retry_delay: float = 1.0):
        self.instance_name = instance_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Optional[ClickhouseDriver] = None
        self._config = ClickHouseConfig.get_connection_config(instance_name)

    def __enter__(self):
        """Контекстный менеджер: создаёт подключение при входе"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер: закрывает подключение при выходе"""
        self.disconnect()

    def connect(self) -> None:
        """Установить подключение к ClickHouse"""
        if self._client is not None:
            return

        try:
            self._client = ClickhouseDriver(**self._config)
            logger.debug(f"Connected to ClickHouse: {self.instance_name}")
        except Exception as e:
            raise ClickHouseConnectionError(
                f"Failed to connect to ClickHouse '{self.instance_name}': {e}"
            ) from e

    def disconnect(self) -> None:
        """Закрыть подключение"""
        if self._client is not None:
            try:
                self._client.disconnect()
                logger.debug(f"Disconnected from ClickHouse: {self.instance_name}")
            except Exception as e:
                logger.warning(f"Error disconnecting: {e}")
            finally:
                self._client = None  # ← ВАЖНО: сначала disconnect(), потом = None

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        with_column_types: bool = True
    ) -> ClickHouseQueryResult:
        """
        Выполнить запрос с поддержкой retry и обработкой ошибок.
        """
        start_time = time.time()

        for attempt in range(self.max_retries + 1):
            try:
                self.connect()

                result = self._client.execute(
                    query,
                    params=params,
                    with_column_types=with_column_types
                )

                execution_time = time.time() - start_time

                if with_column_types and result:
                    data, columns_with_types = result
                    columns = [col[0] for col in columns_with_types]
                else:
                    data = result
                    columns = []

                return ClickHouseQueryResult(
                    data=data,
                    columns=columns,
                    execution_time=execution_time,
                    rows_read=0,
                    bytes_read=0,
                    error=None
                )

            except ClickhouseError as e:
                execution_time = time.time() - start_time
                error_msg = f"ClickHouse error (attempt {attempt + 1}): {e}"

                if attempt == self.max_retries:
                    logger.error(f"Query failed after {self.max_retries + 1} attempts: {error_msg}")
                    return ClickHouseQueryResult(
                        data=[],
                        columns=[],
                        execution_time=execution_time,
                        error=error_msg
                    )

                logger.warning(f"{error_msg}. Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)

            except Exception as e:
                execution_time = time.time() - start_time
                error_msg = f"Unexpected error: {e}"
                logger.error(error_msg)
                return QueryResult(
                    data=[],
                    columns=[],
                    execution_time=execution_time,
                    error=error_msg
                )

    def test_connection(self) -> bool:
        """Проверить подключение простым запросом"""
        try:
            result = self.execute_query('SELECT 1')
            return result.error is None and len(result.data) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False