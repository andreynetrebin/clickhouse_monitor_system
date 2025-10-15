import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from clickhouse_driver import Client as ClickhouseDriver
from clickhouse_driver.errors import Error as ClickhouseError

from .config import ClickHouseConfig
from .exceptions import (
    ClickHouseConnectionError,
    ClickHouseQueryError,
    ClickHouseTimeoutError
)

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Результат выполнения запроса"""
    data: List[Tuple]
    columns: List[str]
    execution_time: float
    rows_read: int = 0
    bytes_read: int = 0
    error: Optional[str] = None


class ClickHouseClient:
    """
    Клиент для работы с ClickHouse с поддержкой retry и error handling
    """

    def __init__(self, instance_name: str = 'default', max_retries: int = 3, retry_delay: float = 1.0):
        self.instance_name = instance_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Optional[ClickhouseDriver] = None
        self._config = ClickHouseConfig.get_connection_config(instance_name)

    def __enter__(self):
        """Контекстный менеджер для автоматического подключения"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер для автоматического отключения"""
        self.disconnect()

    def connect(self) -> None:
        """Установить подключение к ClickHouse"""
        if self._client is not None:
            return

        try:
            self._client = ClickhouseDriver(**self._config)
            logger.info(f"Connected to ClickHouse instance: {self.instance_name}")
        except Exception as e:
            raise ClickHouseConnectionError(
                f"Failed to connect to ClickHouse instance '{self.instance_name}': {e}"
            ) from e

    def disconnect(self) -> None:
        """Закрыть подключение"""
        if self._client is not None:
            try:
                self._client.disconnect()
                logger.debug(f"Disconnected from ClickHouse instance: {self.instance_name}")
            except Exception as e:
                logger.warning(f"Error disconnecting from ClickHouse: {e}")
            finally:
                self._client = None

    def execute_query(
            self,
            query: str,
            params: Optional[Dict] = None,
            with_column_types: bool = True
    ) -> QueryResult:
        """
        Выполнить запрос с поддержкой retry и обработкой ошибок

        Args:
            query: SQL запрос
            params: Параметры для запроса
            with_column_types: Возвращать информацию о колонках

        Returns:
            QueryResult с данными и метаинформацией
        """
        start_time = time.time()

        for attempt in range(self.max_retries + 1):
            try:
                self.connect()

                # Выполняем запрос
                result = self._client.execute(
                    query,
                    params=params,
                    with_column_types=with_column_types
                )

                execution_time = time.time() - start_time

                # Формируем результат
                if with_column_types and result:
                    data, columns_with_types = result
                    columns = [col[0] for col in columns_with_types]
                else:
                    data = result
                    columns = []

                # Получаем статистику выполнения если доступно
                rows_read = self._get_rows_read()
                bytes_read = self._get_bytes_read()

                logger.debug(
                    f"Query executed successfully in {execution_time:.3f}s "
                    f"(attempt {attempt + 1}): {query[:100]}..."
                )

                return QueryResult(
                    data=data,
                    columns=columns,
                    execution_time=execution_time,
                    rows_read=rows_read,
                    bytes_read=bytes_read
                )

            except ClickhouseError as e:
                execution_time = time.time() - start_time
                error_msg = f"ClickHouse error on attempt {attempt + 1}: {e}"

                if attempt == self.max_retries:
                    logger.error(f"Query failed after {self.max_retries} attempts: {error_msg}")
                    return QueryResult(
                        data=[],
                        columns=[],
                        execution_time=execution_time,
                        error=error_msg
                    )

                logger.warning(f"{error_msg}. Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)

            except Exception as e:
                execution_time = time.time() - start_time
                error_msg = f"Unexpected error on attempt {attempt + 1}: {e}"
                logger.error(error_msg)

                return QueryResult(
                    data=[],
                    columns=[],
                    execution_time=execution_time,
                    error=error_msg
                )

    def _get_rows_read(self) -> int:
        """Получить количество прочитанных строк из последнего запроса"""
        try:
            if self._client:
                result = self._client.execute('SELECT read_rows FROM system.query_log ORDER BY event_time DESC LIMIT 1')
                return result[0][0] if result else 0
        except Exception:
            pass
        return 0

    def _get_bytes_read(self) -> int:
        """Получить количество прочитанных байт из последнего запроса"""
        try:
            if self._client:
                result = self._client.execute(
                    'SELECT read_bytes FROM system.query_log ORDER BY event_time DESC LIMIT 1')
                return result[0][0] if result else 0
        except Exception:
            pass
        return 0

    def test_connection(self) -> bool:
        """Проверить подключение простым запросом"""
        try:
            result = self.execute_query('SELECT 1 as test')
            return result.error is None and len(result.data) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_server_info(self) -> Dict[str, Any]:
        """Получить информацию о сервере ClickHouse"""
        try:
            version_result = self.execute_query('SELECT version()')
            uptime_result = self.execute_query('SELECT uptime()')

            return {
                'version': version_result.data[0][0] if version_result.data else 'unknown',
                'uptime_seconds': uptime_result.data[0][0] if uptime_result.data else 0,
                'instance_name': self.instance_name,
            }
        except Exception as e:
            logger.error(f"Failed to get server info: {e}")
            return {
                'version': 'unknown',
                'uptime_seconds': 0,
                'instance_name': self.instance_name,
                'error': str(e)
            }


# Синглтон для быстрого доступа к дефолтному клиенту
_default_client: Optional[ClickHouseClient] = None


def get_default_client() -> ClickHouseClient:
    """Получить дефолтный клиент (singleton)"""
    global _default_client
    if _default_client is None:
        _default_client = ClickHouseClient()
    return _default_client