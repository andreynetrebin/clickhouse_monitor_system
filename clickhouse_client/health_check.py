import logging
from typing import Optional  # ← ДОБАВИТЬ ЭТУ СТРОКУ
from clickhouse_client.config import ClickHouseConfig
from clickhouse_client.exceptions import ClickHouseConnectionError

logger = logging.getLogger(__name__)


def test_connection(instance_name: str = 'default') -> bool:
    """
    Проверить подключение к ClickHouse инстансу
    Возвращает True если подключение успешно, False в случае ошибки
    """
    try:
        from clickhouse_driver import Client

        config = ClickHouseConfig.get_connection_config(instance_name)
        client = Client(**config)

        # Простой запрос для проверки подключения
        result = client.execute('SELECT 1 as test')

        client.disconnect()

        if result and result[0][0] == 1:
            logger.info(f"ClickHouse connection test successful for {instance_name}")
            return True

    except Exception as e:
        logger.error(f"ClickHouse connection test failed for {instance_name}: {e}")

    return False


def get_clickhouse_version(instance_name: str = 'default') -> Optional[str]:
    """
    Получить версию ClickHouse сервера
    """
    try:
        from clickhouse_driver import Client

        config = ClickHouseConfig.get_connection_config(instance_name)
        client = Client(**config)

        result = client.execute('SELECT version()')
        version = result[0][0] if result else None

        client.disconnect()
        return version

    except Exception as e:
        logger.error(f"Failed to get ClickHouse version for {instance_name}: {e}")
        return None