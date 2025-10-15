import os
import logging
from typing import Dict, Any, Optional
from django.conf import settings

logger = logging.getLogger(__name__)


class ClickHouseConfig:
    """
    Конфигурация для подключения к удаленным инстансам ClickHouse
    """

    # Дефолтные настройки для нативного TCP подключения
    DEFAULT_CONFIG = {
        'host': 'localhost',
        'port': 9000,  # ← НАТИВНЫЙ TCP ПОРТ
        'user': 'default',
        'password': '',
        'database': 'default',
        'secure': False,  # Без SSL для нативного протокола
        'verify': False,
        'connect_timeout': 10,
        'send_receive_timeout': 300,
        'sync_request_timeout': 300,
        'compression': False,
        # Дополнительные настройки для нативного протокола
        'settings': {
            'use_numpy': False,
            'strings_encoding': 'utf8',
        }
    }

    @classmethod
    def get_connection_config(cls, instance_name: str = 'default') -> Dict[str, Any]:
        """
        Получить конфигурацию для подключения к конкретному инстансу
        """
        # Сначала пытаемся получить из environment variables
        env_config = cls._get_config_from_env()

        # Затем из настроек Django
        django_config = cls._get_config_from_django(instance_name)

        # Объединяем конфигурации (env имеет высший приоритет)
        config = {**cls.DEFAULT_CONFIG, **django_config, **env_config}

        # Валидация обязательных параметров
        cls._validate_config(config)

        logger.debug(f"ClickHouse config for {instance_name}: {cls._mask_password(config)}")
        return config

    @classmethod
    def _get_config_from_env(cls) -> Dict[str, Any]:
        """Получить конфигурацию из environment variables"""
        config = {}

        # Базовые параметры подключения
        if host := os.getenv('CLICKHOUSE_HOST'):
            config['host'] = host
        if port := os.getenv('CLICKHOUSE_PORT'):
            config['port'] = int(port)
        if user := os.getenv('CLICKHOUSE_USER'):
            config['user'] = user
        if password := os.getenv('CLICKHOUSE_PASSWORD'):
            config['password'] = password
        if database := os.getenv('CLICKHOUSE_DATABASE'):
            config['database'] = database

        # Для нативного протокола SSL обычно не используется
        if secure := os.getenv('CLICKHOUSE_SECURE'):
            config['secure'] = secure.lower() == 'true'
        else:
            config['secure'] = False  # Принудительно отключаем для порта 9000

        if verify := os.getenv('CLICKHOUSE_VERIFY_SSL'):
            config['verify'] = verify.lower() == 'true'
        else:
            config['verify'] = False  # Принудительно отключаем для порта 9000

        # Таймауты
        if timeout := os.getenv('CLICKHOUSE_CONNECT_TIMEOUT'):
            config['connect_timeout'] = int(timeout)
        if timeout := os.getenv('CLICKHOUSE_SEND_TIMEOUT'):
            config['send_receive_timeout'] = int(timeout)

        return config

    @classmethod
    def _get_config_from_django(cls, instance_name: str) -> Dict[str, Any]:
        """Получить конфигурацию из настроек Django"""
        config = {}

        try:
            # Пытаемся получить из CLICKHOUSE_CONFIG в settings.py
            django_config = getattr(settings, 'CLICKHOUSE_CONFIG', {})
            if instance_name in django_config:
                config = django_config[instance_name].copy()
        except Exception as e:
            logger.warning(f"Error reading Django config: {e}")

        return config

    @classmethod
    def _validate_config(cls, config: Dict[str, Any]):
        """Валидация конфигурации"""
        if not config.get('host'):
            raise ValueError("ClickHouse host is required")

        if config.get('port') and not (1 <= config['port'] <= 65535):
            raise ValueError(f"Invalid port: {config['port']}")

    @classmethod
    def _mask_password(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """Скрыть пароль в логах"""
        masked = config.copy()
        if 'password' in masked and masked['password']:
            masked['password'] = '***'
        return masked