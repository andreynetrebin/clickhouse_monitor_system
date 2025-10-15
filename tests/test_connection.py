import os
import sys
import django

# Добавляем корневую директорию в Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()


def debug_env():
    """Проверка environment variables"""
    print("=== Environment Variables ===")
    env_vars = [
        'CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER',
        'CLICKHOUSE_DATABASE', 'CLICKHOUSE_SECURE'
    ]

    for var in env_vars:
        value = os.getenv(var, 'NOT SET')
        print(f"   {var}: {value}")


def debug_config():
    """Простая диагностика конфигурации"""
    from clickhouse_client.config import ClickHouseConfig

    print("\n=== ClickHouse Configuration Debug ===")

    try:
        config = ClickHouseConfig.get_connection_config()
        masked_config = ClickHouseConfig._mask_password(config)
        print("✅ Configuration loaded successfully:")
        for key, value in masked_config.items():
            print(f"   {key}: {value}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")


def test_connection():
    """Тест подключения"""
    from clickhouse_client.health_check import test_connection, get_clickhouse_version

    print("\n=== Connection Test ===")

    if test_connection():
        version = get_clickhouse_version()
        print(f"✅ Connection successful! ClickHouse version: {version}")
    else:
        print("❌ Connection failed")


if __name__ == '__main__':
    debug_env()
    debug_config()
    test_connection()