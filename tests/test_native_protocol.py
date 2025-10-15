import os
import sys
import django

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from clickhouse_client import ClickHouseClient


def test_native_protocol():
    """Тестируем нативный TCP протокол на порту 9000"""
    print("=== Testing Native TCP Protocol (port 9000) ===")

    # Принудительно устанавливаем настройки для нативного протокола
    os.environ['CLICKHOUSE_PORT'] = '9000'
    os.environ['CLICKHOUSE_SECURE'] = 'false'

    try:
        with ClickHouseClient() as client:
            # Тест подключения
            if client.test_connection():
                print("✅ Native protocol connection successful")

                # Тест простого запроса
                result = client.execute_query('SELECT 1 as number, now() as current_time')
                if result.error:
                    print(f"❌ Query failed: {result.error}")
                else:
                    print(f"✅ Query successful: {result.data}")

                # Информация о сервере
                server_info = client.get_server_info()
                print(f"📊 Server version: {server_info['version']}")
                print(f"⏱️ Server uptime: {server_info['uptime_seconds']} seconds")

            else:
                print("❌ Native protocol connection failed")

    except Exception as e:
        print(f"💥 Unexpected error: {e}")


if __name__ == '__main__':
    test_native_protocol()