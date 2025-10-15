import os
import sys
import django

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from clickhouse_client.health_check import test_connection


def test_different_ports():
    """Тестируем подключение к разным портам"""
    ports_to_test = [8123, 8443, 9000, 9440]

    for port in ports_to_test:
        print(f"\nTesting port {port}...")
        os.environ['CLICKHOUSE_PORT'] = str(port)

        # Тестируем с SSL выключенным
        os.environ['CLICKHOUSE_SECURE'] = 'false'
        if test_connection():
            print(f"✅ Port {port} works with SSL disabled")
            return port

        # Тестируем с SSL включенным
        os.environ['CLICKHOUSE_SECURE'] = 'true'
        if test_connection():
            print(f"✅ Port {port} works with SSL enabled")
            return port

    print("❌ No working port found")
    return None


if __name__ == '__main__':
    working_port = test_different_ports()
    if working_port:
        print(f"\n🎉 Use port {working_port} in your .env file")