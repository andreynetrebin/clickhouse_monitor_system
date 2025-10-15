import os
import sys
import django

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from clickhouse_client import ClickHouseClient, system_queries


def test_system_queries():
    """Тестируем системные запросы"""
    print("=== Testing System Queries ===\n")

    with ClickHouseClient() as client:
        # 0. Диагностика структуры system.processes
        print("0. Checking system.processes structure...")
        columns_sql = system_queries.get_system_processes_columns()
        result = client.execute_query(columns_sql)
        if result.error:
            print(f"   ❌ Columns query failed: {result.error}")
        else:
            print(f"   ✅ system.processes has {len(result.data)} columns")
            for name, type in result.data[:5]:  # Показываем первые 5 колонок
                print(f"      {name}: {type}")

        # 1. Тест медленных запросов
        print("\n1. Testing slow queries...")
        slow_queries_sql = system_queries.get_slow_queries(
            threshold_ms=1000,
            lookback_minutes=10,
            limit=10
        )
        result = client.execute_query(slow_queries_sql)
        if result.error:
            print(f"   ❌ Slow queries failed: {result.error}")
        else:
            print(f"   ✅ Found {len(result.data)} slow queries")
            # Показываем пример медленного запроса
            if result.data:
                query_id, query, start_time, duration_ms, read_rows, *_ = result.data[0]
                print(f"      Example: {duration_ms}ms, {read_rows} rows - {query[:100]}...")

        # 2. Тест текущих запросов (исправленная версия)
        print("\n2. Testing current queries (fixed)...")
        current_queries_sql = system_queries.get_current_queries(threshold_ms=1000)
        result = client.execute_query(current_queries_sql)
        if result.error:
            print(f"   ❌ Current queries failed: {result.error}")
        else:
            print(f"   ✅ Found {len(result.data)} current long-running queries")
            if result.data:
                for query_id, query, duration_ms, read_rows, read_bytes, *_ in result.data[:3]:
                    print(f"      {duration_ms}ms - {query[:80]}...")

        # 3. Тест системных метрик
        print("\n3. Testing system metrics...")
        metrics_sql = system_queries.get_system_metrics()
        result = client.execute_query(metrics_sql)
        if result.error:
            print(f"   ❌ System metrics failed: {result.error}")
        else:
            print(f"   ✅ Found {len(result.data)} system metrics")
            # Показываем несколько примеров
            for metric, value, description in result.data[:5]:
                print(f"      {metric}: {value}")

        # 4. Тест статистики query_log
        print("\n4. Testing query log stats...")
        stats_sql = system_queries.get_query_log_stats(lookback_hours=1)
        result = client.execute_query(stats_sql)
        if result.error:
            print(f"   ❌ Query log stats failed: {result.error}")
        else:
            stats = result.data[0] if result.data else []
            if stats:
                total, slow, avg_ms, p95_ms, max_ms, total_rows, total_bytes = stats
                size_mb = total_bytes / (1024 * 1024) if total_bytes else 0
                print(f"   ✅ Stats: {total} total, {slow} slow, avg {avg_ms}ms, p95 {p95_ms}ms")
                print(f"      Data read: {total_rows} rows, {size_mb:.1f} MB")

        # 5. Тест информации о таблицах (проверим другие базы)
        print("\n5. Testing tables info for all databases...")
        # Сначала получим список баз данных
        dbs_sql = "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')"
        db_result = client.execute_query(dbs_sql)
        if db_result.error:
            print(f"   ❌ Databases query failed: {db_result.error}")
        else:
            for db_name, in db_result.data:
                tables_sql = system_queries.get_tables_info(database=db_name)
                result = client.execute_query(tables_sql)
                if result.error:
                    print(f"   ❌ Tables info for {db_name} failed: {result.error}")
                else:
                    print(f"   ✅ Database '{db_name}': {len(result.data)} tables")
                    # Показываем несколько самых больших таблиц
                    for table_name, engine, total_rows, total_bytes, *_ in result.data[:2]:
                        size_mb = total_bytes / (1024 * 1024) if total_bytes else 0
                        print(f"      {table_name}: {total_rows} rows, {size_mb:.1f} MB")


if __name__ == '__main__':
    test_system_queries()