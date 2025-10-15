import os
import sys
import django
from django.core.management.base import BaseCommand
from clickhouse_client import ClickHouseClient, system_queries


class Command(BaseCommand):
    help = 'Диагностика реальных данных из system.query_log'

    def add_arguments(self, parser):
        parser.add_argument(
            '--lookback-minutes',
            type=int,
            default=60,
            help='Период анализа в минутах'
        )
        parser.add_argument(
            '--threshold-ms',
            type=int,
            default=1000,
            help='Порог медленных запросов'
        )

    def handle(self, *args, **options):
        lookback_minutes = options['lookback_minutes']
        threshold_ms = options['threshold_ms']

        self.stdout.write(f"=== Query Log Analysis (last {lookback_minutes}min, >{threshold_ms}ms) ===")

        with ClickHouseClient() as client:
            # 1. Проверяем общее количество запросов
            count_sql = f"""
            SELECT count()
            FROM system.query_log 
            WHERE event_time > now() - INTERVAL {lookback_minutes} MINUTE
            AND type = 'QueryFinish'
            AND is_initial_query = 1
            AND query_duration_ms > {threshold_ms}
            """

            result = client.execute_query(count_sql)
            real_count = result.data[0][0] if result.data else 0
            self.stdout.write(f"\n1. Real slow queries count: {real_count}")

            # 2. Проверяем наш запрос с лимитом
            limited_sql = system_queries.get_slow_queries(
                threshold_ms=threshold_ms,
                lookback_minutes=lookback_minutes,
                limit=100
            )

            result = client.execute_query(limited_sql)
            limited_count = len(result.data)
            self.stdout.write(f"2. With LIMIT 100: {limited_count} queries")

            # 3. Показываем примеры реальных медленных запросов
            if result.data:
                self.stdout.write(f"\n3. Sample slow queries (showing 5 of {limited_count}):")
                for i, row in enumerate(result.data[:5]):
                    query_id, query, start_time, duration_ms, read_rows, read_bytes, *_ = row
                    self.stdout.write(f"   {i + 1}. {duration_ms}ms, {read_rows} rows: {query[:80]}...")

            # 4. Проверяем без лимита (ограничиваемся 1000 для производительности)
            unlimited_sql = f"""
            SELECT count()
            FROM (
                {limited_sql.replace('LIMIT 100', '')}
            )
            """

            result = client.execute_query(unlimited_sql)
            unlimited_count = result.data[0][0] if result.data else 0
            self.stdout.write(f"\n4. Without LIMIT: {unlimited_count} queries")

            # 5. Рекомендации
            self.stdout.write(f"\n5. Recommendations:")
            if unlimited_count > 100:
                self.stdout.write(f"   - Use --limit {unlimited_count} to capture all slow queries")
                self.stdout.write(f"   - Or use --limit 500 for balanced performance")
            else:
                self.stdout.write(f"   - Current limit 100 is sufficient")