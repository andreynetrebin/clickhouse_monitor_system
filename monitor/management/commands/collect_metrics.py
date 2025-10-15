import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from clickhouse_client import ClickHouseClient, system_queries
from monitor.models import ClickHouseInstance, QueryLog
from query_lab.models import SlowQuery

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Сбор метрик производительности из ClickHouse'

    def add_arguments(self, parser):
        parser.add_argument(
            '--instance',
            type=str,
            default='default',
            help='Имя инстанса ClickHouse для мониторинга'
        )
        parser.add_argument(
            '--lookback-minutes',
            type=int,
            default=5,
            help='Период сбора данных в минутах'
        )
        parser.add_argument(
            '--threshold-ms',
            type=int,
            default=1000,
            help='Порог для медленных запросов в миллисекундах'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=1000,  # ← УВЕЛИЧИВАЕМ ЛИМИТ
            help='Максимальное количество запросов для сбора'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Тестовый запуск без сохранения данных'
        )

    def handle(self, *args, **options):
        instance_name = options['instance']
        lookback_minutes = options['lookback_minutes']
        threshold_ms = options['threshold_ms']
        dry_run = options['dry_run']

        self.stdout.write(
            self.style.SUCCESS(
                f'Starting metrics collection for {instance_name} '
                f'(last {lookback_minutes} minutes, threshold: {threshold_ms}ms)'
            )
        )

        try:
            # Получаем или создаем инстанс в базе
            clickhouse_instance, created = ClickHouseInstance.objects.get_or_create(
                name=instance_name,
                defaults={
                    'host': 'configured_in_env',
                    'port': 9000,
                    'username': 'default',  # ← ИСПРАВЛЕНО
                    'is_active': True,
                }
            )

            if created:
                self.stdout.write(
                    self.style.WARNING(f'Created new instance: {instance_name}')
                )

            # Собираем метрики
            stats = self.collect_metrics(
                clickhouse_instance,
                lookback_minutes,
                threshold_ms,
                options['limit'],  # ← ДОБАВЛЯЕМ ЛИМИТ
                dry_run
            )

            # Выводим отчет
            self.print_report(stats, dry_run)

        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            self.stdout.write(
                self.style.ERROR(f'Collection failed: {e}')
            )

    def collect_metrics(self, instance, lookback_minutes, threshold_ms, limit, dry_run=False):
        """
        Основной метод сбора метрик
        """
        stats = {
            'total_queries': 0,
            'slow_queries': 0,
            'new_slow_queries': 0,
            'errors': 0
        }

        with ClickHouseClient(instance.name) as client:
            # 1. Собираем медленные запросы из query_log
            slow_queries = self.collect_slow_queries(
                client, instance, lookback_minutes, threshold_ms, limit, dry_run
            )
            stats['slow_queries'] = len(slow_queries)

            # 2. Создаем записи в лаборатории для новых медленных запросов
            if not dry_run:
                new_slow_queries = self.create_slow_query_records(slow_queries)
                stats['new_slow_queries'] = new_slow_queries

            # 3. Собираем статистику по всем запросам
            query_stats = self.collect_query_stats(client, lookback_minutes)
            stats['total_queries'] = query_stats.get('total_queries', 0)

            # 4. Собираем системные метрики (для будущего использования)
            system_metrics = self.collect_system_metrics(client)

        return stats

    def collect_slow_queries(self, client, instance, lookback_minutes, threshold_ms, limit, dry_run):
        """
        Сбор медленных запросов из system.query_log
        """
        slow_queries_sql = system_queries.get_slow_queries(
            threshold_ms=threshold_ms,
            lookback_minutes=lookback_minutes,
            limit=limit  # ← ПЕРЕДАЕМ ЛИМИТ
        )

        result = client.execute_query(slow_queries_sql)
        if result.error:
            logger.error(f"Failed to collect slow queries: {result.error}")
            self.stdout.write(self.style.ERROR(f'Slow queries error: {result.error}'))
            return []

        slow_queries = []
        for row in result.data:
            try:
                query_data = self.parse_query_log_row(row, instance)
                if not dry_run:
                    # Сохраняем в базу
                    query_log = self.save_query_log(query_data)
                    slow_queries.append(query_log)
                else:
                    slow_queries.append(query_data)

            except Exception as e:
                logger.error(f"Error processing query row: {e}")
                stats['errors'] += 1

        return slow_queries

    def parse_query_log_row(self, row, instance):
        """
        Парсинг строки из system.query_log
        """
        (
            query_id, query, query_start_time, query_duration_ms,
            read_rows, read_bytes, memory_usage, user, client_name,
            databases, tables, columns, normalized_query_hash
        ) = row

        # Преобразуем naive datetime в aware datetime
        from django.utils import timezone
        if query_start_time and timezone.is_naive(query_start_time):
            query_start_time = timezone.make_aware(query_start_time)

        return {
            'query_id': query_id,
            'clickhouse_instance': instance,
            'query_text': query,
            'normalized_query_hash': normalized_query_hash or '',
            'user': user,
            'duration_ms': float(query_duration_ms),
            'read_rows': read_rows or 0,
            'read_bytes': read_bytes or 0,
            'memory_usage': memory_usage or 0,
            'query_start_time': query_start_time,  # ← Теперь aware datetime
            'is_slow': True,
            'is_initial': True
        }

    def save_query_log(self, query_data):
        """
        Сохранение QueryLog в базу с обработкой дубликатов
        """
        try:
            # Пытаемся найти существующую запись
            query_log, created = QueryLog.objects.get_or_create(
                query_id=query_data['query_id'],
                defaults=query_data
            )

            if not created:
                # Обновляем существующую запись если нужно
                query_log.duration_ms = query_data['duration_ms']
                query_log.read_rows = query_data['read_rows']
                query_log.read_bytes = query_data['read_bytes']
                query_log.memory_usage = query_data['memory_usage']
                query_log.save()

            return query_log

        except Exception as e:
            logger.error(f"Failed to save QueryLog: {e}")
            raise

    def create_slow_query_records(self, slow_queries):
        """
        Создание записей в лаборатории для новых медленных запросов
        """
        new_records = 0

        for query_log in slow_queries:
            # Проверяем, нет ли уже такой записи в лаборатории
            if not SlowQuery.objects.filter(query_log=query_log).exists():
                SlowQuery.objects.create(query_log=query_log)
                new_records += 1

        return new_records

    def collect_query_stats(self, client, lookback_minutes):
        """
        Сбор общей статистики по запросам
        """
        stats_sql = system_queries.get_query_log_stats(
            lookback_hours=lookback_minutes // 60 or 1
        )

        result = client.execute_query(stats_sql)
        if result.error:
            logger.error(f"Failed to collect query stats: {result.error}")
            return {}

        if result.data:
            stats = result.data[0]
            return {
                'total_queries': stats[0],
                'slow_queries': stats[1],
                'avg_duration_ms': stats[2],
                'p95_duration_ms': stats[3],
                'max_duration_ms': stats[4],
                'total_rows_read': stats[5],
                'total_bytes_read': stats[6]
            }

        return {}

    def collect_system_metrics(self, client):
        """
        Сбор системных метрик (заглушка для будущего использования)
        """
        # Пока просто собираем, но не сохраняем
        metrics_sql = system_queries.get_system_metrics()
        result = client.execute_query(metrics_sql)

        if result.error:
            logger.warning(f"Failed to collect system metrics: {result.error}")
            return {}

        return {metric: value for metric, value, _ in result.data}

    def print_report(self, stats, dry_run):
        """
        Вывод отчета о выполнении
        """
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("COLLECTION REPORT")
        self.stdout.write("=" * 50)

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN - No data saved"))

        self.stdout.write(f"Total queries processed: {stats['total_queries']}")
        self.stdout.write(f"Slow queries found: {stats['slow_queries']}")
        self.stdout.write(f"New slow queries for lab: {stats['new_slow_queries']}")

        if stats['errors'] > 0:
            self.stdout.write(self.style.ERROR(f"Errors: {stats['errors']}"))

        self.stdout.write(self.style.SUCCESS("Collection completed!"))
