from django.core.management.base import BaseCommand
from monitor.models import QueryLog
from query_lab.optimization_guide import optimization_guide


class Command(BaseCommand):
    help = 'Массовый анализ запросов и генерация рекомендаций'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Количество запросов для анализа'
        )
        parser.add_argument(
            '--threshold-ms',
            type=int,
            default=1000,
            help='Порог для медленных запросов'
        )

    def handle(self, *args, **options):
        limit = options['limit']
        threshold_ms = options['threshold_ms']

        self.stdout.write(f"Анализ {limit} самых медленных запросов (>{threshold_ms}ms)...")

        slow_queries = QueryLog.objects.filter(
            is_slow=True,
            duration_ms__gt=threshold_ms
        ).order_by('-duration_ms')[:limit]

        total_patterns = 0
        critical_count = 0

        for query in slow_queries:
            analysis = optimization_guide.analyze_query(query.query_text)

            self.stdout.write(f"\n--- Запрос #{query.id} ({query.duration_ms}ms) ---")
            self.stdout.write(f"Проблем: {analysis['summary']['total_patterns']}")

            for pattern in analysis['detected_patterns']:
                self.stdout.write(f"  {pattern['name']} ({pattern['priority']})")
                total_patterns += 1
                if pattern['priority'] == 'critical':
                    critical_count += 1

        self.stdout.write(f"\n=== Сводка ===")
        self.stdout.write(f"Проанализировано запросов: {len(slow_queries)}")
        self.stdout.write(f"Обнаружено проблем: {total_patterns}")
        self.stdout.write(f"Критичных проблем: {critical_count}")
        self.stdout.write(f"Среднее проблем на запрос: {total_patterns / len(slow_queries) if slow_queries else 0:.1f}")