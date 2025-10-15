from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from django.db import models  # ← ДОБАВИТЬ ИМПОРТ
from monitor.models import QueryLog, ClickHouseInstance
from query_lab.models import SlowQuery


class Command(BaseCommand):
    help = 'Показать статистику собранных данных'

    def handle(self, *args, **options):
        self.stdout.write("=== MONITORING SYSTEM STATISTICS ===")

        # Общая статистика
        self.stdout.write(f"\n1. Database Statistics:")
        self.stdout.write(f"   ClickHouse instances: {ClickHouseInstance.objects.count()}")
        self.stdout.write(f"   QueryLog records: {QueryLog.objects.count()}")
        self.stdout.write(f"   Slow queries: {QueryLog.objects.filter(is_slow=True).count()}")
        self.stdout.write(f"   Lab queries: {SlowQuery.objects.count()}")

        # Статистика по статусам лаборатории
        self.stdout.write(f"\n2. Lab Status Distribution:")
        status_counts = SlowQuery.objects.values('status').annotate(count=models.Count('id'))
        for status in status_counts:
            self.stdout.write(f"   {status['status']}: {status['count']}")

        # Самые медленные запросы
        self.stdout.write(f"\n3. Top 5 Slowest Queries:")
        slow_queries = QueryLog.objects.filter(is_slow=True).order_by('-duration_ms')[:5]
        for i, query in enumerate(slow_queries, 1):
            self.stdout.write(f"   {i}. {query.duration_ms / 1000:.1f}s - {query.query_text[:80]}...")

        # Активность по времени
        self.stdout.write(f"\n4. Recent Activity:")
        last_hour = timezone.now() - timedelta(hours=1)
        recent_queries = QueryLog.objects.filter(collected_at__gte=last_hour).count()
        self.stdout.write(f"   Queries collected in last hour: {recent_queries}")