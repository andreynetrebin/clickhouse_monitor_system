import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from monitor.models import QueryLog

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Очистка устаревших данных мониторинга'

    def add_arguments(self, parser):
        parser.add_argument(
            '--retention-days',
            type=int,
            default=30,
            help='Срок хранения данных в днях'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет удалено без фактического удаления'
        )

    def handle(self, *args, **options):
        retention_days = options['retention_days']
        dry_run = options['dry_run']

        cutoff_date = timezone.now() - timedelta(days=retention_days)

        self.stdout.write(f"Purging data older than {retention_days} days (before {cutoff_date})")

        # Находим записи для удаления
        old_queries = QueryLog.objects.filter(query_start_time__lt=cutoff_date)
        count = old_queries.count()

        if dry_run:
            self.stdout.write(
                self.style.WARNING(f"DRY RUN: Would delete {count} old query logs")
            )
            # Показываем примеры
            for query in old_queries[:5]:
                self.stdout.write(f"  - {query.query_start_time}: {query.query_text[:100]}...")
        else:
            deleted_count, _ = old_queries.delete()
            self.stdout.write(
                self.style.SUCCESS(f"Deleted {deleted_count} old query logs")
            )