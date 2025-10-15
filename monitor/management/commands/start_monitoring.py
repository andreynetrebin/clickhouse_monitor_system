import time
import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Запуск непрерывного мониторинга ClickHouse'

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval',
            type=int,
            default=300,
            help='Интервал сбора в секундах (по умолчанию 5 минут)'
        )
        parser.add_argument(
            '--threshold-ms',
            type=int,
            default=1000,
            help='Порог для медленных запросов'
        )

    def handle(self, *args, **options):
        interval = options['interval']
        threshold_ms = options['threshold_ms']

        self.stdout.write(
            self.style.SUCCESS(
                f'Starting continuous monitoring (interval: {interval}s, threshold: {threshold_ms}ms)'
            )
        )
        self.stdout.write("Press Ctrl+C to stop...")

        try:
            while True:
                start_time = timezone.now()

                self.stdout.write(f"\n[{start_time.strftime('%H:%M:%S')}] Collecting metrics...")

                # Запускаем сбор метрик
                from django.core.management import call_command
                call_command(
                    'collect_metrics',
                    lookback_minutes=interval // 60,
                    threshold_ms=threshold_ms,
                    limit=500
                )

                # Ждем до следующего запуска
                elapsed = (timezone.now() - start_time).total_seconds()
                sleep_time = max(interval - elapsed, 0)

                if sleep_time > 0:
                    self.stdout.write(f"Waiting {sleep_time:.1f}s until next collection...")
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.stdout.write(self.style.SUCCESS("\nMonitoring stopped by user"))