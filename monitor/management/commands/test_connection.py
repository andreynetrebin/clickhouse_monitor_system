import logging
from django.core.management.base import BaseCommand
from clickhouse_client import test_connection, get_clickhouse_version

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Тестирование подключения к ClickHouse'

    def add_arguments(self, parser):
        parser.add_argument(
            '--instance',
            type=str,
            default='default',
            help='Имя инстанса ClickHouse'
        )

    def handle(self, *args, **options):
        instance_name = options['instance']

        self.stdout.write(f"Testing connection to ClickHouse instance: {instance_name}")

        if test_connection(instance_name):
            version = get_clickhouse_version(instance_name)
            self.stdout.write(
                self.style.SUCCESS(f"✅ Connection successful! ClickHouse version: {version}")
            )
        else:
            self.stdout.write(
                self.style.ERROR("❌ Connection failed!")
            )