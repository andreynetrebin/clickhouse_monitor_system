# data_registry/management/commands/sync_dag_metadata.py
import csv
import json
import os
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.timezone import make_aware, utc
from data_registry.models import DAGMetadata
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Синхронизирует DAG metadata из CSV-файла (заглушка вместо ClickHouse)"

    def handle(self, *args, **options):
        csv_path = os.path.join(settings.BASE_DIR, 'airflow_data.txt')
        if not os.path.exists(csv_path):
            self.stdout.write(
                self.style.ERROR(f"❌ Файл не найден: {csv_path}")
            )
            return

        created_count = 0
        total_count = 0

        with open(csv_path, 'r', encoding='utf-8') as f:
            # Пропускаем заголовок
            reader = csv.DictReader(f, delimiter=';', quoting=csv.QUOTE_ALL)

            for row in reader:
                total_count += 1
                dag_id = row['dag_id']
                # Парсим даты вручную (ClickHouse формат: YYYY-MM-DD HH:MM:SS.uuuuuu)
                from datetime import datetime
                def parse_datetime(s):
                    if not s.strip():
                        return None
                    if '.' in s:
                        dt_part, us_part = s.split('.')
                        us_part = us_part.ljust(6, '0')[:6]
                        s = f"{dt_part}.{us_part}"
                    dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
                    return make_aware(dt, timezone=utc)  # ← вот так

                # Парсим lineage (двойные кавычки → валидный JSON)
                lineage_str = row['lineage'].strip()
                lineage = {}
                if lineage_str and lineage_str != '{}':
                    try:
                        # Заменяем удвоенные кавычки на одинарные (стандарт CSV экранирования)
                        lineage_json = lineage_str.replace('""', '"')
                        lineage = json.loads(lineage_json)
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка парсинга lineage для {dag_id}: {e}")
                        lineage = {}

                # Преобразуем tags из строки вида "{'tag1','tag2'}" → list
                tags_str = row['tags'].strip()
                tags = []
                if tags_str.startswith('{') and tags_str.endswith('}'):
                    tags_clean = tags_str[1:-1].replace("'", "").replace('"', "")
                    if tags_clean:
                        tags = [t.strip() for t in tags_clean.split(',')]

                # Преобразуем is_paused
                is_paused = row['is_paused'].lower() == 'true'

                obj, created = DAGMetadata.objects.update_or_create(
                    dag_id=dag_id,
                    created_at=parse_datetime(row['created_at']),
                    defaults={
                        'schedule': row['schedule'],
                        'description': row['description'],
                        'is_paused': is_paused,
                        'fileloc': row['fileloc'],
                        'last_parsed': parse_datetime(row['last_parsed']),
                        'lineage': lineage,
                    }
                )
                if created:
                    created_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Загружено из CSV: {total_count} записей, новых: {created_count}"
            )
        )