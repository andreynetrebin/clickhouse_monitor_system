# data_registry/management/commands/update_data_registry.py
from django.core.management.base import BaseCommand
from django.conf import settings
from data_registry.services.registry_builder import build_registry_rows
from data_registry.utils.google_sheets_writer import GoogleSheetsWriter

class Command(BaseCommand):
    help = "Обновляет Реестр данных в Google Таблице"

    def handle(self, *args, **options):
        # Генерация строк
        rows = build_registry_rows()
        self.stdout.write(f"Сгенерировано {len(rows)} строк")

        if not rows:
            self.stdout.write("Нет данных для записи")
            return

        # Заголовки в порядке Реестра
        headers = [
            "Таблица в Clickhouse",
            "Поле в ClickHouse",
            "Коментарий",
            "Тип в ClickHouse",
            "Источник данных",
            "Ссылка на источник",
            "Название реквизита источника",
            "Правило обмена",
            "Расписание"
        ]

        # Преобразуем словари в списки
        data_rows = [
            [
                r["Таблица в Clickhouse"],
                r["Поле в ClickHouse"],
                r["Коментарий"],
                r["Тип в ClickHouse"],
                r["Источник данных"],
                r["Ссылка на источник"],
                r["Название реквизита источника"],
                r["Правило обмена"],
                r["Расписание"],
            ]
            for r in rows
        ]

        # Запись в Google Таблицу
        writer = GoogleSheetsWriter()
        writer.write_registry(
            spreadsheet_id=settings.DATA_REGISTRY_SPREADSHEET_ID,
            sheet_name=settings.DATA_REGISTRY_SHEET_NAME,
            headers=headers,
            rows=data_rows
        )

        self.stdout.write(
            self.style.SUCCESS("✅ Реестр данных успешно обновлён!")
        )