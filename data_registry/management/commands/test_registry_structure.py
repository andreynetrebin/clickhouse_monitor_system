# data_registry/management/commands/test_registry_structure.py
from django.core.management.base import BaseCommand
from django.conf import settings
from data_registry.utils.google_sheets_writer import GoogleSheetsWriter

class Command(BaseCommand):
    help = "Тест: читает Google Таблицу и выводит найденные диапазоны таблиц"

    def handle(self, *args, **options):
        writer = GoogleSheetsWriter()
        try:
            structure = writer.read_full_registry_structure(
                settings.DATA_REGISTRY_SPREADSHEET_ID,
                settings.DATA_REGISTRY_SHEET_NAME
            )

            if not structure:
                self.stdout.write(self.style.WARNING("⚠️ Блоки таблиц не найдены"))
                return

            self.stdout.write(self.style.SUCCESS("✅ Найдены следующие блоки:"))
            for table_name, interval in structure.items():
                self.stdout.write(
                    f"  '{table_name}' → строки {interval['start']}–{interval['end']} "
                    f"({interval['end'] - interval['start'] + 1} строк)"
                )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Ошибка: {e}"))
            raise