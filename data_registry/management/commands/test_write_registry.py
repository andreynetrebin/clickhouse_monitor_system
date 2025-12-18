# data_registry/management/commands/test_write_registry.py
from django.core.management.base import BaseCommand
from django.conf import settings
from data_registry.models import DAGMetadata
from data_registry.utils.google_sheets_writer import GoogleSheetsWriter
from data_registry.utils.comment_parser import parse_lineage_comment
from data_registry.utils.cron_humanize import cron_to_human
from clickhouse_driver import Client

class Command(BaseCommand):
    help = "Обновляет F–J для таблицы ОборачиваемостьРасчетная (динамический диапазон)"

    TARGET_TABLE_SHORT = "ОборачиваемостьРасчетная"
    TARGET_TABLE_FULL = "extractor.`ОборачиваемостьРасчетная`"

    def get_clickhouse_columns(self):
        ch_config = settings.CLICKHOUSE_CONFIG["default"]
        client = Client(
            host=ch_config["host"],
            port=ch_config["port"],
            user=ch_config["user"],
            password=ch_config["password"]
        )
        query = """
        SELECT name, comment
        FROM system.columns
        WHERE database = 'extractor' AND table = %(table)s
        ORDER BY name
        """
        rows = client.execute(query, {"table": self.TARGET_TABLE_SHORT})
        return {row[0]: row[1] for row in rows}

    def get_airflow_schedule(self):
        """Возвращает обобщённое расписание: 'каждый день; 2 раза в 11:30, 16:30'"""
        from data_registry.models import DAGMetadata

        cron_expressions = []
        for dag in DAGMetadata.objects.filter(is_paused=False):
            lineage = dag.lineage
            if not isinstance(lineage, dict):
                continue
            for target in lineage.get("targets", []):
                if target.get("target_table") == self.TARGET_TABLE_FULL:
                    if dag.schedule and dag.schedule.strip():
                        cron_expressions.append(dag.schedule)
                    break

        if not cron_expressions:
            return ""

        # Убираем дубли
        cron_expressions = list(dict.fromkeys(cron_expressions))

        # Парсим все cron-выражения
        parsed_times = []
        common_period = None

        for cron in cron_expressions:
            parts = cron.split()
            if len(parts) != 5:
                continue

            minute, hour, dom, month, dow = parts

            # Определяем периодичность (упрощённо)
            if dom == "*" and month == "*" and dow == "*":
                period = "каждый день"
            elif dom != "*" and month == "*" and dow == "*":
                period = "каждый месяц"
            elif dom == "*" and month == "*" and dow != "*":
                if dow == "1-5":
                    period = "по будням"
                elif dow in ("0", "6"):
                    period = "по выходным"
                else:
                    period = "каждую неделю"
            else:
                period = "разное"

            # Проверяем, что все периоды одинаковые
            if common_period is None:
                common_period = period
            elif common_period != period:
                # Если периоды разные — используем простой список
                human_list = [cron_to_human(c) for c in cron_expressions]
                return "; ".join(human_list)

            # Извлекаем время
            if hour == "*" or minute == "*":
                # Пропускаем неявные времена (например, @hourly)
                time_str = cron
            else:
                time_str = f"{hour}:{minute.zfill(2)}"
            parsed_times.append(time_str)

        if not parsed_times:
            return "; ".join(cron_expressions)

        # Сортируем времена
        def time_key(t):
            if ":" in t:
                h, m = t.split(":")
                return int(h) * 60 + int(m)
            return 99999  # неизвестные в конец

        parsed_times.sort(key=time_key)
        times_str = ", ".join(parsed_times)
        count = len(parsed_times)

        if count == 1:
            return f"{common_period} в {times_str}"
        else:
            return f"{common_period}; {count} раза в {times_str}"

    def handle(self, *args, **options):
        writer = GoogleSheetsWriter()

        # 1. Получаем структуру Реестра
        structure = writer.read_full_registry_structure(
            settings.DATA_REGISTRY_SPREADSHEET_ID,
            settings.DATA_REGISTRY_SHEET_NAME
        )

        # 2. Ищем блок по короткому имени
        target_interval = None
        for table_name_in_registry, interval in structure.items():
            if table_name_in_registry == self.TARGET_TABLE_SHORT:
                target_interval = interval
                break

        if not target_interval:
            self.stdout.write(
                self.style.ERROR(f"❌ Таблица '{self.TARGET_TABLE_SHORT}' не найдена в Реестре")
            )
            return

        start_row = target_interval["start"]
        end_row = target_interval["end"]

        # 3. Данные из ClickHouse и Airflow
        ch_columns = self.get_clickhouse_columns()
        if not ch_columns:
            self.stdout.write(self.style.ERROR("❌ Таблица не найдена в ClickHouse"))
            return

        schedule_human = self.get_airflow_schedule()

        # 4. Читаем текущие строки
        current_rows = writer.read_rows(
            settings.DATA_REGISTRY_SPREADSHEET_ID,
            settings.DATA_REGISTRY_SHEET_NAME,
            start_row,
            end_row
        )

        # 5. Обновляем F–J с сопоставлением по колонке C (Поле в ClickHouse)
        updated_rows = []
        for row in current_rows:
            # Колонка C = индекс 2
            field_in_registry = row[2].strip() if len(row) > 2 else ""

            if not field_in_registry:
                # Если поле пустое — оставляем строку как есть
                updated_rows.append(row)
                continue

            # Ищем комментарий для этого поля
            comment = ch_columns.get(field_in_registry, "")
            parsed = parse_lineage_comment(comment)
            source_table = parsed.get("Ссылка на источник", "")
            source_column = parsed.get("Название реквизита источника", "")

            # Определяем тип источника
            data_source_type = ""
            if source_table:
                if "google" in source_table.lower() or "spreadsheets" in source_table.lower():
                    data_source_type = "Google Sheets"
                elif source_table not in ("calendar", "system"):
                    data_source_type = "ClickHouse"

            # Обновляем F–J, остальное оставляем
            new_row = row[:5] + [
                data_source_type,  # F: Источник данных
                source_table,  # G: Ссылка на источник
                source_column,  # H: Название реквизита источника
                "Airflow",  # I: Правило обмена
                schedule_human  # J: Расписание
            ]
            updated_rows.append(new_row)

        # 6. Запись
        writer.write_registry_rows(
            settings.DATA_REGISTRY_SPREADSHEET_ID,
            settings.DATA_REGISTRY_SHEET_NAME,
            start_row,
            updated_rows
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Обновлено {len(updated_rows)} строк для '{self.TARGET_TABLE_SHORT}'"
            )
        )