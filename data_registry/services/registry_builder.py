# data_registry/services/registry_builder.py
from typing import List, Dict
from data_registry.models import DAGMetadata


def build_registry_rows() -> List[Dict[str, str]]:
    """
    Генерирует строки для Реестра данных на основе DAGMetadata с lineage.

    Поддерживаемые форматы lineage:
    1. Google Sheets:
        {
          "type": "google_sheets",
          "spreadsheet_id": "...",
          "sheet_name": "...",
          "column_mapping": {"Исходное поле": "Целевое поле", ...}
        }
    2. ClickHouse → ClickHouse:
        {
          "table": "source_table",
          "fields": {"Целевое поле": ["Исходное поле 1", "Исходное поле 2", ...], ...}
        }
    """
    rows = []

    for dag in DAGMetadata.objects.all():
        lineage = dag.lineage
        if not isinstance(lineage, dict):
            continue

        targets = lineage.get("targets", [])
        if not targets:
            continue

        for target in targets:
            target_table = target.get("target_table", "").strip()
            if not target_table:
                continue

            sources = target.get("sources", [])
            for source in sources:
                # === Случай 1: Google Sheets ===
                if source.get("type") == "google_sheets":
                    spreadsheet_id = source.get("spreadsheet_id", "")
                    column_mapping = source.get("column_mapping", {})

                    if not spreadsheet_id or not column_mapping:
                        continue

                    source_name = "Google Sheets"
                    source_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=0"

                    for src_col, ch_col in column_mapping.items():
                        rows.append({
                            "Таблица в Clickhouse": target_table,
                            "Поле в ClickHouse": ch_col,
                            "Коментарий": "",
                            "Тип в ClickHouse": "",
                            "Источник данных": source_name,
                            "Ссылка на источник": source_link,
                            "Название реквизита источника": src_col,
                            "Правило обмена": "Airflow",
                            "Расписание": dag.schedule or "",
                        })

                # === Случай 2: ClickHouse как источник (без "type", но с "table" и "fields") ===
                elif "table" in source and "fields" in source:
                    source_table = source.get("table", "").strip()
                    fields_mapping = source.get("fields", {})

                    if not source_table or not fields_mapping:
                        continue

                    source_name = "ClickHouse"
                    source_link = source_table

                    for ch_col, src_fields in fields_mapping.items():
                        # `src_fields` — это список исходных полей (часто из массива или выражения)
                        if isinstance(src_fields, list):
                            for src_col in src_fields:
                                rows.append({
                                    "Таблица в Clickhouse": target_table,
                                    "Поле в ClickHouse": ch_col,
                                    "Коментарий": "",
                                    "Тип в ClickHouse": "",
                                    "Источник данных": source_name,
                                    "Ссылка на источник": source_link,
                                    "Название реквизита источника": src_col,
                                    "Правило обмена": "Airflow",
                                    "Расписание": dag.schedule or "",
                                })
                        else:
                            # На случай, если вдруг не список (защита от ошибок)
                            rows.append({
                                "Таблица в Clickhouse": target_table,
                                "Поле в ClickHouse": ch_col,
                                "Коментарий": "",
                                "Тип в ClickHouse": "",
                                "Источник данных": source_name,
                                "Ссылка на источник": source_link,
                                "Название реквизита источника": str(src_fields),
                                "Правило обмена": "Airflow",
                                "Расписание": dag.schedule or "",
                            })

    return rows