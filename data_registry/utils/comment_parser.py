# data_registry/utils/comment_parser.py
import json

def parse_lineage_comment(comment: str) -> dict:
    """
    Парсит валидный JSON из комментария колонки ClickHouse.
    Пример комментария:
        '{"source_table": "extractor.Srez_Ostatkov", "source_column": "НоменклатураГуид"}'
    """
    if not comment or not isinstance(comment, str):
        return {}
    try:
        data = json.loads(comment)
        return {
            "Ссылка на источник": data.get("source_table", ""),
            "Название реквизита источника": data.get("source_column", ""),
        }
    except (json.JSONDecodeError, ValueError):
        return {}