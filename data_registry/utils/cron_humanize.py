# data_registry/utils/cron_humanize.py
import re

def cron_to_human(cron_expr: str) -> str:
    """
    Преобразует cron-выражение в человекочитаемый вид.
    Поддерживает:
      - Минуты: 0, *, */N
      - Часы: 0-23, *, N,M,K, */N
      - Дни: * (игнорируем, если * везде)
    Примеры:
      "00 8,10,13 * * *" → "каждый день; 3 раза в 8:00, 10:00, 13:00"
      "30 5 * * *"       → "каждый день в 5:30"
      "0 2 * * 1-5"      → "по будням в 2:00"
    """
    if not cron_expr or cron_expr.strip() == "* * * * *":
        return "каждую минуту"

    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return cron_expr  # неизвестный формат

    try:
        minutes, hours, dom, month, dow = parts

        # Минуты
        if minutes == "0" or minutes == "00":
            minute_str = ""
        elif re.match(r"^\d+$", minutes):
            minute_str = f":{minutes.zfill(2)}"
        else:
            minute_str = f" в минуты {minutes}"  # редкий случай

        # Дни недели
        days_map = {
            "0": "воскресенье", "1": "понедельник", "2": "вторник",
            "3": "среда", "4": "четверг", "5": "пятница", "6": "суббота"
        }

        # Обработка дней недели
        if dow == "*" and dom == "*":
            day_part = "каждый день"
        elif dom != "*" and dow == "*":
            day_part = "каждый день"
        elif dom == "*" and dow != "*":
            if dow == "1-5":
                day_part = "по будням"
            elif dow == "0" or dow == "6":
                day_part = "по выходным"
            elif "," in dow:
                days = [days_map.get(d, d) for d in dow.split(",")]
                day_part = f"в {', '.join(days)}"
            elif dow in days_map:
                day_part = f"каждое {days_map[dow]}"
            else:
                day_part = f"дни недели {dow}"
        else:
            day_part = "каждый день"

        # Обработка часов
        if hours == "*":
            time_part = "каждый час" + minute_str
            return f"{day_part} {time_part}"

        # Конкретные часы
        if "," in hours:
            hour_list = [int(h) for h in hours.split(",")]
            hour_list.sort()
            time_points = [f"{h}{minute_str or ':00'}" for h in hour_list]
            if len(time_points) == 1:
                time_desc = time_points[0]
            else:
                time_desc = f"{len(time_points)} раза в {', '.join(time_points)}"
            return f"{day_part}; {time_desc}"

        # Диапазон часов (например, 9-17)
        if "-" in hours:
            start, end = map(int, hours.split("-"))
            if minute_str:
                time_desc = f"каждый час с {start}{minute_str} до {end}{minute_str}"
            else:
                time_desc = f"каждый час с {start}:00 до {end}:00"
            return f"{day_part} {time_desc}"

        # Один час
        hour = int(hours)
        time_desc = f"{hour}{minute_str or ':00'}"
        return f"{day_part} в {time_desc}"

    except Exception:
        return cron_expr