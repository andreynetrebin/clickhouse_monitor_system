from typing import Dict, List, Optional
import re


class QueryOptimizationGuide:
    """
    Система рекомендаций по оптимизации запросов ClickHouse
    на основе лучших практик и распространенных паттернов проблем
    """

    # Паттерны проблем и рекомендации
    OPTIMIZATION_PATTERNS = {
        'full_scan': {
            'name': '📊 Полносканирование таблицы',
            'patterns': [
                r'WHERE.*!=',
                r'WHERE.*NOT IN',
                r'WHERE.*LIKE.*%',
                r'WHERE.*IS NULL',
                r'WHERE.*OR.*=',
            ],
            'recommendations': [
                'Добавьте индексы на колонки в условиях WHERE',
                'Используйте партиционирование для больших таблиц',
                'Рассмотрите материализованные представления для часто запрашиваемых данных',
                'Используйте условия с = вместо != или NOT IN',
                'Для поиска по тексту используйте полнотекстовый поиск'
            ],
            'priority': 'high'
        },

        'missing_index': {
            'name': '📈 Отсутствие индекса',
            'patterns': [
                r'WHERE.*\w+.*=',
                r'WHERE.*\w+.*>',
                r'WHERE.*\w+.*<',
                r'JOIN.*ON.*=',
            ],
            'recommendations': [
                'Создайте индексы на колонки, используемые в условиях WHERE',
                'Для JOIN создайте индексы на колонки связей',
                'Используйте составные индексы для multiple условий',
                'Проверьте порядок колонок в индексах (high cardinality first)'
            ],
            'priority': 'high'
        },

        'cross_join': {
            'name': '❌ CROSS JOIN',
            'patterns': [
                r'CROSS JOIN',
                r'JOIN.*ON.*1=1',
                r',.*,',  # implicit cross join
            ],
            'recommendations': [
                'Замените CROSS JOIN на INNER JOIN с явными условиями',
                'Добавьте условия JOIN для ограничения декартова произведения',
                'Рассмотрите использование подзапросов вместо CROSS JOIN',
                'Проверьте что все JOIN имеют явные условия'
            ],
            'priority': 'critical'
        },

        'subquery': {
            'name': '🔄 Неоптимизированный подзапрос',
            'patterns': [
                r'WHERE.*IN\s*\(SELECT',
                r'WHERE.*EXISTS\s*\(SELECT',
                r'SELECT.*\(\s*SELECT',
            ],
            'recommendations': [
                'Замените подзапросы на JOIN где это возможно',
                'Используйте CTE (WITH) для сложных подзапросов',
                'Для IN подзапросов рассмотрите временные таблицы',
                'Проверьте что подзапросы возвращают разумное количество строк'
            ],
            'priority': 'medium'
        },

        'large_result': {
            'name': '💾 Большой результат',
            'patterns': [
                r'SELECT\s*\*',
                r'LIMIT\s+100000',
                r'LIMIT\s+10000',
            ],
            'recommendations': [
                'Выбирайте только необходимые колонки вместо SELECT *',
                'Добавьте агрегации для уменьшения объема данных',
                'Используйте пагинацию с разумными LIMIT',
                'Рассмотрите материализованные представления для часто запрашиваемых агрегаций'
            ],
            'priority': 'medium'
        },

        'memory_usage': {
            'name': '🧠 Высокое использование памяти',
            'patterns': [
                r'DISTINCT',
                r'GROUP BY.*\w+,\s*\w+,\s*\w+',  # много колонок в GROUP BY
                r'ORDER BY.*\w+,\s*\w+,\s*\w+',  # много колонок в ORDER BY
            ],
            'recommendations': [
                'Для DISTINCT рассмотрите приближенные агрегации (uniqCombined)',
                'Упростите GROUP BY - используйте только необходимые колонки',
                'Для сортировки больших результатов используйте индексы',
                'Увеличьте настройки памяти если необходимо (max_memory_usage)'
            ],
            'priority': 'medium'
        },

        'datetime_optimization': {
            'name': '⏰ Оптимизация работы с датами',
            'patterns': [
                r'WHERE.*date.*>.*now\(\)',
                r'WHERE.*toDate\(',
                r'WHERE.*toString\(date\)',
            ],
            'recommendations': [
                'Используйте партиционирование по дате для временных рядов',
                'Для фильтрации по датам используйте Date/DateTime типы',
                'Избегайте преобразований типов в условиях WHERE',
                'Используйте предварительно рассчитанные агрегаты по периодам'
            ],
            'priority': 'medium'
        }
    }

    @classmethod
    def analyze_query(cls, query: str) -> Dict:
        """
        Анализирует запрос и возвращает рекомендации по оптимизации
        """
        recommendations = []
        detected_patterns = []

        query_upper = query.upper()

        for pattern_key, pattern_info in cls.OPTIMIZATION_PATTERNS.items():
            for regex_pattern in pattern_info['patterns']:
                if re.search(regex_pattern, query_upper, re.IGNORECASE):
                    detected_patterns.append({
                        'pattern': pattern_key,
                        'name': pattern_info['name'],
                        'priority': pattern_info['priority'],
                        'recommendations': pattern_info['recommendations']
                    })
                    break  # Не дублируем рекомендации для одного паттерна

        # Уникальные рекомендации
        unique_recommendations = []
        seen_recommendations = set()

        for pattern in detected_patterns:
            for rec in pattern['recommendations']:
                if rec not in seen_recommendations:
                    unique_recommendations.append(rec)
                    seen_recommendations.add(rec)

        # Группировка по приоритету
        critical_recs = [p for p in detected_patterns if p['priority'] == 'critical']
        high_recs = [p for p in detected_patterns if p['priority'] == 'high']
        medium_recs = [p for p in detected_patterns if p['priority'] == 'medium']

        return {
            'detected_patterns': detected_patterns,
            'recommendations': unique_recommendations,
            'summary': {
                'critical_count': len(critical_recs),
                'high_count': len(high_recs),
                'medium_count': len(medium_recs),
                'total_patterns': len(detected_patterns)
            }
        }

    @classmethod
    def generate_optimized_template(cls, original_query: str, patterns: List) -> str:
        """
        Генерирует шаблон оптимизированного запроса на основе обнаруженных проблем
        """
        if not patterns:
            return original_query

        optimized = original_query

        # Примеры преобразований (упрощенные)
        for pattern in patterns:
            if pattern['pattern'] == 'full_scan':
                # Замена != на = где возможно
                optimized = re.sub(r'WHERE\s+(\w+)\s*!=\s*(\S+)',
                                   r'WHERE \1 = \2', optimized, flags=re.IGNORECASE)

            elif pattern['pattern'] == 'large_result':
                # Замена SELECT * на конкретные колонки
                if 'SELECT *' in optimized.upper():
                    optimized = re.sub(r'SELECT\s*\*',
                                       r'SELECT /* specify columns here */',
                                       optimized, flags=re.IGNORECASE)

            elif pattern['pattern'] == 'subquery':
                # Пример замены IN подзапроса на JOIN
                if ' IN (SELECT' in optimized.upper():
                    optimized = re.sub(
                        r'WHERE\s+(\w+)\s+IN\s*\(\s*SELECT\s+(\w+)\s+FROM\s+(\w+)',
                        r'JOIN \3 ON \1 = \2 WHERE',
                        optimized, flags=re.IGNORECASE
                    )

        return optimized

    @classmethod
    def get_best_practices_checklist(cls) -> List[Dict]:
        """
        Возвращает чеклист лучших практик ClickHouse
        """
        return [
            {
                'category': '📊 Структура запроса',
                'items': [
                    'Используйте только необходимые колонки в SELECT',
                    'Избегайте SELECT * в production запросах',
                    'Используйте LIMIT для ограничения больших результатов',
                    'Группируйте сложные условия в подзапросы или CTE'
                ]
            },
            {
                'category': '⚡ Производительность',
                'items': [
                    'Создавайте индексы на колонки в условиях WHERE и JOIN',
                    'Используйте партиционирование для больших таблиц',
                    'Избегайте преобразований типов в условиях WHERE',
                    'Используйте приближенные агрегации для больших данных'
                ]
            },
            {
                'category': '🔍 Условия фильтрации',
                'items': [
                    'Используйте = вместо != или NOT IN где возможно',
                    'Для текстового поиска используйте полнотекстовые индексы',
                    'Избегайте OR условий - используйте UNION ALL',
                    'Помещайте самые селективные условия первыми'
                ]
            },
            {
                'category': '🔄 JOIN операции',
                'items': [
                    'Всегда указывайте явные условия JOIN',
                    'Избегайте CROSS JOIN в production',
                    'Используйте INNER JOIN вместо подзапросов где возможно',
                    'Проверяйте порядок таблиц в JOIN (меньшая таблица first)'
                ]
            }
        ]


# Синглтон для удобного доступа
optimization_guide = QueryOptimizationGuide()