import re
from typing import Dict, List, Optional
from clickhouse_client import ClickHouseClient


class AdvancedQueryAnalyzer:
    """
    Продвинутый анализатор запросов с использованием:
    - EXPLAIN PLAN
    - EXPLAIN PIPELINE
    - system.query_log
    - system.tables
    """

    def __init__(self, client: ClickHouseClient):
        self.client = client

    def get_explain_plan(self, query: str) -> Dict:
        """
        Получает план выполнения запроса через EXPLAIN
        """
        try:
            # EXPLAIN PLAN
            plan_result = self.client.execute_query(f"EXPLAIN PLAN {query}")
            plan_steps = [step[0] for step in plan_result.data] if plan_result.data else []

            # EXPLAIN PIPELINE
            pipeline_result = self.client.execute_query(f"EXPLAIN PIPELINE {query}")
            pipeline_steps = [step[0] for step in pipeline_result.data] if pipeline_result.data else []

            return {
                'plan_steps': plan_steps,
                'pipeline_steps': pipeline_steps,
                'has_full_scan': any('Full' in step for step in plan_steps),
                'has_sorting': any('Sort' in step for step in plan_steps),
                'has_aggregation': any('Aggregating' in step for step in plan_steps),
                'pipeline_complexity': len(pipeline_steps)
            }
        except Exception as e:
            return {'error': str(e)}

    def get_table_stats(self, table_name: str, database: str = 'default') -> Dict:
        """
        Получает статистику по таблице из system.tables
        """
        try:
            query = f"""
            SELECT 
                name,
                total_rows,
                total_bytes,
                engine,
                partition_key,
                sorting_key
            FROM system.tables 
            WHERE database = '{database}' AND name = '{table_name}'
            """
            result = self.client.execute_query(query)

            if result.data:
                name, total_rows, total_bytes, engine, partition_key, sorting_key = result.data[0]
                return {
                    'name': name,
                    'total_rows': total_rows,
                    'total_bytes': total_bytes,
                    'engine': engine,
                    'partition_key': partition_key,
                    'sorting_key': sorting_key,
                    'size_gb': total_bytes / (1024 ** 3) if total_bytes else 0
                }
            return {}
        except Exception as e:
            return {'error': str(e)}

    def extract_tables_from_query(self, query: str) -> List[str]:
        """
        Извлекает имена таблиц из SQL запроса
        """
        # Простой парсинг для извлечения таблиц
        tables = []

        # FROM clause
        from_matches = re.findall(r'FROM\s+(\w+)', query, re.IGNORECASE)
        tables.extend(from_matches)

        # JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)', query, re.IGNORECASE)
        tables.extend(join_matches)

        # INSERT/UPDATE/DELETE
        dml_matches = re.findall(r'(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)?\s*(\w+)', query, re.IGNORECASE)
        tables.extend(dml_matches)

        return list(set(tables))  # Уникальные таблицы

    def analyze_with_explain(self, query: str) -> Dict:
        """
        Комплексный анализ запроса с EXPLAIN и статистикой таблиц
        """
        analysis = {
            'query': query,
            'explain_analysis': {},
            'table_analysis': {},
            'recommendations': [],
            'warnings': []
        }

        # 1. Анализ через EXPLAIN
        explain_data = self.get_explain_plan(query)
        analysis['explain_analysis'] = explain_data

        if 'error' not in explain_data:
            # Анализ плана выполнения
            if explain_data['has_full_scan']:
                analysis['warnings'].append({
                    'type': 'full_scan',
                    'message': 'Обнаружено полносканирование таблицы',
                    'priority': 'high'
                })
                analysis['recommendations'].append(
                    'Добавьте индексы на колонки в условиях WHERE'
                )

            if explain_data['has_sorting']:
                analysis['warnings'].append({
                    'type': 'sorting',
                    'message': 'Обнаружена операция сортировки',
                    'priority': 'medium'
                })
                analysis['recommendations'].append(
                    'Используйте индексы для избежания сортировки'
                )

            if explain_data['pipeline_complexity'] > 10:
                analysis['warnings'].append({
                    'type': 'complex_pipeline',
                    'message': f'Сложный pipeline ({explain_data["pipeline_complexity"]} шагов)',
                    'priority': 'medium'
                })

        # 2. Анализ таблиц
        tables = self.extract_tables_from_query(query)
        analysis['tables_found'] = tables

        for table in tables:
            table_stats = self.get_table_stats(table)
            analysis['table_analysis'][table] = table_stats

            if table_stats and 'error' not in table_stats:
                # Анализ размера таблицы
                if table_stats.get('size_gb', 0) > 1:  # Больше 1GB
                    analysis['warnings'].append({
                        'type': 'large_table',
                        'message': f'Большая таблица {table}: {table_stats["size_gb"]:.1f} GB',
                        'priority': 'info'
                    })

                # Анализ движка таблицы
                if table_stats.get('engine') == 'MergeTree':
                    if not table_stats.get('partition_key'):
                        analysis['recommendations'].append(
                            f'Для таблицы {table} рассмотрите добавление партиционирования'
                        )
                    if not table_stats.get('sorting_key'):
                        analysis['recommendations'].append(
                            f'Для таблицы {table} рассмотрите добавление ключа сортировки'
                        )

        # 3. Анализ структуры запроса
        query_upper = query.upper()

        # Поиск SELECT *
        if 'SELECT *' in query_upper:
            analysis['warnings'].append({
                'type': 'select_all',
                'message': 'Используется SELECT *',
                'priority': 'medium'
            })
            analysis['recommendations'].append(
                'Укажите конкретные колонки вместо SELECT *'
            )

        # Поиск LIMIT без ORDER BY
        if 'LIMIT' in query_upper and 'ORDER BY' not in query_upper:
            analysis['warnings'].append({
                'type': 'limit_no_order',
                'message': 'LIMIT без ORDER BY может давать непредсказуемые результаты',
                'priority': 'low'
            })

        return analysis

    def get_query_history_stats(self, normalized_query_hash: str, days: int = 7) -> Dict:
        """
        Получает историческую статистику выполнения запроса
        """
        try:
            query = f"""
            SELECT 
                count() as execution_count,
                avg(query_duration_ms) as avg_duration_ms,
                max(query_duration_ms) as max_duration_ms,
                min(query_duration_ms) as min_duration_ms,
                quantile(0.95)(query_duration_ms) as p95_duration_ms
            FROM system.query_log 
            WHERE normalized_query_hash = '{normalized_query_hash}'
            AND event_time > now() - INTERVAL {days} DAY
            AND type = 'QueryFinish'
            """

            result = self.client.execute_query(query)

            if result.data:
                count, avg_ms, max_ms, min_ms, p95_ms = result.data[0]
                return {
                    'execution_count': count,
                    'avg_duration_ms': avg_ms,
                    'max_duration_ms': max_ms,
                    'min_duration_ms': min_ms,
                    'p95_duration_ms': p95_ms,
                    'stability': 'stable' if (max_ms / avg_ms) < 2 else 'unstable'
                }
            return {}
        except Exception as e:
            return {'error': str(e)}

    def generate_comprehensive_report(self, query: str, query_hash: str = None) -> Dict:
        """
        Генерирует комплексный отчет по запросу
        """
        report = {
            'basic_analysis': self.analyze_with_explain(query),
            'performance_metrics': {}
        }

        # Добавляем историческую статистику если есть hash
        if query_hash:
            history_stats = self.get_query_history_stats(query_hash)
            report['performance_metrics']['history'] = history_stats

        # Оценка сложности запроса
        complexity_score = self._calculate_complexity_score(query)
        report['complexity_score'] = complexity_score

        return report

    def _calculate_complexity_score(self, query: str) -> int:
        """
        Рассчитывает оценку сложности запроса
        """
        score = 0

        # Количество JOIN
        joins = len(re.findall(r'JOIN', query, re.IGNORECASE))
        score += joins * 5

        # Количество подзапросов
        subqueries = len(re.findall(r'\(\s*SELECT', query, re.IGNORECASE))
        score += subqueries * 10

        # Количество условий WHERE
        where_conditions = len(re.findall(r'WHERE', query, re.IGNORECASE))
        score += where_conditions * 3

        # Наличие агрегаций
        if re.search(r'GROUP BY|COUNT|SUM|AVG|MAX|MIN', query, re.IGNORECASE):
            score += 8

        # Наличие сортировки
        if re.search(r'ORDER BY', query, re.IGNORECASE):
            score += 5

        return min(score, 100)  # Ограничиваем максимальную сложность


# Синглтон для удобного доступа
def get_advanced_analyzer():
    return AdvancedQueryAnalyzer(ClickHouseClient())