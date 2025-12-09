import re
from typing import Dict, List
from clickhouse_client import ClickHouseClient


class AdvancedQueryAnalyzer:
    """
    Продвинутый анализатор запросов с использованием:
    - EXPLAIN indexes = 1 (для full scan),
    - EXPLAIN PLAN (для сортировки/агрегации),
    - EXPLAIN PIPELINE (для сложности пайплайна)
    """

    def __init__(self, client: ClickHouseClient):
        self.client = client

    def get_explain_plan(self, query: str) -> Dict:
        """
        Надёжный анализ запроса через EXPLAIN indexes = 1.
        Дополнительные EXPLAIN (PLAN, PIPELINE) — опциональны.
        """
        try:
            query = query.strip().rstrip(';')

            # Унифицированная функция безопасного выполнения EXPLAIN
            def safe_explain(explain_type: str) -> List[str]:
                try:
                    result = self.client.execute_query(f"EXPLAIN {explain_type} {query}")
                    return [step[0] for step in result.data] if result.data else []
                except Exception:
                    return []  # Молча игнорируем ошибку

            # Основной источник — только EXPLAIN indexes = 1
            indexes_lines = safe_explain("indexes = 1")

            # Опционально — PLAN и PIPELINE (если поддерживается)
            plan_lines = safe_explain("PLAN")
            pipeline_steps = safe_explain("PIPELINE")

            # Анализ full scan
            has_full_scan = False
            for line in indexes_lines:
                if 'Parts:' in line:
                    parts_match = re.search(r'Parts:\s*(\d+)/(\d+)', line)
                    if parts_match:
                        read_parts = int(parts_match.group(1))
                        total_parts = int(parts_match.group(2))
                        if total_parts > 10 and read_parts >= total_parts:
                            has_full_scan = True
                            break

            # Извлекаем метрики из indexes_lines, если PLAN не доступен
            has_sorting = any('Sorting' in line for line in indexes_lines)
            has_aggregation = any('Aggregating' in line for line in indexes_lines)

            return {
                'indexes_lines': indexes_lines,
                'plan_lines': plan_lines,
                'pipeline_steps': pipeline_steps,
                'has_full_scan': has_full_scan,
                'has_sorting': has_sorting,
                'has_aggregation': has_aggregation,
                'pipeline_complexity': len(pipeline_steps),
                'explain_output': "\n".join(indexes_lines)  # Для сохранения в БД
            }
        except Exception as e:
            return {'error': str(e)}

    def get_table_stats(self, table_name: str, database: str = 'default') -> Dict:
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
        tables = []
        from_matches = re.findall(r'FROM\s+(\w+)', query, re.IGNORECASE)
        join_matches = re.findall(r'JOIN\s+(\w+)', query, re.IGNORECASE)
        dml_matches = re.findall(r'(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)?\s*(\w+)', query, re.IGNORECASE)
        return list(set(from_matches + join_matches + dml_matches))

    def analyze_with_explain(self, query: str) -> Dict:
        analysis = {
            'query': query,
            'explain_analysis': {},
            'table_analysis': {},
            'recommendations': [],
            'warnings': [],
            'tables_found': [],
            'complexity_score': 0,
        }

        # 1. Получаем EXPLAIN indexes = 1
        explain_data = self.get_explain_plan(query)
        analysis['explain_analysis'] = explain_data

        if 'error' not in explain_data:
            # 2. Извлекаем признаки из EXPLAIN indexes = 1
            indexes_lines = explain_data.get('indexes_lines', [])
            has_full_scan = explain_data.get('has_full_scan', False)
            has_sorting = explain_data.get('has_sorting', False)
            has_aggregation = explain_data.get('has_aggregation', False)
            pipeline_complexity = explain_data.get('pipeline_complexity', 0)

            # 3. Рекомендации на основе EXPLAIN
            if has_full_scan:
                analysis['warnings'].append({
                    'type': 'full_scan',
                    'message': 'Обнаружено полносканирование таблицы',
                    'priority': 'high'
                })
                analysis['recommendations'].append(
                    'Добавьте партиционирование или пропускающий индекс по полю фильтрации')

            if has_sorting:
                analysis['warnings'].append({
                    'type': 'sorting',
                    'message': 'Обнаружена операция сортировки',
                    'priority': 'medium'
                })
                analysis['recommendations'].append('Используйте ключ сортировки или проекцию для избежания сортировки')

            if has_aggregation:
                analysis['recommendations'].append('Рассмотрите агрегирующую проекцию для ускорения GROUP BY')

            if pipeline_complexity > 100:
                analysis['warnings'].append({
                    'type': 'complex_pipeline',
                    'message': f'Сложный pipeline ({pipeline_complexity} шагов)',
                    'priority': 'medium'
                })

            # 4. Расчёт сложности
            score = 0
            if has_full_scan: score += 30
            if has_sorting: score += 15
            if has_aggregation: score += 20
            if pipeline_complexity > 100:
                score += 35
            elif pipeline_complexity > 50:
                score += 20
            analysis['complexity_score'] = min(score, 100)

        # 5. Анализ таблиц
        tables = self.extract_tables_from_query(query)
        analysis['tables_found'] = tables

        for table in tables:
            table_stats = self.get_table_stats(table)
            analysis['table_analysis'][table] = table_stats

            if table_stats and 'error' not in table_stats:
                if table_stats.get('size_gb', 0) > 1:
                    analysis['warnings'].append({
                        'type': 'large_table',
                        'message': f'Большая таблица {table}: {table_stats["size_gb"]:.1f} GB',
                        'priority': 'info'
                    })

                if table_stats.get('engine') == 'MergeTree':
                    if not table_stats.get('partition_key'):
                        analysis['recommendations'].append(
                            f'Для таблицы {table} рассмотрите добавление партиционирования по дате')
                    if not table_stats.get('sorting_key'):
                        analysis['recommendations'].append(
                            f'Для таблицы {table} рассмотрите добавление ключа сортировки')

        # 6. Статический анализ запроса
        query_upper = query.upper()

        if 'SELECT *' in query_upper:
            analysis['warnings'].append({
                'type': 'select_all',
                'message': 'Используется SELECT *',
                'priority': 'medium'
            })
            analysis['recommendations'].append('Укажите конкретные колонки вместо SELECT *')

        if 'LIMIT' in query_upper and 'ORDER BY' not in query_upper:
            analysis['warnings'].append({
                'type': 'limit_no_order',
                'message': 'LIMIT без ORDER BY может давать непредсказуемые результаты',
                'priority': 'low'
            })

        return analysis

    def get_query_history_stats(self, normalized_query_hash: str, days: int = 7) -> Dict:
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
        basic_analysis = self.analyze_with_explain(query)
        explain_data = basic_analysis['explain_analysis']

        report = {
            'basic_analysis': basic_analysis,
            'performance_metrics': {}
        }

        if query_hash:
            history_stats = self.get_query_history_stats(query_hash)
            report['performance_metrics']['history'] = history_stats

        complexity_score = self._calculate_complexity_score(explain_data)
        report['complexity_score'] = complexity_score

        return report

    def _calculate_complexity_score(self, explain_analysis: dict) -> int:
        score = 0
        if explain_analysis.get('has_full_scan'):
            score += 30
        if explain_analysis.get('has_sorting'):
            score += 15
        if explain_analysis.get('has_aggregation'):
            score += 20

        pipeline_complexity = explain_analysis.get('pipeline_complexity', 0)
        if pipeline_complexity > 100:
            score += 35
        elif pipeline_complexity > 50:
            score += 20
        elif pipeline_complexity > 20:
            score += 10

        return min(score, 100)


def get_advanced_analyzer():
    return AdvancedQueryAnalyzer(ClickHouseClient())
