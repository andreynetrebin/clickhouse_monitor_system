import time
from django.utils import timezone
from .models import QueryAnalysisResult, TableAnalysis, IndexRecommendation
from .advanced_analyzer import get_advanced_analyzer


class AnalysisService:
    """
    Сервис для сохранения и управления результатами анализа
    """

    def __init__(self):
        self.analyzer = get_advanced_analyzer()

    def analyze_and_save(self, query_log, force_refresh=False):
        """
        Анализирует запрос и сохраняет результаты
        """
        # Проверяем, есть ли уже анализ
        if not force_refresh:
            existing_analysis = QueryAnalysisResult.objects.filter(
                query_log=query_log
            ).first()

            if existing_analysis and (
                    timezone.now() - existing_analysis.analyzed_at
            ).days < 1:  # Не обновляем если анализ свежий (1 день)
                return existing_analysis

        start_time = time.time()

        try:
            # Выполняем анализ
            analysis_data = self.analyzer.analyze_with_explain(query_log.query_text)

            # Сохраняем результаты
            analysis_result = QueryAnalysisResult.objects.update_or_create(
                query_log=query_log,
                defaults={
                    'complexity_score': self.analyzer._calculate_complexity_score(query_log.query_text),
                    'has_full_scan': analysis_data.get('explain_analysis', {}).get('has_full_scan', False),
                    'has_sorting': analysis_data.get('explain_analysis', {}).get('has_sorting', False),
                    'has_aggregation': analysis_data.get('explain_analysis', {}).get('has_aggregation', False),
                    'pipeline_complexity': analysis_data.get('explain_analysis', {}).get('pipeline_complexity', 0),
                    'table_stats': analysis_data.get('table_analysis', {}),
                    'recommendations': analysis_data.get('recommendations', []),
                    'warnings': analysis_data.get('warnings', []),
                    'explain_plan': analysis_data.get('explain_analysis', {}).get('plan_steps', []),
                    'explain_pipeline': analysis_data.get('explain_analysis', {}).get('pipeline_steps', []),
                    'analysis_duration_ms': (time.time() - start_time) * 1000,
                }
            )[0]

            # Сохраняем анализ таблиц
            self._save_table_analysis(analysis_data.get('table_analysis', {}))

            # Генерируем рекомендации по индексам
            self._generate_index_recommendations(analysis_data, query_log)

            return analysis_result

        except Exception as e:
            # В случае ошибки сохраняем информацию об ошибке
            analysis_result = QueryAnalysisResult.objects.update_or_create(
                query_log=query_log,
                defaults={
                    'warnings': [{'type': 'analysis_error', 'message': str(e), 'priority': 'high'}],
                    'analysis_duration_ms': (time.time() - start_time) * 1000,
                }
            )[0]
            return analysis_result

    def _save_table_analysis(self, table_analysis_data):
        """
        Сохраняет анализ таблиц
        """
        for table_name, stats in table_analysis_data.items():
            if stats and 'error' not in stats:
                TableAnalysis.objects.update_or_create(
                    table_name=table_name,
                    database=stats.get('database', 'default'),
                    defaults={
                        'total_rows': stats.get('total_rows', 0),
                        'total_bytes': stats.get('total_bytes', 0),
                        'engine': stats.get('engine', ''),
                        'partition_key': stats.get('partition_key', ''),
                        'sorting_key': stats.get('sorting_key', ''),
                    }
                )

    def _generate_index_recommendations(self, analysis_data, query_log):
        """
        Генерирует рекомендации по индексам на основе анализа
        """
        if analysis_data.get('explain_analysis', {}).get('has_full_scan'):
            # Анализируем условия WHERE для рекомендаций индексов
            where_conditions = self._extract_where_conditions(query_log.query_text)

            for table_name, column_name in where_conditions:
                table_analysis = TableAnalysis.objects.filter(
                    table_name=table_name
                ).first()

                if table_analysis:
                    IndexRecommendation.objects.get_or_create(
                        table_analysis=table_analysis,
                        column_name=column_name,
                        defaults={
                            'index_type': 'skip',
                            'recommendation_reason': 'Полносканирование таблицы в условиях WHERE',
                            'expected_improvement': 70.0,  # Примерное улучшение
                            'analysis_source': 'explain',
                            'query_count': 1,
                        }
                    )

    def _extract_where_conditions(self, query_text):
        """
        Извлекает условия WHERE из запроса для рекомендаций индексов
        """
        conditions = []
        query_upper = query_text.upper()

        # Простой парсинг условий WHERE
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+(GROUP|ORDER|LIMIT|$))', query_upper, re.IGNORECASE | re.DOTALL)

        if where_match:
            where_clause = where_match.group(1)
            # Ищем условия с колонками
            column_matches = re.findall(r'(\w+)\.?(\w+)\s*[=<>!]', where_clause)
            conditions.extend(column_matches)

        return conditions

    def get_analysis_stats(self):
        """
        Возвращает статистику по анализам
        """
        total_analyses = QueryAnalysisResult.objects.count()
        analyses_with_full_scan = QueryAnalysisResult.objects.filter(has_full_scan=True).count()
        avg_complexity = QueryAnalysisResult.objects.aggregate(
            avg=Avg('complexity_score')
        )['avg'] or 0

        return {
            'total_analyses': total_analyses,
            'analyses_with_full_scan': analyses_with_full_scan,
            'full_scan_percentage': (analyses_with_full_scan / total_analyses * 100) if total_analyses else 0,
            'avg_complexity': avg_complexity,
            'total_tables_analyzed': TableAnalysis.objects.count(),
            'total_index_recommendations': IndexRecommendation.objects.count(),
        }


# Синглтон для удобного доступа
analysis_service = AnalysisService()