import time
import re
from django.core.management.base import BaseCommand
from django.utils import timezone
from monitor.models import QueryLog, ClickHouseInstance
from query_lab.advanced_analyzer import AdvancedQueryAnalyzer
from query_lab.models import QueryAnalysisResult, TableAnalysis, IndexRecommendation
from clickhouse_client import ClickHouseClient


class Command(BaseCommand):
    help = 'Комплексный анализ запросов с EXPLAIN и статистикой с сохранением результатов'

    def add_arguments(self, parser):
        parser.add_argument('--query-id', type=int, help='ID конкретного запроса для анализа')
        parser.add_argument('--limit', type=int, default=5, help='Количество запросов для анализа')
        parser.add_argument(
            '--instance',
            type=str,
            default='default',
            help='Имя инстанса ClickHouse для анализа'
        )
        parser.add_argument(
            '--force-reanalyze',
            action='store_true',
            help='Принудительный переанализ даже если уже есть результаты'
        )
        parser.add_argument(
            '--save-tables',
            action='store_true',
            help='Сохранять анализ таблиц в отдельные модели'
        )
        parser.add_argument(
            '--analyze-non-select',
            action='store_true',
            help='Анализировать не-SELECT запросы (по умолчанию пропускаются)'
        )

    def handle(self, *args, **options):
        instance_name = options['instance']

        try:
            # Получаем инстанс ClickHouse
            clickhouse_instance, created = ClickHouseInstance.objects.get_or_create(
                name=instance_name,
                defaults={
                    'host': 'configured_in_env',
                    'port': 9000,
                    'username': 'default',
                    'is_active': True,
                }
            )

            if created:
                self.stdout.write(
                    self.style.WARNING(f'Created new instance: {instance_name}')
                )

            # Создаем анализатор с клиентом ClickHouse
            with ClickHouseClient(instance_name) as client:
                analyzer = AdvancedQueryAnalyzer(client)

                if options['query_id']:
                    # Анализ конкретного запроса
                    try:
                        query_log = QueryLog.objects.get(id=options['query_id'])
                        self.analyze_and_save_single_query(
                            analyzer, query_log, options['force_reanalyze'],
                            options['save_tables'], options['analyze_non_select']
                        )
                    except QueryLog.DoesNotExist:
                        self.stdout.write(self.style.ERROR(f'Query #{options["query_id"]} not found'))
                else:
                    # Анализ топ медленных запросов
                    slow_queries = QueryLog.objects.filter(
                        is_slow=True
                    ).order_by('-duration_ms')[:options['limit']]

                    if not slow_queries:
                        self.stdout.write(self.style.WARNING("No slow queries found for analysis"))
                        return

                    for query in slow_queries:
                        self.analyze_and_save_single_query(
                            analyzer, query, options['force_reanalyze'],
                            options['save_tables'], options['analyze_non_select']
                        )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Analysis failed: {e}'))

    def analyze_and_save_single_query(self, analyzer, query_log, force_reanalyze=False,
                                      save_tables=False, analyze_non_select=False):
        """
        Анализ одного запроса с сохранением результатов
        """
        # Проверяем, есть ли уже анализ (если не принудительный переанализ)
        if not force_reanalyze:
            existing_analysis = QueryAnalysisResult.objects.filter(query_log=query_log).first()
            if existing_analysis:
                self.stdout.write(f"\n{'=' * 60}")
                self.stdout.write(f"Анализ уже существует для Query #{query_log.id}")
                self.stdout.write(f"Используйте --force-reanalyze для переанализа")
                return

        self.stdout.write(f"\n{'=' * 60}")
        self.stdout.write(f"Анализ запроса #{query_log.id} ({query_log.duration_ms}ms)")
        self.stdout.write(f"{'=' * 60}")

        # Проверяем тип запроса и извлекаем SELECT часть если нужно
        query_type, analysis_query = self.prepare_query_for_analysis(query_log.query_text)
        self.stdout.write(f"Тип запроса: {query_type}")

        # Пропускаем не-SELECT запросы если не включена опция analyze_non_select
        skip_non_select = not analyze_non_select
        if skip_non_select and query_type not in ['SELECT', 'INSERT_SELECT']:
            self.stdout.write(self.style.WARNING(
                f"⚠️  Пропуск {query_type} запроса (только SELECT поддерживает EXPLAIN)"
            ))

            # Сохраняем базовую информацию даже для не-SELECT запросов
            self.save_basic_analysis(query_log, query_type)
            return

        start_time = time.time()

        try:
            # Выполняем анализ - передаем подготовленный запрос
            analysis = analyzer.analyze_with_explain(analysis_query)
            analysis_duration_ms = (time.time() - start_time) * 1000

            # Добавляем информацию о типе запроса в анализ
            analysis['original_query_type'] = query_type
            analysis['analyzed_query_type'] = 'SELECT' if query_type == 'INSERT_SELECT' else query_type

            # Сохраняем результаты анализа
            analysis_result = self.save_analysis_results(query_log, analysis, analysis_duration_ms, query_type)

            # Сохраняем анализ таблиц если нужно
            if save_tables and analysis.get('table_analysis'):
                self.save_table_analysis(analysis['table_analysis'], query_log)

            # Выводим результаты
            self.print_analysis_results(analysis, analysis_result, query_type)

            self.stdout.write(self.style.SUCCESS(
                f"✓ Анализ сохранен (ID: {analysis_result.id}, время: {analysis_duration_ms:.1f}ms)"
            ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Ошибка анализа запроса: {e}"))

            # Сохраняем информацию об ошибке
            self.save_error_analysis(query_log, str(e), query_type)

    def prepare_query_for_analysis(self, query_text):
        """
        Подготавливает запрос для анализа: определяет тип и извлекает SELECT часть если нужно
        """
        if not query_text:
            return 'UNKNOWN', query_text

        # Очищаем запрос от комментариев и лишних пробелов
        cleaned_query = self.clean_query(query_text)
        query_upper = cleaned_query.upper().strip()

        # Определяем тип запроса
        query_type = self.detect_query_type(cleaned_query)

        # Если это INSERT ... SELECT, извлекаем SELECT часть
        if query_type == 'INSERT_SELECT':
            select_part = self.extract_select_from_insert(cleaned_query)
            if select_part:
                return 'INSERT_SELECT', select_part
            else:
                return 'INSERT', cleaned_query  # Не смогли извлечь SELECT

        # Для других типов возвращаем как есть
        return query_type, cleaned_query

    def clean_query(self, query_text):
        """
        Очищает запрос от комментариев и лишних пробелов, сохраняя структуру CTE
        """
        if not query_text:
            return ""

        # Удаляем блочные комментарии /* ... */
        cleaned = re.sub(r'/\*.*?\*/', '', query_text, flags=re.DOTALL)
        # Удаляем строчные комментарии --
        cleaned = re.sub(r'--.*$', '', cleaned, flags=re.MULTILINE)

        # Сохраняем переносы строк в CTE для лучшего парсинга
        # Заменяем множественные пробелы на один, но сохраняем ключевые переносы
        cleaned = re.sub(r'[ \t]+', ' ', cleaned)
        # Убираем лишние пробелы вокруг скобок и запятых
        cleaned = re.sub(r'\s*([(),])\s*', r'\1', cleaned)

        return cleaned.strip()

    def detect_query_type(self, query_text):
        """
        Определяет тип SQL запроса
        """
        if not query_text:
            return 'UNKNOWN'

        # Очищаем запрос
        cleaned = self.clean_query(query_text)
        query_upper = cleaned.upper()[:200]  # Берем больше символов для анализа CTE

        # Проверяем наличие WITH в начале (может быть CTE перед любым запросом)
        has_cte = query_upper.startswith('WITH') or ' WITH ' in query_upper

        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            # Проверяем различные паттерны INSERT
            if has_cte or ' SELECT ' in query_upper:
                return 'INSERT_SELECT'
            elif ' VALUES ' in query_upper:
                return 'INSERT_VALUES'
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif query_upper.startswith('CREATE'):
            return 'CREATE'
        elif query_upper.startswith('ALTER'):
            return 'ALTER'
        elif query_upper.startswith('DROP'):
            return 'DROP'
        elif query_upper.startswith('OPTIMIZE'):
            return 'OPTIMIZE'
        elif query_upper.startswith('SHOW'):
            return 'SHOW'
        elif query_upper.startswith('DESCRIBE') or query_upper.startswith('DESC'):
            return 'DESCRIBE'
        elif query_upper.startswith('EXPLAIN'):
            return 'EXPLAIN'
        else:
            # Пытаемся определить по ключевым словам
            if 'FROM' in query_upper and ('WHERE' in query_upper or 'JOIN' in query_upper):
                return 'SELECT'
            return 'OTHER'

    def extract_select_from_insert(self, query_text):
        """
        Извлекает SELECT часть из INSERT ... SELECT запроса, включая CTE
        """
        try:
            # Удаляем комментарии для упрощения анализа
            cleaned_query = self.clean_query(query_text)
            query_upper = cleaned_query.upper()

            # Ищем позицию начала INSERT и конец описания вставки
            insert_pattern = r'INSERT\s+INTO\s+[^(]*(?:\s*\([^)]*\))?\s*'
            insert_match = re.search(insert_pattern, cleaned_query, re.IGNORECASE | re.DOTALL)

            if not insert_match:
                return None

            # Получаем часть запроса после INSERT INTO ...
            after_insert = cleaned_query[insert_match.end():].strip()

            # Проверяем различные варианты:

            # 1. Случай: INSERT ... WITH ... SELECT
            if after_insert.upper().startswith('WITH'):
                # Весь запрос после WITH - это CTE + SELECT, который можно анализировать
                return after_insert

            # 2. Случай: INSERT ... SELECT (прямой SELECT)
            select_pos = after_insert.upper().find('SELECT')
            if select_pos != -1:
                select_part = after_insert[select_pos:]
                # Проверяем, что это валидный SELECT (имеет FROM или является подзапросом)
                if self.is_valid_select(select_part):
                    return select_part

            # 3. Случай: INSERT ... (VALUES) - не поддерживается для EXPLAIN
            if after_insert.upper().startswith('VALUES'):
                return None

            # 4. Пытаемся найти любой допустимый для EXPLAIN паттерн
            # Ищем WITH в начале всего запроса (может быть перед INSERT)
            whole_query_with_pos = query_upper.find('WITH')
            if whole_query_with_pos != -1 and whole_query_with_pos < insert_match.start():
                # WITH находится перед INSERT - извлекаем все с WITH
                return cleaned_query[whole_query_with_pos:]

            # 5. Если не нашли явный SELECT, но есть другие конструкции
            # Проверяем наличие CTE паттернов
            cte_patterns = [
                r'WITH\s+[\w"]+\s+AS\s*\([^)]+\)',
                r',\s*[\w"]+\s+AS\s*\([^)]+\)'
            ]

            for pattern in cte_patterns:
                if re.search(pattern, after_insert, re.IGNORECASE):
                    # Нашли CTE - возвращаем часть после INSERT
                    return after_insert

            return None

        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Ошибка извлечения SELECT из INSERT: {e}"))
            return None

    def is_valid_select(self, query_part):
        """
        Проверяет, является ли часть запроса валидным SELECT для EXPLAIN
        """
        query_upper = query_part.upper()

        # Должен содержать SELECT в начале
        if not query_upper.startswith('SELECT'):
            return False

        # Должен содержать хотя бы один из ключевых элементов
        required_keywords = ['FROM', 'WHERE', 'JOIN', 'UNION', 'INTERSECT', 'EXCEPT']

        for keyword in required_keywords:
            if keyword in query_upper:
                return True

        # Или это может быть подзапрос в CTE
        if 'WITH' in query_upper:
            return True

        # Или это может быть простой SELECT без FROM (например, SELECT 1)
        # Но такие запросы редко бывают в INSERT, так что лучше пропустить
        return False

    def save_basic_analysis(self, query_log, query_type):
        """
        Сохраняет базовую информацию для не-SELECT запросов
        """
        try:
            analysis_data = {
                'query_log': query_log,
                'complexity_score': 0,
                'has_full_scan': False,
                'has_sorting': False,
                'has_aggregation': False,
                'pipeline_complexity': 0,
                'table_stats': {},
                'recommendations': [f"Запрос типа {query_type} не поддерживает EXPLAIN анализ"],
                'warnings': [{
                    'message': f'EXPLAIN не поддерживается для {query_type} запросов',
                    'priority': 'info'
                }],
                'explain_plan': {},
                'explain_pipeline': [],
                'estimated_improvement': None,
                'analysis_duration_ms': 0,
                'analyzed_at': timezone.now(),
                'analysis_version': '1.0'
            }

            analysis_result, created = QueryAnalysisResult.objects.update_or_create(
                query_log=query_log,
                defaults=analysis_data
            )

            self.stdout.write(self.style.WARNING(
                f"✓ Базовая информация сохранена для {query_type} запроса"
            ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Ошибка сохранения базового анализа: {e}"))

    def save_error_analysis(self, query_log, error_message, query_type):
        """
        Сохраняет информацию об ошибке анализа
        """
        try:
            analysis_data = {
                'query_log': query_log,
                'complexity_score': 0,
                'has_full_scan': False,
                'has_sorting': False,
                'has_aggregation': False,
                'pipeline_complexity': 0,
                'table_stats': {},
                'recommendations': ['Анализ завершился с ошибкой'],
                'warnings': [{
                    'message': f'Ошибка анализа: {error_message}',
                    'priority': 'critical'
                }],
                'explain_plan': {},
                'explain_pipeline': [],
                'estimated_improvement': None,
                'analysis_duration_ms': 0,
                'analyzed_at': timezone.now(),
                'analysis_version': '1.0'
            }

            analysis_result, created = QueryAnalysisResult.objects.update_or_create(
                query_log=query_log,
                defaults=analysis_data
            )

            self.stdout.write(self.style.WARNING(
                f"✓ Информация об ошибке сохранена"
            ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Ошибка сохранения информации об ошибке: {e}"))

    def save_analysis_results(self, query_log, analysis, analysis_duration_ms, query_type='SELECT'):
        """
        Сохранение результатов анализа в модель QueryAnalysisResult
        """
        explain_analysis = analysis.get('explain_analysis', {})
        table_analysis = analysis.get('table_analysis', {})

        # Подготавливаем данные для сохранения
        analysis_data = {
            'query_log': query_log,
            'complexity_score': analysis.get('complexity_score', 0),
            'has_full_scan': explain_analysis.get('has_full_scan', False),
            'has_sorting': explain_analysis.get('has_sorting', False),
            'has_aggregation': explain_analysis.get('has_aggregation', False),
            'pipeline_complexity': len(analysis.get('explain_pipeline', [])),

            # JSON поля
            'table_stats': table_analysis,
            'recommendations': analysis.get('recommendations', []),
            'warnings': analysis.get('warnings', []),
            'explain_plan': analysis.get('explain_output', ''),
            'explain_pipeline': analysis.get('explain_pipeline', []),

            # Дополнительная информация о типе запроса
            'original_query_type': analysis.get('original_query_type', query_type),
            'analyzed_query_type': analysis.get('analyzed_query_type', query_type),

            # Производительность
            'estimated_improvement': analysis.get('estimated_improvement'),
            'analysis_duration_ms': analysis_duration_ms,

            # Метаданные
            'analyzed_at': timezone.now(),
            'analysis_version': '1.0'
        }

        # Создаем или обновляем запись
        analysis_result, created = QueryAnalysisResult.objects.update_or_create(
            query_log=query_log,
            defaults=analysis_data
        )

        return analysis_result

    def save_table_analysis(self, table_analysis_data, query_log):
        """
        Сохранение анализа таблиц в отдельные модели
        """
        for table_name, table_stats in table_analysis_data.items():
            if table_stats and 'error' not in table_stats:
                try:
                    # Создаем или обновляем анализ таблицы
                    table_analysis, created = TableAnalysis.objects.update_or_create(
                        table_name=table_name,
                        database=table_stats.get('database', 'default'),
                        defaults={
                            'total_rows': table_stats.get('total_rows', 0),
                            'total_bytes': table_stats.get('total_bytes', 0),
                            'engine': table_stats.get('engine', ''),
                            'partition_key': table_stats.get('partition_key', ''),
                            'sorting_key': table_stats.get('sorting_key', ''),
                            'query_count': TableAnalysis.objects.filter(table_name=table_name).count() + 1,
                        }
                    )

                    # Создаем рекомендации по индексам если есть
                    self.create_index_recommendations(table_analysis, table_stats, query_log)

                    if created:
                        self.stdout.write(f"  📊 Создан анализ таблицы: {table_name}")
                    else:
                        self.stdout.write(f"  📊 Обновлен анализ таблицы: {table_name}")

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  Ошибка сохранения таблицы {table_name}: {e}"))

    def create_index_recommendations(self, table_analysis, table_stats, query_log):
        """
        Создание рекомендаций по индексам на основе анализа
        """
        # Анализируем ключи сортировки и партиционирования для рекомендаций
        sorting_key = table_stats.get('sorting_key', '')
        partition_key = table_stats.get('partition_key', '')

        recommendations = []

        # Рекомендация по ключу сортировки если его нет или он неоптимален
        if not sorting_key and table_stats.get('total_rows', 0) > 100000:
            recommendations.append({
                'column_name': 'id',  # или анализировать наиболее частые фильтры
                'index_type': 'skip',
                'recommendation_reason': 'Большая таблица без ключа сортировки',
                'expected_improvement': 50.0,
                'analysis_source': 'query_log'
            })

        # Рекомендация по партиционированию для больших таблиц
        if not partition_key and table_stats.get('total_rows', 0) > 1000000:
            recommendations.append({
                'column_name': 'toYYYYMM(created_at)',  # пример для временных данных
                'index_type': 'partition',
                'recommendation_reason': 'Большая таблица без партиционирования',
                'expected_improvement': 30.0,
                'analysis_source': 'query_log'
            })

        # Сохраняем рекомендации
        for rec_data in recommendations:
            try:
                IndexRecommendation.objects.get_or_create(
                    table_analysis=table_analysis,
                    column_name=rec_data['column_name'],
                    index_type=rec_data['index_type'],
                    defaults={
                        'recommendation_reason': rec_data['recommendation_reason'],
                        'expected_improvement': rec_data['expected_improvement'],
                        'analysis_source': rec_data['analysis_source'],
                        'query_count': 1
                    }
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"    Ошибка создания рекомендации индекса: {e}"))

    def print_analysis_results(self, analysis, analysis_result, query_type):
        """
        Вывод результатов анализа
        """
        # Показываем информацию о типе запроса
        original_type = analysis.get('original_query_type', query_type)
        analyzed_type = analysis.get('analyzed_query_type', query_type)

        if original_type != analyzed_type:
            self.stdout.write(f"📝 Анализируется {analyzed_type} часть из {original_type} запроса")

        if analysis.get('explain_analysis', {}).get('has_full_scan'):
            self.stdout.write(self.style.WARNING("⚠️  Обнаружено полносканирование"))

        if analysis.get('warnings'):
            for warning in analysis['warnings']:
                self.stdout.write(f"⚠️  {warning['message']} ({warning['priority']})")

        # Рекомендации
        if analysis.get('recommendations'):
            self.stdout.write("\n📋 Рекомендации:")
            for rec in analysis['recommendations']:
                self.stdout.write(f"  • {rec}")

        # Статистика таблиц
        if analysis.get('table_analysis'):
            self.stdout.write("\n📊 Таблицы:")
            for table, stats in analysis['table_analysis'].items():
                if stats and 'error' not in stats:
                    self.stdout.write(
                        f"  {table}: {stats['engine']}, {stats['total_rows']} rows, {stats['size_gb']:.1f}GB")

        # Метрики сложности
        self.stdout.write(f"\n🎯 Оценка сложности: {analysis_result.complexity_score}")
        self.stdout.write(f"🔧 Сложность pipeline: {analysis_result.pipeline_complexity}")

        if analysis_result.estimated_improvement:
            self.stdout.write(f"📈 Ожидаемое улучшение: {analysis_result.estimated_improvement}%")

        # Статистика анализа
        self.stdout.write(f"\n⏱️  Время анализа: {analysis_result.analysis_duration_ms:.1f}ms")
        self.stdout.write(f"📝 Рекомендаций: {analysis_result.get_recommendations_count()}")
        self.stdout.write(f"⚠️  Предупреждений: {analysis_result.get_warnings_count()}")

        critical_warnings = analysis_result.get_critical_warnings()
        if critical_warnings:
            self.stdout.write(self.style.ERROR(f"🚨 Критических предупреждений: {len(critical_warnings)}"))
