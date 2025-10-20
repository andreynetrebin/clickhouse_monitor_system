from django.db import models
from django.contrib.auth.models import User


class SlowQuery(models.Model):
    STATUS_CHOICES = [
        ('new', '🆕 Новый'),
        ('in_analysis', '🔍 В анализе'),
        ('waiting_feedback', '⏳ Ожидает фидбека'),
        ('optimized', '✅ Оптимизирован'),
        ('ignored', '❌ Игнорирован'),
        ('cannot_optimize', '🚫 Не подлежит оптимизации'),
    ]

    PROBLEM_CATEGORIES = [
        ('full_scan', '📊 Полносканирование таблицы'),
        ('missing_index', '📈 Отсутствие индекса'),
        ('cross_join', '❌ CROSS JOIN'),
        ('subquery', '🔄 Неоптимизированный подзапрос'),
        ('large_result', '💾 Большой результат'),
        ('memory_usage', '🧠 Высокое использование памяти'),
        ('network', '🌐 Сетевые проблемы'),
        ('configuration', '⚙️ Проблема конфигурации'),
        ('other', '❓ Другое'),
    ]

    query_log = models.OneToOneField('monitor.QueryLog', on_delete=models.CASCADE, verbose_name="Лог запроса")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='new', db_index=True,
                              verbose_name="Статус")

    # Анализ проблемы
    problem_category = models.CharField(max_length=20, choices=PROBLEM_CATEGORIES, blank=True,
                                        verbose_name="Категория проблемы")
    analysis_notes = models.TextField(blank=True, verbose_name="Заметки анализа")
    tags = models.CharField(max_length=500, blank=True, verbose_name="Теги", help_text="Через запятую")

    # Оптимизация
    optimized_query = models.TextField(blank=True, verbose_name="Оптимизированный запрос")
    optimization_notes = models.TextField(blank=True, verbose_name="Объяснение оптимизации")
    expected_improvement = models.FloatField(null=True, blank=True, verbose_name="Ожидаемое улучшение (%)")

    # Результаты
    actual_improvement = models.FloatField(null=True, blank=True, verbose_name="Фактическое улучшение (%)")
    before_duration_ms = models.FloatField(null=True, blank=True, verbose_name="Длительность до (мс)")
    after_duration_ms = models.FloatField(null=True, blank=True, verbose_name="Длительность после (мс)")

    # Метаданные
    assigned_to = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL, verbose_name="Назначен")
    analysis_started_at = models.DateTimeField(null=True, blank=True, verbose_name="Начало анализа")
    optimized_at = models.DateTimeField(null=True, blank=True, verbose_name="Время оптимизации")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Медленный запрос"
        verbose_name_plural = "Медленные запросы"
        ordering = ['-created_at']

    def __str__(self):
        return f"SlowQuery #{self.id} ({self.status})"

    def get_duration_seconds(self):
        """Длительность исходного запроса в секундах"""
        return self.query_log.duration_ms / 1000 if self.query_log.duration_ms else 0

    def get_improvement_color(self):
        """Цвет для отображения улучшения"""
        if not self.actual_improvement:
            return 'gray'
        elif self.actual_improvement > 50:
            return 'green'
        elif self.actual_improvement > 20:
            return 'orange'
        else:
            return 'red'


class QueryAnalysisResult(models.Model):
    """
    Результаты расширенного анализа запроса
    """
    query_log = models.OneToOneField('monitor.QueryLog', on_delete=models.CASCADE, verbose_name="Лог запроса")

    # Основные метрики анализа
    complexity_score = models.IntegerField(default=0, verbose_name="Оценка сложности")
    has_full_scan = models.BooleanField(default=False, verbose_name="Полносканирование")
    has_sorting = models.BooleanField(default=False, verbose_name="Сортировка")
    has_aggregation = models.BooleanField(default=False, verbose_name="Агрегация")
    pipeline_complexity = models.IntegerField(default=0, verbose_name="Сложность pipeline")

    # Статистика таблиц (JSON поле)
    table_stats = models.JSONField(default=dict, verbose_name="Статистика таблиц")

    # Рекомендации (JSON поле)
    recommendations = models.JSONField(default=list, verbose_name="Рекомендации")
    warnings = models.JSONField(default=list, verbose_name="Предупреждения")

    # EXPLAIN данные (JSON поле)
    explain_plan = models.JSONField(default=list, verbose_name="EXPLAIN PLAN")
    explain_pipeline = models.JSONField(default=list, verbose_name="EXPLAIN PIPELINE")

    # Производительность
    estimated_improvement = models.FloatField(null=True, blank=True, verbose_name="Ожидаемое улучшение (%)")
    analysis_duration_ms = models.FloatField(default=0, verbose_name="Время анализа (мс)")

    # Метаданные
    analyzed_at = models.DateTimeField(auto_now_add=True, verbose_name="Время анализа")
    analysis_version = models.CharField(max_length=20, default='1.0', verbose_name="Версия анализа")

    class Meta:
        verbose_name = "Результат анализа запроса"
        verbose_name_plural = "Результаты анализа запросов"
        ordering = ['-analyzed_at']

    def __str__(self):
        return f"Analysis for Query #{self.query_log.id}"

    def get_recommendations_count(self):
        return len(self.recommendations)

    def get_warnings_count(self):
        return len(self.warnings)

    def get_critical_warnings(self):
        return [w for w in self.warnings if w.get('priority') == 'critical']

    def get_high_priority_warnings(self):
        return [w for w in self.warnings if w.get('priority') in ['critical', 'high']]


class TableAnalysis(models.Model):
    """
    Детальный анализ таблиц, используемых в запросах
    """
    table_name = models.CharField(max_length=200, verbose_name="Название таблицы")
    database = models.CharField(max_length=100, default='default', verbose_name="База данных")

    # Статистика таблицы
    total_rows = models.BigIntegerField(default=0, verbose_name="Всего строк")
    total_bytes = models.BigIntegerField(default=0, verbose_name="Размер в байтах")
    engine = models.CharField(max_length=100, blank=True, verbose_name="Движок")
    partition_key = models.TextField(blank=True, verbose_name="Ключ партиционирования")
    sorting_key = models.TextField(blank=True, verbose_name="Ключ сортировки")

    # Анализ использования
    query_count = models.IntegerField(default=0, verbose_name="Количество запросов")
    avg_duration_ms = models.FloatField(default=0, verbose_name="Среднее время выполнения")
    last_analyzed = models.DateTimeField(auto_now=True, verbose_name="Последний анализ")

    # Рекомендации для таблицы
    table_recommendations = models.JSONField(default=list, verbose_name="Рекомендации для таблицы")

    class Meta:
        verbose_name = "Анализ таблицы"
        verbose_name_plural = "Анализ таблиц"
        unique_together = ['table_name', 'database']
        indexes = [
            models.Index(fields=['table_name', 'database']),
            models.Index(fields=['total_rows']),
        ]

    def __str__(self):
        return f"{self.database}.{self.table_name}"

    def get_size_gb(self):
        return self.total_bytes / (1024 ** 3) if self.total_bytes else 0

    def get_size_mb(self):
        return self.total_bytes / (1024 ** 2) if self.total_bytes else 0


class IndexRecommendation(models.Model):
    """
    Рекомендации по созданию индексов
    """
    ANALYSIS_SOURCE_CHOICES = [
        ('explain', 'EXPLAIN анализ'),
        ('query_log', 'Query Log статистика'),
        ('manual', 'Ручная рекомендация'),
    ]

    table_analysis = models.ForeignKey(TableAnalysis, on_delete=models.CASCADE, verbose_name="Анализ таблицы")
    column_name = models.CharField(max_length=200, verbose_name="Название колонки")
    index_type = models.CharField(max_length=50, default='skip', verbose_name="Тип индекса")
    recommendation_reason = models.TextField(verbose_name="Причина рекомендации")
    expected_improvement = models.FloatField(null=True, blank=True, verbose_name="Ожидаемое улучшение (%)")
    analysis_source = models.CharField(max_length=20, choices=ANALYSIS_SOURCE_CHOICES, default='explain')

    # Статистика использования
    query_count = models.IntegerField(default=0, verbose_name="Количество запросов")
    avg_filter_selectivity = models.FloatField(null=True, blank=True, verbose_name="Селективность фильтра")

    created_at = models.DateTimeField(auto_now_add=True)
    implemented = models.BooleanField(default=False, verbose_name="Реализовано")
    implemented_at = models.DateTimeField(null=True, blank=True, verbose_name="Время реализации")

    class Meta:
        verbose_name = "Рекомендация индекса"
        verbose_name_plural = "Рекомендации индексов"
        ordering = ['-expected_improvement']

    def __str__(self):
        return f"Index on {self.table_analysis.table_name}.{self.column_name}"