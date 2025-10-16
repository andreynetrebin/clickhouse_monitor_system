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