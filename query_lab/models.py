from django.db import models


class SlowQuery(models.Model):
    STATUS_CHOICES = [
        ('new', 'Новый'),
        ('in_analysis', 'В анализе'),
        ('optimized', 'Оптимизирован'),
        ('ignored', 'Игнорирован'),
    ]

    query_log = models.OneToOneField('monitor.QueryLog', on_delete=models.CASCADE, verbose_name="Лог запроса")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='new', db_index=True,
                              verbose_name="Статус")

    # Анализ проблемы
    problem_pattern = models.CharField(max_length=200, blank=True, verbose_name="Паттерн проблемы")
    analysis_notes = models.TextField(blank=True, verbose_name="Заметки анализа")

    # Оптимизация
    optimized_query = models.TextField(blank=True, verbose_name="Оптимизированный запрос")
    expected_improvement = models.FloatField(null=True, blank=True, verbose_name="Ожидаемое улучшение (%)")

    # Метаданные
    assigned_to = models.ForeignKey('auth.User', null=True, blank=True, on_delete=models.SET_NULL,
                                    verbose_name="Назначен")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Медленный запрос"
        verbose_name_plural = "Медленные запросы"
        ordering = ['-created_at']