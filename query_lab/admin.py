from django.contrib import admin
from .models import SlowQuery


@admin.register(SlowQuery)
class SlowQueryAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'status', 'problem_category', 'assigned_to',
        'get_duration_seconds', 'expected_improvement', 'created_at'
    ]
    list_filter = ['status', 'problem_category', 'created_at', 'assigned_to']
    search_fields = [
        'query_log__query_text', 'analysis_notes',
        'optimization_notes', 'tags'
    ]
    readonly_fields = ['created_at', 'updated_at']
    fieldsets = (
        ('Основная информация', {
            'fields': ('query_log', 'status', 'assigned_to')
        }),
        ('Анализ проблемы', {
            'fields': ('problem_category', 'analysis_notes', 'tags')
        }),
        ('Оптимизация', {
            'fields': ('optimized_query', 'optimization_notes', 'expected_improvement')
        }),
        ('Результаты', {
            'fields': ('actual_improvement', 'before_duration_ms', 'after_duration_ms')
        }),
        ('Временные метки', {
            'fields': ('analysis_started_at', 'optimized_at', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def get_duration_seconds(self, obj):
        return f"{obj.get_duration_seconds():.1f}с"

    get_duration_seconds.short_description = 'Длительность'