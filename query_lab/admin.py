from django.contrib import admin
from .models import SlowQuery, QueryAnalysisResult, TableAnalysis, IndexRecommendation



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

    @admin.register(QueryAnalysisResult)
    class QueryAnalysisResultAdmin(admin.ModelAdmin):
        list_display = ['query_log', 'complexity_score', 'has_full_scan', 'analyzed_at']
        list_filter = ['has_full_scan', 'has_sorting', 'analyzed_at']
        readonly_fields = ['analyzed_at']
        search_fields = ['query_log__query_text']

    @admin.register(TableAnalysis)
    class TableAnalysisAdmin(admin.ModelAdmin):
        list_display = ['table_name', 'database', 'engine', 'total_rows', 'get_size_gb']
        list_filter = ['engine', 'database']
        search_fields = ['table_name']

        def get_size_gb(self, obj):
            return f"{obj.get_size_gb():.2f} GB"

        get_size_gb.short_description = 'Размер'

    @admin.register(IndexRecommendation)
    class IndexRecommendationAdmin(admin.ModelAdmin):
        list_display = ['table_analysis', 'column_name', 'index_type', 'expected_improvement', 'implemented']
        list_filter = ['index_type', 'analysis_source', 'implemented', 'created_at']
        search_fields = ['table_analysis__table_name', 'column_name']