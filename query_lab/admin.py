from django.contrib import admin
from .models import SlowQuery

@admin.register(SlowQuery)
class SlowQueryAdmin(admin.ModelAdmin):
    list_display = ['id', 'status', 'problem_pattern', 'assigned_to', 'created_at']
    list_filter = ['status', 'problem_pattern', 'created_at']
    search_fields = ['query_log__query_text', 'analysis_notes']
    readonly_fields = ['created_at', 'updated_at']