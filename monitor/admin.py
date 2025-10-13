from django.contrib import admin
from .models import ClickHouseInstance, QueryLog

@admin.register(ClickHouseInstance)
class ClickHouseInstanceAdmin(admin.ModelAdmin):
    list_display = ['name', 'host', 'port', 'database', 'is_active']
    list_filter = ['is_active']
    search_fields = ['name', 'host']

@admin.register(QueryLog)
class QueryLogAdmin(admin.ModelAdmin):
    list_display = ['query_id', 'clickhouse_instance', 'user', 'duration_ms', 'read_rows', 'query_start_time']
    list_filter = ['clickhouse_instance', 'is_slow', 'query_start_time']
    search_fields = ['query_text', 'query_id']
    readonly_fields = ['collected_at']