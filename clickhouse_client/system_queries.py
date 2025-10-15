import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SystemQueries:
    """
    Класс с системными запросами для сбора метрик из ClickHouse
    """

    @staticmethod
    def get_slow_queries(
            threshold_ms: int = 1000,
            lookback_minutes: int = 5,
            limit: int = 100
    ) -> str:
        """
        Запрос для получения медленных запросов из query_log

        Args:
            threshold_ms: Порог медленных запросов в миллисекундах
            lookback_minutes: За какой период искать запросы (минуты)
            limit: Ограничение количества результатов

        Returns:
            SQL запрос
        """
        return f"""
        SELECT 
            query_id,
            query,
            query_start_time,
            query_duration_ms,
            read_rows,
            read_bytes,
            memory_usage,
            user,
            client_name,
            databases,
            tables,
            columns,
            normalized_query_hash
        FROM system.query_log 
        WHERE query_duration_ms > {threshold_ms}
        AND event_time > now() - INTERVAL {lookback_minutes} MINUTE
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        AND query NOT LIKE '%%system.query_log%%'  -- Исключаем запросы к самой системе мониторинга
        ORDER BY query_duration_ms DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_current_queries(
            threshold_ms: int = 1000
    ) -> str:
        """
        Запрос для получения текущих выполняемых запросов
        Адаптирован для ClickHouse 25.x

        Args:
            threshold_ms: Порог для фильтрации долгих запросов

        Returns:
            SQL запрос
        """
        return f"""
        SELECT 
            query_id,
            query,
            elapsed as query_duration_ms,
            read_rows,
            read_bytes,
            memory_usage,
            user,
            client_name,
            -- Для совместимости с более старыми версиями
            '' as databases,
            '' as tables,
            '' as columns
        FROM system.processes
        WHERE elapsed > {threshold_ms}
        AND query != ''
        ORDER BY elapsed DESC
        """

    @staticmethod
    def get_system_metrics() -> str:
        """
        Запрос для получения системных метрик

        Returns:
            SQL запрос
        """
        return """
        SELECT 
            metric,
            value,
            description
        FROM system.metrics
        WHERE metric IN (
            'Query', 'Merge', 'Read', 'Write', 'TCPConnection', 
            'HTTPConnection', 'OpenFileForRead'
        )
        ORDER BY metric
        """

    @staticmethod
    def get_system_events() -> str:
        """
        Запрос для получения системных событий

        Returns:
            SQL запрос
        """
        return """
        SELECT 
            event,
            value,
            description
        FROM system.events
        WHERE event IN (
            'Query', 'SelectQuery', 'InsertQuery', 'FailedQuery',
            'FailedSelectQuery', 'FailedInsertQuery'
        )
        ORDER BY event
        """

    @staticmethod
    def get_tables_info(
            database: str = 'default'
    ) -> str:
        """
        Запрос для получения информации о таблицах

        Args:
            database: База данных для анализа

        Returns:
            SQL запрос
        """
        return f"""
        SELECT 
            name as table_name,
            engine,
            total_rows,
            total_bytes,
            metadata_modification_time,
            partition_key,
            sorting_key
        FROM system.tables
        WHERE database = '{database}'
        ORDER BY total_bytes DESC
        """

    @staticmethod
    def get_query_log_stats(
            lookback_hours: int = 1
    ) -> str:
        """
        Статистика по query_log для аналитики

        Args:
            lookback_hours: Период для анализа в часах

        Returns:
            SQL запрос
        """
        return f"""
        SELECT 
            count() as total_queries,
            countIf(query_duration_ms > 1000) as slow_queries,
            round(avg(query_duration_ms), 2) as avg_duration_ms,
            round(quantile(0.95)(query_duration_ms), 2) as p95_duration_ms,
            max(query_duration_ms) as max_duration_ms,
            sum(read_rows) as total_rows_read,
            sum(read_bytes) as total_bytes_read
        FROM system.query_log
        WHERE event_time > now() - INTERVAL {lookback_hours} HOUR
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        """

    @staticmethod
    def get_normalized_query_patterns(
            lookback_hours: int = 24,
            min_executions: int = 5
    ) -> str:
        """
        Поиск часто выполняемых паттернов запросов

        Args:
            lookback_hours: Период анализа
            min_executions: Минимальное количество выполнений

        Returns:
            SQL запрос
        """
        return f"""
        SELECT 
            normalized_query_hash,
            any(query) as sample_query,
            count() as execution_count,
            round(avg(query_duration_ms), 2) as avg_duration_ms,
            round(max(query_duration_ms), 2) as max_duration_ms,
            sum(read_rows) as total_rows_read,
            sum(read_bytes) as total_bytes_read
        FROM system.query_log
        WHERE event_time > now() - INTERVAL {lookback_hours} HOUR
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        AND normalized_query_hash != ''
        GROUP BY normalized_query_hash
        HAVING execution_count >= {min_executions}
        ORDER BY execution_count DESC
        LIMIT 50
        """

    @staticmethod
    def get_system_processes_columns() -> str:
        """
        Диагностический запрос для просмотра структуры system.processes
        """
        return """
        SELECT 
            name,
            type
        FROM system.columns 
        WHERE database = 'system' 
        AND table = 'processes'
        ORDER BY name
        """


# Синглтон для удобного доступа
system_queries = SystemQueries()