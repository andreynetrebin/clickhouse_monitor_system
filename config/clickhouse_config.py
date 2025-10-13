# config/clickhouse_config.py
import os
from django.conf import settings

def get_clickhouse_config():
    return {
        'default': {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', 8123)),
            'user': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'default'),
            'timeout': int(os.getenv('CLICKHOUSE_TIMEOUT', 10)),
            'verify_ssl': os.getenv('CLICKHOUSE_VERIFY_SSL', 'False').lower() == 'true',
        }
    }

def get_monitoring_config():
    return {
        'slow_query_threshold_ms': int(os.getenv('SLOW_QUERY_THRESHOLD_MS', 1000)),
        'collection_interval_minutes': int(os.getenv('COLLECTION_INTERVAL_MINUTES', 5)),
        'retention_days': int(os.getenv('RETENTION_DAYS', 30)),
    }