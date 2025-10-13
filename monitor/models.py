from django.db import models


class ClickHouseInstance(models.Model):
    name = models.CharField(max_length=200, verbose_name="Название инстанса")
    host = models.CharField(max_length=200, verbose_name="Хост")
    port = models.IntegerField(default=8123, verbose_name="Порт")
    database = models.CharField(max_length=100, default='default', verbose_name="База данных")
    username = models.CharField(max_length=100, verbose_name="Пользователь")
    password = models.CharField(max_length=200, blank=True, verbose_name="Пароль")

    # Настройки мониторинга
    is_active = models.BooleanField(default=True, verbose_name="Активный мониторинг")
    slow_query_threshold_ms = models.IntegerField(default=1000, verbose_name="Порог медленных запросов (мс)")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.host}:{self.port})"

    class Meta:
        verbose_name = "Инстанс ClickHouse"
        verbose_name_plural = "Инстансы ClickHouse"


class QueryLog(models.Model):
    # Идентификаторы
    query_id = models.CharField(max_length=255, db_index=True, verbose_name="ID запроса")
    clickhouse_instance = models.ForeignKey(ClickHouseInstance, on_delete=models.CASCADE, verbose_name="Инстанс")

    # Основная информация о запросе
    query_text = models.TextField(verbose_name="Текст запроса")
    normalized_query_hash = models.CharField(max_length=64, db_index=True, verbose_name="Хеш нормализованного запроса")
    user = models.CharField(max_length=100, verbose_name="Пользователь")

    # Метрики производительности
    duration_ms = models.FloatField(verbose_name="Время выполнения (мс)")
    read_rows = models.BigIntegerField(default=0, verbose_name="Прочитано строк")
    read_bytes = models.BigIntegerField(default=0, verbose_name="Прочитано байт")
    memory_usage = models.BigIntegerField(default=0, verbose_name="Использовано памяти")

    # Временные метки
    query_start_time = models.DateTimeField(verbose_name="Время начала запроса")
    collected_at = models.DateTimeField(auto_now_add=True, verbose_name="Время сбора")

    # Флаги
    is_slow = models.BooleanField(default=False, db_index=True, verbose_name="Медленный запрос")
    is_initial = models.BooleanField(default=True, verbose_name="Изначальный запрос")

    class Meta:
        verbose_name = "Лог запроса"
        verbose_name_plural = "Логи запросов"
        indexes = [
            models.Index(fields=['collected_at']),
            models.Index(fields=['normalized_query_hash', 'collected_at']),
        ]
        ordering = ['-query_start_time']