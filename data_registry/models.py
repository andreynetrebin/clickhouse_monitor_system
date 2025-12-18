# data_registry/models.py
from django.db import models

class DAGMetadata(models.Model):
    dag_id = models.CharField(max_length=255, verbose_name="DAG ID")
    schedule = models.CharField(max_length=255, blank=True, verbose_name="Расписание")
    description = models.TextField(blank=True, verbose_name="Описание")
    is_paused = models.BooleanField(verbose_name="На паузе")
    fileloc = models.CharField(max_length=500, verbose_name="Путь к файлу DAG")
    last_parsed = models.DateTimeField(null=True, blank=True, verbose_name="Последний парсинг")
    created_at = models.DateTimeField(verbose_name="Время фиксации")
    lineage = models.JSONField(verbose_name="Lineage (JSON)", default=dict)

    class Meta:
        db_table = 'data_registry_dagmetadata'
        verbose_name = "Метаданные DAG"
        verbose_name_plural = "Метаданные DAG-ов"
        ordering = ['-created_at']
        unique_together = ('dag_id', 'created_at')

    def __str__(self):
        return f"{self.dag_id} @ {self.created_at.strftime('%Y-%m-%d %H:%M')}"