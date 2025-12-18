# data_registry/apps.py
from django.apps import AppConfig

class DataRegistryConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'data_registry'
    verbose_name = 'Data Registry'