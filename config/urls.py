from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

# Добавляем редирект с корневого URL на мониторинг
def root_redirect(request):
    return redirect('monitor_dashboard')

urlpatterns = [
    path('', root_redirect, name='root'),  # Редирект с корня на мониторинг
    path('admin/', admin.site.urls),
    path('monitor/', include('monitor.urls')),
    path('lab/', include('query_lab.urls')),
]