from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView

urlpatterns = [
    # Редирект с корня на дашборд мониторинга
    path('', RedirectView.as_view(pattern_name='monitor_dashboard', permanent=False)),

    path('admin/', admin.site.urls),
    path('monitor/', include('monitor.urls')),
    path('lab/', include('query_lab.urls')),
]