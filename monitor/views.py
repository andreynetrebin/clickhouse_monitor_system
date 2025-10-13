from django.shortcuts import render
from .models import QueryLog, ClickHouseInstance

def dashboard(request):
    context = {
        'total_queries': QueryLog.objects.count(),
        'slow_queries': QueryLog.objects.filter(is_slow=True).count(),
        'instances': ClickHouseInstance.objects.filter(is_active=True),
    }
    return render(request, 'monitor/dashboard.html', context)