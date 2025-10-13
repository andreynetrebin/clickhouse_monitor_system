from django.shortcuts import render
from .models import SlowQuery

def lab_dashboard(request):
    context = {
        'total_slow_queries': SlowQuery.objects.count(),
        'new_queries': SlowQuery.objects.filter(status='new').count(),
        'in_analysis_queries': SlowQuery.objects.filter(status='in_analysis').count(),
    }
    return render(request, 'query_lab/dashboard.html', context)