from django.urls import path
from . import views

app_name = 'query_lab'  # ← ЭТО ОБЯЗАТЕЛЬНО

urlpatterns = [
    path('', views.lab_dashboard, name='lab_dashboard'),
    path('queries/', views.query_list, name='query_list'),
    path('queries/<int:query_id>/', views.query_detail, name='query_detail'),
    path('queries/<int:query_id>/update-status/', views.update_query_status, name='update_query_status'),
    path('analytics/', views.analytics_dashboard, name='analytics_dashboard'),
    path('reports/optimizations/', views.optimization_report, name='optimization_report'),
    path('reports/performance/', views.performance_report, name='performance_report'),
    path('reports/export/csv/', views.export_optimizations_csv, name='export_optimizations_csv'),
    path('api/analyze-query/', views.AnalyzeQueryAPI.as_view(), name='analyze_query_api'),
    path('api-test/', views.api_test_page, name='api_test_page'),
    path('analyze/<int:slow_query_id>/', views.analyze_query, name='analyze_query'),
]