from django.urls import path
from . import views

urlpatterns = [
    path('', views.lab_dashboard, name='lab_dashboard'),
    path('queries/', views.query_list, name='query_list'),
    path('queries/<int:query_id>/', views.query_detail, name='query_detail'),
    path('queries/<int:query_id>/update-status/', views.update_query_status, name='update_query_status'),  # ← ДОБАВИТЬ
]