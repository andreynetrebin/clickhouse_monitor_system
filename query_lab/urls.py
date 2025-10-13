from django.urls import path
from . import views

urlpatterns = [
    path('', views.lab_dashboard, name='lab_dashboard'),
]