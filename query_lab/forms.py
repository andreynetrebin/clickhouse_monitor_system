from django import forms
from .models import SlowQuery

class QueryAnalysisForm(forms.ModelForm):
    """Форма для анализа проблемы"""
    class Meta:
        model = SlowQuery
        fields = ['problem_category', 'analysis_notes', 'tags']
        widgets = {
            'problem_category': forms.Select(attrs={
                'class': 'form-select',
                'style': 'width: 100%; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            }),
            'analysis_notes': forms.Textarea(attrs={
                'class': 'form-textarea',
                'rows': 6,
                'placeholder': 'Опишите проблему, возможные причины и пути решения...',
                'style': 'width: 100%; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px; font-family: inherit;'
            }),
            'tags': forms.TextInput(attrs={
                'class': 'form-input',
                'placeholder': 'медленный_join, полносканирование, большой_результат...',
                'style': 'width: 100%; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            })
        }
        labels = {
            'problem_category': 'Категория проблемы',
            'analysis_notes': 'Заметки анализа',
            'tags': 'Теги (через запятую)'
        }

class QueryOptimizationForm(forms.ModelForm):
    """Форма для предложения оптимизации"""
    class Meta:
        model = SlowQuery
        fields = ['optimized_query', 'optimization_notes', 'expected_improvement']
        widgets = {
            'optimized_query': forms.Textarea(attrs={
                'class': 'form-textarea sql-editor',
                'rows': 8,
                'placeholder': 'Введите оптимизированную версию запроса...',
                'style': 'width: 100%; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px; font-family: monospace; font-size: 0.9rem;'
            }),
            'optimization_notes': forms.Textarea(attrs={
                'class': 'form-textarea',
                'rows': 4,
                'placeholder': 'Объясните, какие изменения были сделаны и почему они должны улучшить производительность...',
                'style': 'width: 100%; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            }),
            'expected_improvement': forms.NumberInput(attrs={
                'class': 'form-input',
                'placeholder': '50',
                'min': 0,
                'max': 1000,
                'step': 1,
                'style': 'width: 100px; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            })
        }
        labels = {
            'optimized_query': 'Оптимизированный запрос',
            'optimization_notes': 'Объяснение оптимизации',
            'expected_improvement': 'Ожидаемое улучшение (%)'
        }

class ResultsForm(forms.ModelForm):
    """Форма для записи результатов оптимизации"""
    class Meta:
        model = SlowQuery
        fields = ['actual_improvement', 'before_duration_ms', 'after_duration_ms']
        widgets = {
            'actual_improvement': forms.NumberInput(attrs={
                'class': 'form-input',
                'placeholder': '75',
                'min': 0,
                'max': 1000,
                'step': 1,
                'style': 'width: 100px; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            }),
            'before_duration_ms': forms.NumberInput(attrs={
                'class': 'form-input',
                'placeholder': '15000',
                'step': 1,
                'style': 'width: 150px; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            }),
            'after_duration_ms': forms.NumberInput(attrs={
                'class': 'form-input',
                'placeholder': '3750',
                'step': 1,
                'style': 'width: 150px; padding: 0.5rem; border: 1px solid #bdc3c7; border-radius: 4px;'
            })
        }
        labels = {
            'actual_improvement': 'Фактическое улучшение (%)',
            'before_duration_ms': 'Длительность до (мс)',
            'after_duration_ms': 'Длительность после (мс)'
        }