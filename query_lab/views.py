from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from django.utils import timezone
from .models import SlowQuery
from .forms import QueryAnalysisForm, QueryOptimizationForm, ResultsForm


@login_required
def lab_dashboard(request):
    """Дашборд лаборатории запросов"""
    # Статистика по статусам
    status_stats = SlowQuery.objects.values('status').annotate(
        count=Count('id')
    ).order_by('status')

    # Статистика по категориям проблем
    category_stats = SlowQuery.objects.exclude(problem_category='').values(
        'problem_category'
    ).annotate(
        count=Count('id')
    ).order_by('-count')

    # Последние запросы
    recent_queries = SlowQuery.objects.select_related(
        'query_log', 'assigned_to'
    ).order_by('-created_at')[:10]

    # Запросы назначенные текущему пользователю
    my_queries = SlowQuery.objects.filter(
        assigned_to=request.user,
        status__in=['new', 'in_analysis']
    ).count()

    context = {
        'status_stats': status_stats,
        'category_stats': category_stats,
        'recent_queries': recent_queries,
        'my_queries': my_queries,
        'total_queries': SlowQuery.objects.count(),
        'new_queries': SlowQuery.objects.filter(status='new').count(),
    }
    return render(request, 'query_lab/dashboard.html', context)


@login_required
def query_detail(request, query_id):
    """Детальная страница запроса с формами"""
    slow_query = get_object_or_404(
        SlowQuery.objects.select_related('query_log', 'assigned_to'),
        id=query_id
    )

    analysis_form = QueryAnalysisForm(instance=slow_query)
    optimization_form = QueryOptimizationForm(instance=slow_query)
    results_form = ResultsForm(instance=slow_query)

    # Обработка быстрых действий
    if 'quick_action' in request.POST:
        action = request.POST.get('quick_action')

        if action == 'assign_to_me':
            slow_query.assigned_to = request.user
            slow_query.save()

        elif action == 'start_analysis':
            slow_query.status = 'in_analysis'
            slow_query.assigned_to = request.user
            slow_query.analysis_started_at = timezone.now()
            slow_query.save()

        elif action == 'mark_optimized':
            slow_query.status = 'optimized'
            slow_query.optimized_at = timezone.now()
            slow_query.save()

        elif action == 'mark_ignored':
            slow_query.status = 'ignored'
            slow_query.save()

        elif action == 'cannot_optimize':
            slow_query.status = 'cannot_optimize'
            slow_query.save()

        return redirect('query_detail', query_id=query_id)

    # Обработка формы анализа
    if 'analyze' in request.POST:
        analysis_form = QueryAnalysisForm(request.POST, instance=slow_query)
        if analysis_form.is_valid():
            analysis_form.save()
            if slow_query.status == 'new':
                slow_query.status = 'in_analysis'
                slow_query.assigned_to = request.user
                slow_query.save()
            return redirect('query_detail', query_id=query_id)

    # Обработка формы оптимизации
    elif 'optimize' in request.POST:
        optimization_form = QueryOptimizationForm(request.POST, instance=slow_query)
        if optimization_form.is_valid():
            optimization_form.save()
            slow_query.status = 'waiting_feedback'
            slow_query.save()
            return redirect('query_detail', query_id=query_id)

    # Обработка формы результатов
    elif 'save_results' in request.POST:
        results_form = ResultsForm(request.POST, instance=slow_query)
        if results_form.is_valid():
            results_form.save()
            slow_query.status = 'optimized'
            slow_query.save()
            return redirect('query_detail', query_id=query_id)

    context = {
        'sq': slow_query,
        'query_log': slow_query.query_log,
        'analysis_form': analysis_form,
        'optimization_form': optimization_form,
        'results_form': results_form,
    }
    return render(request, 'query_lab/query_detail.html', context)


@login_required
def query_list(request):
    """Список всех запросов в лаборатории"""
    status_filter = request.GET.get('status', '')
    category_filter = request.GET.get('category', '')
    assigned_filter = request.GET.get('assigned', '')

    queries = SlowQuery.objects.select_related('query_log', 'assigned_to')

    # Применяем фильтры
    if status_filter:
        queries = queries.filter(status=status_filter)
    if category_filter:
        queries = queries.filter(problem_category=category_filter)
    if assigned_filter == 'me':
        queries = queries.filter(assigned_to=request.user)
    elif assigned_filter == 'unassigned':
        queries = queries.filter(assigned_to__isnull=True)

    queries = queries.order_by('-created_at')

    context = {
        'queries': queries,
        'status_filter': status_filter,
        'category_filter': category_filter,
        'assigned_filter': assigned_filter,
        'status_choices': SlowQuery.STATUS_CHOICES,
        'category_choices': SlowQuery.PROBLEM_CATEGORIES,
    }
    return render(request, 'query_lab/query_list.html', context)


@login_required
def update_query_status(request, query_id):
    """Обновление статуса запроса"""
    slow_query = get_object_or_404(SlowQuery, id=query_id)

    if request.method == 'POST':
        new_status = request.POST.get('status')
        if new_status in dict(SlowQuery.STATUS_CHOICES):
            slow_query.status = new_status

            if new_status == 'in_analysis' and not slow_query.analysis_started_at:
                slow_query.analysis_started_at = timezone.now()
                slow_query.assigned_to = request.user
            elif new_status == 'optimized' and not slow_query.optimized_at:
                slow_query.optimized_at = timezone.now()

            slow_query.save()

    return redirect('query_detail', query_id=query_id)
