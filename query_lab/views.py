from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Avg, Q, F, ExpressionWrapper, FloatField, Sum
from django.db.models.functions import TruncDay, TruncWeek
from django.utils import timezone
from django.http import HttpResponse
from datetime import timedelta
import csv
from datetime import datetime
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


@login_required
def analytics_dashboard(request):
    """Дашборд аналитики эффективности лаборатории"""

    # Период анализа (последние 30 дней)
    end_date = timezone.now()
    start_date = end_date - timedelta(days=30)

    # Основные метрики
    total_queries = SlowQuery.objects.filter(created_at__gte=start_date).count()
    optimized_queries = SlowQuery.objects.filter(
        status='optimized',
        created_at__gte=start_date
    ).count()
    avg_improvement = SlowQuery.objects.filter(
        status='optimized',
        actual_improvement__isnull=False
    ).aggregate(avg=Avg('actual_improvement'))['avg'] or 0

    # Распределение по категориям проблем
    problem_categories = SlowQuery.objects.filter(
        created_at__gte=start_date
    ).exclude(problem_category='').values(
        'problem_category'
    ).annotate(
        count=Count('id'),
        avg_improvement=Avg('actual_improvement')
    ).order_by('-count')

    # Статистика по времени анализа
    analysis_time_stats = SlowQuery.objects.filter(
        status='optimized',
        analysis_started_at__isnull=False,
        optimized_at__isnull=False
    ).annotate(
        analysis_days=ExpressionWrapper(
            F('optimized_at') - F('analysis_started_at'),
            output_field=FloatField()
        )
    ).aggregate(
        avg_analysis_days=Avg('analysis_days')
    )

    # Тренды по дням
    daily_trends = SlowQuery.objects.filter(
        created_at__gte=start_date
    ).annotate(
        day=TruncDay('created_at')
    ).values('day').annotate(
        total=Count('id'),
        optimized=Count('id', filter=Q(status='optimized')),
        improvement=Avg('actual_improvement', filter=Q(status='optimized'))
    ).order_by('day')

    # Топ успешных оптимизаций
    top_optimizations = SlowQuery.objects.filter(
        status='optimized',
        actual_improvement__isnull=False
    ).select_related('query_log').order_by('-actual_improvement')[:10]

    # Эффективность по аналитикам
    analyst_performance = SlowQuery.objects.filter(
        assigned_to__isnull=False,
        created_at__gte=start_date
    ).values(
        'assigned_to__username',
        'assigned_to__first_name',
        'assigned_to__last_name'
    ).annotate(
        total=Count('id'),
        optimized=Count('id', filter=Q(status='optimized')),
        avg_improvement=Avg('actual_improvement', filter=Q(status='optimized'))
    ).order_by('-optimized')

    context = {
        'total_queries': total_queries,
        'optimized_queries': optimized_queries,
        'optimization_rate': (optimized_queries / total_queries * 100) if total_queries else 0,
        'avg_improvement': avg_improvement,
        'problem_categories': problem_categories,
        'analysis_time_stats': analysis_time_stats,
        'daily_trends': list(daily_trends),
        'top_optimizations': top_optimizations,
        'analyst_performance': analyst_performance,
        'start_date': start_date,
        'end_date': end_date,
    }
    return render(request, 'query_lab/analytics_dashboard.html', context)


@login_required
def optimization_report(request):
    """Детальный отчет по оптимизациям"""
    # Параметры фильтрации
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    category = request.GET.get('category')
    status = request.GET.get('status')

    # Базовый queryset - используем prefetch_related для безопасного доступа
    queries = SlowQuery.objects.select_related('query_log').prefetch_related('assigned_to')

    # Применяем фильтры
    if start_date:
        queries = queries.filter(created_at__gte=start_date)
    if end_date:
        queries = queries.filter(created_at__lte=end_date)
    if category:
        queries = queries.filter(problem_category=category)
    if status:
        queries = queries.filter(status=status)

    # Статистика для отчета
    stats = {
        'total': queries.count(),
        'optimized': queries.filter(status='optimized').count(),
        'avg_improvement': queries.filter(
            status='optimized',
            actual_improvement__isnull=False
        ).aggregate(avg=Avg('actual_improvement'))['avg'] or 0,
        'total_time_saved': queries.filter(
            status='optimized',
            before_duration_ms__isnull=False,
            after_duration_ms__isnull=False
        ).aggregate(
            total_saved=Sum(F('before_duration_ms') - F('after_duration_ms'))
        )['total_saved'] or 0,
    }

    context = {
        'queries': queries.order_by('-created_at'),
        'stats': stats,
        'filters': {
            'start_date': start_date,
            'end_date': end_date,
            'category': category,
            'status': status,
        },
        'category_choices': SlowQuery.PROBLEM_CATEGORIES,
        'status_choices': SlowQuery.STATUS_CHOICES,
    }
    return render(request, 'query_lab/optimization_report.html', context)


@login_required
def export_optimizations_csv(request):
    """Экспорт оптимизаций в CSV"""
    # Параметры фильтрации
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    category = request.GET.get('category')
    status = request.GET.get('status')

    queries = SlowQuery.objects.select_related('query_log').prefetch_related('assigned_to')

    if start_date:
        queries = queries.filter(created_at__gte=start_date)
    if end_date:
        queries = queries.filter(created_at__lte=end_date)
    if category:
        queries = queries.filter(problem_category=category)
    if status:
        queries = queries.filter(status=status)

    response = HttpResponse(content_type='text/csv')
    response[
        'Content-Disposition'] = f'attachment; filename="optimizations_{datetime.now().strftime("%Y%m%d_%H%M")}.csv"'

    writer = csv.writer(response)
    writer.writerow([
        'ID', 'Статус', 'Категория проблемы', 'Аналитик',
        'Исходная длительность (мс)', 'Длительность после (мс)',
        'Улучшение (%)', 'Экономия времени (мс)',
        'Прочитано строк', 'Прочитано байт',
        'Дата создания', 'Дата оптимизации', 'Теги'
    ])

    for query in queries:
        time_saved = query.before_duration_ms - query.after_duration_ms if query.before_duration_ms and query.after_duration_ms else 0

        # Безопасное получение имени аналитика
        analyst_name = ''
        if query.assigned_to:
            analyst_name = query.assigned_to.get_full_name() or query.assigned_to.username

        writer.writerow([
            query.id,
            query.get_status_display(),
            query.get_problem_category_display(),
            analyst_name,
            query.before_duration_ms or query.query_log.duration_ms,
            query.after_duration_ms,
            query.actual_improvement,
            time_saved,
            query.query_log.read_rows,
            query.query_log.read_bytes,
            query.created_at.strftime("%Y-%m-%d %H:%M"),
            query.optimized_at.strftime("%Y-%m-%d %H:%M") if query.optimized_at else '',
            query.tags
        ])

    return response


@login_required
def performance_report(request):
    """Отчет по производительности системы"""
    # Период анализа (последние 90 дней)
    end_date = timezone.now()
    start_date = end_date - timedelta(days=90)

    # Еженедельная статистика
    weekly_stats = SlowQuery.objects.filter(
        created_at__gte=start_date
    ).annotate(
        week=TruncWeek('created_at')
    ).values('week').annotate(
        total=Count('id'),
        optimized=Count('id', filter=Q(status='optimized')),
        avg_improvement=Avg('actual_improvement', filter=Q(status='optimized')),
        avg_analysis_days=Avg(
            ExpressionWrapper(
                F('optimized_at') - F('analysis_started_at'),
                output_field=FloatField()
            ),
            filter=Q(analysis_started_at__isnull=False, optimized_at__isnull=False)
        )
    ).order_by('week')

    # Эффективность по месяцам
    monthly_trends = []
    current = start_date.replace(day=1)
    while current <= end_date:
        next_month = current.replace(day=28) + timedelta(days=4)  # Переход к следующему месяцу
        next_month = next_month.replace(day=1)

        month_queries = SlowQuery.objects.filter(
            created_at__gte=current,
            created_at__lt=next_month
        )

        month_optimized = month_queries.filter(status='optimized')
        month_improvement = month_optimized.aggregate(
            avg=Avg('actual_improvement', filter=Q(actual_improvement__isnull=False))
        )['avg'] or 0

        monthly_trends.append({
            'month': current.strftime("%Y-%m"),
            'total': month_queries.count(),
            'optimized': month_optimized.count(),
            'improvement': month_improvement,
            'optimization_rate': (month_optimized.count() / month_queries.count() * 100) if month_queries.count() else 0
        })

        current = next_month

    context = {
        'weekly_stats': list(weekly_stats),
        'monthly_trends': monthly_trends,
        'start_date': start_date,
        'end_date': end_date,
    }
    return render(request, 'query_lab/performance_report.html', context)
