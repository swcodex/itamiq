import tempfile
import os
import subprocess
import logging
import time
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from .models import Job, Script
from django.forms import modelformset_factory
from .forms import JobForm, ScriptFormSet
from scheduler.scheduler import remove_job, get_scheduler, add_job, update_job_schedule
from django.utils import timezone
from datetime import timedelta
from apscheduler.jobstores.base import JobLookupError
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from .job_execution import execute_job_core
from django.db import connection
from django.db.utils import ProgrammingError


logger = logging.getLogger(__name__)

def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")


def home(request):
    return render(request, 'pages/reconciliation/home.html')


def add_job(request):
    if request.method == 'POST':
        job_form = JobForm(request.POST)
        script_formset = ScriptFormSet(request.POST)
        if job_form.is_valid() and script_formset.is_valid():
            job = job_form.save(commit=False)
            if 'schedule_days' in job_form.cleaned_data:
                job.set_schedule_days(job_form.cleaned_data['schedule_days'])
            job.save()
            
            scripts = script_formset.save(commit=False)
            for script in scripts:
                script.job = job
                script.save()
            #update_job_schedule(job)
            update_job_schedule(job, app_name='reconciliation', execute_job_func=execute_job_core)
            messages.success(request, 'Job saved successfully.')
            return redirect('reconciliation:job_list')
        else:
            messages.error(request, 'There were errors in your submission. Please check the form and try again.')
    else:
        job_form = JobForm()
        script_formset = ScriptFormSet(initial=[{'order': 1}])  # Set initial order to 1
    
    return render(request, 'pages/reconciliation/add_job.html', {'job_form': job_form, 'script_formset': script_formset})


def edit_job(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    if request.method == 'POST':
        job_form = JobForm(request.POST, instance=job)
        script_formset = ScriptFormSet(request.POST, instance=job)
        if job_form.is_valid() and script_formset.is_valid():
            job = job_form.save(commit=False)
            if 'schedule_days' in job_form.cleaned_data:
                job.set_schedule_days(job_form.cleaned_data['schedule_days'])
            job.save()
           
            # Save formset and handle deletions
            scripts = script_formset.save(commit=False)
            for script in script_formset.deleted_objects:
                script.delete()
            for script in scripts:
                # Check if table_name has changed
                if script.id:
                    old_script = Script.objects.get(id=script.id)
                    if old_script.table_name != script.table_name:
                        # Delete associated Table and Column records
                        Table.objects.filter(script=script).delete()
                        Column.objects.filter(script=script).delete()
                        
                        # Create a new Table entry for the new table name
                        if script.table_name:
                            Table.objects.create(
                                script=script,
                                table_name=script.table_name,
                                last_import=timezone.now(),
                                row_count=0,
                                row_count_prev=0
                            )
                
                script.job = job
                script.save()
            script_formset.save_m2m()
           
#            update_job_schedule(job)
            update_job_schedule(job, app_name='reconciliation', execute_job_func=execute_job_core)
            messages.success(request, 'Job updated successfully.')
            return redirect('reconciliation:job_list')
    else:
        job_form = JobForm(instance=job)
        script_formset = ScriptFormSet(instance=job)
   
    context = {
        'job': job,
        'job_form': job_form,
        'script_formset': script_formset,
    }
    return render(request, 'pages/reconciliation/edit_job.html', context)


def job_list(request):
    jobs = Job.objects.all().order_by('name')
    return render(request, 'pages/reconciliation/job_list.html', {'jobs': jobs})


@csrf_exempt
@require_http_methods(["GET", "POST"])
def execute_job(request, job_id):
    job, success, output, error = execute_job_core(job_id)
    return JsonResponse({
        'success': success,
        'output': output,
        'error': error
    })


def scheduled_job_execution(job_id):
    execute_job_core(job_id)
    # No need for additional logging here, as it's done in execute_job_core


def schedule_job(job):
    add_job(job, scheduled_job_execution)


def delete_job(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    
    job.delete() 

    scheduler = get_scheduler()
    if scheduler:
        try:
            scheduler.remove_job(f'job_{job_id}')
            print(f"Job {job_id} removed from scheduler")
        except JobLookupError:
            print(f"Job {job_id} not found in scheduler")
        
        print("Current jobs in scheduler:")
        for job in scheduler.get_jobs():
            print(f"- {job.id}")
    else:
        print("Scheduler not initialized")
    
    messages.success(request, 'Job deleted successfully from database.')
    return redirect('reconciliation:job_list')


def table_list(request):
    if request.method == 'POST':
        for script in Script.objects.all():
            run_transform_key = f'run_transform_{script.id}'
            transform_script_key = f'transform_script_{script.id}'
            
            # Update run_transform (will be False if checkbox is unchecked)
            script.run_transform = run_transform_key in request.POST
            
            # Update transform_script regardless of run_transform status
            if transform_script_key in request.POST:
                script.transform_script = request.POST[transform_script_key]
            
            script.save()

            # Handle column updates
            for column in script.columns.all():
                comment_key = f'column_comment_{column.id}'
                related_column_key = f'column_related_{column.id}'

                if comment_key in request.POST:
                    column.comment = request.POST[comment_key]
                if related_column_key in request.POST:
                    column.override_column_name = request.POST[related_column_key]
                
                column.save()
        
        messages.success(request, 'Changes saved successfully.')
        return redirect('reconciliation:table_list')
    
    scripts = Script.objects.select_related('job').prefetch_related(
        'columns', 
        'tables'
    ).all()
    return render(request, 'pages/reconciliation/table_list.html', {'scripts': scripts})


def table_view(request, table_id):
    table = get_object_or_404(Table, id=table_id)
    script = table.script
    
    # Properly quote the table name
    db_table = connection.ops.quote_name(table.table_name)
    
    try:
        # Fetch the first 100 rows from the table
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT * FROM {db_table} LIMIT 100')
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        
        context = {
            'table': table,
            'script': script,
            'columns': columns,
            'rows': rows,
        }
        
        return render(request, 'pages/reconciliation/table_view.html', context)
    
    except ProgrammingError as e:
        messages.error(request, f"Error accessing table: {str(e)}")
        return redirect('reconciliation:table_list')


def save_table_list(request):
    if request.method == 'POST':
        for script in Script.objects.all():
            run_transform_key = f'run_transform_{script.id}'
            transform_script_key = f'transform_script_{script.id}'
            
            # Update run_transform (will be False if checkbox is unchecked)
            script.run_transform = run_transform_key in request.POST
            
            # Update transform_script regardless of run_transform status
            if transform_script_key in request.POST:
                script.transform_script = request.POST[transform_script_key]
            
            script.save()

            # Handle column updates
            for column in script.columns.all():
                comment_key = f'column_comment_{column.id}'
                related_column_key = f'column_related_{column.id}'

                if comment_key in request.POST:
                    column.comment = request.POST[comment_key]
                if related_column_key in request.POST:
                    column.override_column_name = request.POST[related_column_key]
                
                column.save()
    
    messages.success(request, 'Changes saved successfully.')
    return redirect('reconciliation:table_list')


#https://claude.ai/chat/950437a5-152f-444d-84ff-9bb5866455e8
def edit_table(request, table_id):
    table = get_object_or_404(Table, id=table_id)
    script = table.script
   
    ColumnFormSet = modelformset_factory(
        Column,
        form=CustomColumnForm,
        extra=0
    )
   
    if request.method == 'POST':
        logger.debug(f"POST edit table data: {request.POST}")
        form = CustomEditTableForm(request.POST)
        column_formset = ColumnFormSet(
            request.POST,
            queryset=Column.objects.filter(script=script, table_name=table.table_name)
        )
       
        if form.is_valid() and column_formset.is_valid():
            # Update Table
            table.transform_script = form.cleaned_data['transform_script']
            table.run_transform = form.cleaned_data['run_transform']
            table.override_column_names = form.cleaned_data['override_column_names']
            table.save()
           
            # Update Columns
            columns = column_formset.save(commit=False)
            for column in columns:
                existing_column = Column.objects.get(id=column.id)
                existing_column.override_data_type = column.override_data_type
                existing_column.override_column_name = column.override_column_name
                existing_column.primary_key = column.primary_key
                existing_column.foreign_key_reference = column.foreign_key_reference
                existing_column.save()
        else:
            logger.debug(f"Table form errors: {form.errors}")
            logger.debug(f"column_formset errors: {column_formset.errors}")
            return render(request, 'pages/reconciliation/edit_table.html', {'form': form, 'column_formset': column_formset, 'table': table, 'script': script})
       
        return redirect('reconciliation:table_list')  
       
    else:
        initial_data = {
            'transform_script': table.transform_script,
            'run_transform': table.run_transform,
            'override_column_names': table.override_column_names,
            'default_column_names': table.default_column_names,
        }
        form = CustomEditTableForm(initial=initial_data)
        column_formset = ColumnFormSet(
            queryset=Column.objects.filter(script=script, table_name=table.table_name)
        )
   
    context = {
        'script': script,
        'table': table,
        'form': form,
        'column_formset': column_formset,
    }
   
    return render(request, 'pages/reconciliation/edit_table.html', context)