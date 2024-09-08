from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from django_apscheduler.jobstores import DjangoJobStore
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.conf import settings  # Add this import
from django.apps import apps
from django.db import connection
import logging


logger = logging.getLogger(__name__)
scheduler = None

def get_scheduler():
    global scheduler
    if scheduler is None:
        initialize_scheduler()
    return scheduler

def initialize_scheduler():
    global scheduler
    if scheduler is None:
        try:
            scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE)
            scheduler.add_jobstore(DjangoJobStore(), "default")
            logger.info("Scheduler initialized successfully.")
            ensure_scheduler_started()
        except Exception as e:
            logger.error(f"Error initializing scheduler: {str(e)}")

def ensure_scheduler_started():
    global scheduler
    if scheduler and not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started.")
    elif scheduler and scheduler.running:
        logger.info("Scheduler is already running.")
    else:
        logger.error("Scheduler not initialized. Cannot start.")


def add_job(job, execute_job_func, app_name):
    scheduler = get_scheduler()
    if not scheduler:
        logger.error("Scheduler not initialized")
        return

    day_name_mapping = {
        'MON': 'mon', 'TUE': 'tue', 'WED': 'wed', 
        'THU': 'thu', 'FRI': 'fri', 'SAT': 'sat', 'SUN': 'sun'
    }

    if job.schedule_time and job.schedule_days:
        days_of_week = job.get_schedule_days()
        logger.info(f"Raw days of week: {days_of_week}")
        
        converted_days = [day_name_mapping.get(day, day) for day in days_of_week]
        logger.info(f"Converted days of week: {converted_days}")
        
        logger.info(f"Scheduling job {job.id} for app {app_name}: {job.name} for {converted_days} at {job.schedule_time}")
        scheduler.add_job(
            execute_job_func,
            'cron',
            day_of_week=','.join(converted_days),
            hour=job.schedule_time.hour,
            minute=job.schedule_time.minute,
            args=[job.id, app_name],  # Pass both job.id and app_name
            id=f'{app_name}_job_{job.id}'
        )


def update_job_schedule(job, app_name, execute_job_func):
    scheduler = get_scheduler()
    if not scheduler:
        logger.error("Scheduler not initialized")
        return

    job_id = f'{app_name}_job_{job.id}'
    
    # Remove existing job if it's scheduled
    try:
        scheduler.remove_job(job_id)
        print(f"Removed existing job {job_id} from scheduler")
    except JobLookupError:
        print(f"Job {job_id} not found in scheduler for removal")
    
    # If job has a schedule, add it to the scheduler
    if job.schedule_time and job.schedule_days:
        days = job.get_schedule_days()
        time = job.schedule_time
        
        # Convert days to cron-style day of week (0-6 where 0 is Monday)
        day_mapping = {'MON': 0, 'TUE': 1, 'WED': 2, 'THU': 3, 'FRI': 4, 'SAT': 5, 'SUN': 6}
        cron_days = ','.join(str(day_mapping[day]) for day in days if day in day_mapping)
        
        if cron_days:
            scheduler.add_job(
                execute_job_func,  # Use the app-specific execution function
                'cron',
                id=job_id,
                day_of_week=cron_days,
                hour=time.hour,
                minute=time.minute,
                args=[job.id],  # We only pass job.id as execute_job_func is app-specific
                replace_existing=True
            )
            
            print(f"Job {job.name} (ID: {job_id}) for app {app_name} scheduled for {cron_days} at {time}")
        else:
            print(f"No valid schedule days provided for job {job.name} (ID: {job_id}) for app {app_name}")
    else:
        print(f"Job {job.name} (ID: {job_id}) for app {app_name} has no schedule")

    # Print all jobs in the scheduler for debugging
    print("Current jobs in scheduler after update:")
    for job in scheduler.get_jobs():
        print(f"- {job.id}")

def remove_job(script_id, app_name):
    if scheduler:
        job_id = f'{app_name}_job_{script_id}'
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            logger.info(f"Removed job {job_id} from scheduler")
        else:
            logger.warning(f"Job {job_id} not found in scheduler")