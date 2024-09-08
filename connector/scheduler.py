from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from django_apscheduler.jobstores import DjangoJobStore
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.apps import apps
from django.db import connection
from .job_execution import scheduled_job_execution
import logging


logger = logging.getLogger(__name__)
scheduler = None


def get_scheduler():
    global scheduler
    if scheduler is None:
        initialize()
    return scheduler


def initialize():
    global scheduler
    logger.info("Entering initialize function")
    if scheduler is None:
        logger.info("Scheduler is None. Creating new scheduler...")
        try:
            scheduler = BackgroundScheduler()
            scheduler.add_jobstore(DjangoJobStore(), "default")
            scheduler.start()
            logger.info("Scheduler initialized and started successfully.")
            
            # Register the signal handler here
            Script = apps.get_model('reconciliation', 'Script')
            #post_save.connect(script_saved, sender=Script)
            logger.info("Signal handler registered for Script model.")
        except Exception as e:
            logger.error(f"Error initializing scheduler: {str(e)}")
    else:
        logger.info("Scheduler already initialized.")


def add_job(job, execute_job_func):
    if scheduler is None:
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
        
        logger.info(f"Scheduling job {job.id}: {job.name} for {converted_days} at {job.schedule_time}")
        scheduler.add_job(
            execute_job_func,
            'cron',
            day_of_week=','.join(converted_days),
            hour=job.schedule_time.hour,
            minute=job.schedule_time.minute,
            args=[job.id],  # Pass only the job.id
            id=f'job_{job.id}'
        )


def update_job_schedule(job):
    scheduler = get_scheduler()
    if not scheduler:
        print("Scheduler not initialized")
        return

    job_id = f'job_{job.id}'
    
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
                scheduled_job_execution,
                'cron',
                id=job_id,
                day_of_week=cron_days,
                hour=time.hour,
                minute=time.minute,
                args=[job.id],
                replace_existing=True
            )
            
            print(f"Job {job.name} (ID: {job_id}) scheduled for {cron_days} at {time}")
        else:
            print(f"No valid schedule days provided for job {job.name} (ID: {job_id})")
    else:
        print(f"Job {job.name} (ID: {job_id}) has no schedule")

    # Print all jobs in the scheduler for debugging
    print("Current jobs in scheduler after update:")
    for job in scheduler.get_jobs():
        print(f"- {job.id}")


def remove_job(script_id):
    if scheduler:
        job_id = f'script_{script_id}'
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            logger.info(f"Removed job {job_id} from scheduler")
        else:
            logger.warning(f"Job {job_id} not found in scheduler")