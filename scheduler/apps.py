from django.apps import AppConfig
from django.db.models.signals import post_migrate
import logging

logger = logging.getLogger(__name__)

class SchedulerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'scheduler'

    def ready(self):
        logger.info("SchedulerConfig.ready() called")
        from .scheduler import initialize_scheduler, ensure_scheduler_started
        initialize_scheduler()
        post_migrate.connect(self.start_scheduler, sender=self)

    def start_scheduler(self, sender, **kwargs):
        from .scheduler import ensure_scheduler_started
        ensure_scheduler_started()