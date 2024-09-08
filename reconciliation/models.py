from django.db import models
from django.db.models import Q

class Job(models.Model):
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True, null=True)
    schedule_time = models.TimeField(null=True, blank=True)
    schedule_days = models.CharField(max_length=49, blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_execution_time = models.DateTimeField(null=True, blank=True)
    last_execution_success = models.BooleanField(null=True)
    last_execution_error = models.TextField(null=True, blank=True)
    last_execution_duration = models.DurationField(null=True, blank=True)

    def get_schedule_days(self):
        return self.schedule_days.split(',') if self.schedule_days else []

    def set_schedule_days(self, days):
        self.schedule_days = ','.join(days) if days else ''

    def __str__(self):
        return self.name

class Script(models.Model):
    job = models.ForeignKey(Job, related_name='scripts', on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    content = models.TextField()
    order = models.PositiveIntegerField()
    table_name = models.CharField(max_length=255, null=True, blank=True)  # New required field
    import_enabled = models.BooleanField(default=False)  # New field

    class Meta:
        ordering = ['order']

    #https://claude.ai/chat/dd36b243-bb48-43e9-8bca-030c48a1d71a
    def get_table(self):
        return self.tables.filter(Q(table_name__isnull=False) & ~Q(table_name=''))\
                          .order_by('-last_import')\
                          .first()

    def __str__(self):
        return self.name