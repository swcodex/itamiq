from django.db import models
from django.db.models import Q, Max

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
    order_exec = models.PositiveIntegerField()
    table_name = models.CharField(max_length=255, null=True, blank=True)  # New required field
    #column_names = models.CharField(max_length=4000, null=True, blank=True) #Should change this to textfield 
    import_enabled = models.BooleanField(default=False)  # New field
    #transform_script = models.TextField(blank=True, null=True)  # New field
    #run_transform = models.BooleanField(default=False)  # New field

    class Meta:
        ordering = ['order_exec']

    #https://claude.ai/chat/dd36b243-bb48-43e9-8bca-030c48a1d71a
    def get_table(self):
        return self.tables.filter(Q(table_name__isnull=False) & ~Q(table_name=''))\
                          .order_by('-last_import')\
                          .first()

    def __str__(self):
        return self.name
    
    @classmethod
    def reorder_scripts(cls, job_id):
        scripts = cls.objects.filter(job_id=job_id).order_by('order_exec', 'id')
        
        expected_order = 1
        changes_made = False
        
        for script in scripts:
            if script.order_exec != expected_order:
                script.order_exec = expected_order
                script.save(update_fields=['order_exec'])
                changes_made = True
            expected_order += 1
        
        return changes_made


class Table(models.Model):
    script = models.ForeignKey(Script, on_delete=models.CASCADE, related_name='tables')
    table_name = models.CharField(max_length=255, null=True, blank=True)
    last_import = models.DateTimeField(null=True, blank=True)
    row_count = models.IntegerField(default=0)
    row_count_prev = models.IntegerField(default=0)
    run_transform = models.BooleanField(default=False)
    transform_script = models.TextField(blank=True, null=True)


    def __str__(self):
        return f"{self.script.name} - {self.table_name}"

    class Meta:
        unique_together = ('script', 'table_name')


class Column(models.Model):
    OVERRIDE_DATA_TYPE_CHOICES = [
        ('DATE', 'Date'),
        ('TIMESTAMP', 'Datetime'),
        ('INTEGER', 'Integer'),
        ('TEXT', 'String'),
        ('NUMERIC', 'Decimal'),
        ('BIGINT', 'Big Integer'),
        ('BOOLEAN', 'Boolean'),
    ]
    script = models.ForeignKey(Script, on_delete=models.CASCADE, related_name='columns')
    table_name = models.CharField(max_length=255)
    column_name = models.CharField(max_length=255)
    detected_data_type = models.CharField(max_length=255, null=True, blank=True)
    override_data_type = models.CharField(
        max_length=10, 
        choices=OVERRIDE_DATA_TYPE_CHOICES, 
        default=None,
        blank=True,
        null=True
    )
    override_column_name = models.CharField(max_length=255, blank=True)
    primary_key = models.BooleanField(default=False)
    # Remove the old foreign_key field
    # foreign_key = models.BooleanField(default=False)
    # Add the new foreign_key_reference field
    foreign_key_reference = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        limit_choices_to={'is_unique': True},
        related_name='referencing_columns'
    )
    is_unique = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.script.name} - {self.table_name}.{self.column_name}"

    class Meta:
        unique_together = ('script', 'table_name', 'column_name')