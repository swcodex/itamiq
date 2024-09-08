# Generated by Django 5.0.7 on 2024-08-15 17:24

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
                ('description', models.TextField(blank=True, null=True)),
                ('schedule_time', models.TimeField(blank=True, null=True)),
                ('schedule_days', models.CharField(blank=True, default='', max_length=49)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('last_execution_time', models.DateTimeField(blank=True, null=True)),
                ('last_execution_success', models.BooleanField(null=True)),
                ('last_execution_error', models.TextField(blank=True, null=True)),
                ('last_execution_duration', models.DurationField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Script',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('content', models.TextField()),
                ('order', models.PositiveIntegerField()),
                ('table_name', models.CharField(blank=True, max_length=255, null=True)),
                ('import_enabled', models.BooleanField(default=False)),
                ('job', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='scripts', to='reconciliation.job')),
            ],
            options={
                'ordering': ['order'],
            },
        ),
    ]
