# Generated by Django 5.0.7 on 2024-09-04 16:47

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('connector', '0026_remove_column_foreign_key_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='table',
            name='default_column_names',
        ),
        migrations.RemoveField(
            model_name='table',
            name='override_column_names',
        ),
    ]
