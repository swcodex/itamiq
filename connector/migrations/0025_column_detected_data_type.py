# Generated by Django 5.0.7 on 2024-08-12 22:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('connector', '0024_rename_data_type_column_override_data_type_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='column',
            name='detected_data_type',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
