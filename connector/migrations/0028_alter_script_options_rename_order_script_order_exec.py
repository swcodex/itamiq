# Generated by Django 5.0.7 on 2024-09-07 17:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('connector', '0027_remove_table_default_column_names_and_more'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='script',
            options={'ordering': ['order_exec']},
        ),
        migrations.RenameField(
            model_name='script',
            old_name='order',
            new_name='order_exec',
        ),
    ]
