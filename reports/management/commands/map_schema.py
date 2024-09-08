from django.core.management.base import BaseCommand
from reports.schema_mapper import map_database_schema

class Command(BaseCommand):
    help = 'Maps the database schema for the report builder'

    def handle(self, *args, **options):
        self.stdout.write("Starting database schema mapping...")
        map_database_schema()
        self.stdout.write(self.style.SUCCESS("Database schema mapping completed successfully."))
