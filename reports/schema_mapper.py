from django.db import connections, transaction
from .models import Table, Column, Relationship
import logging

logger = logging.getLogger(__name__)

def map_database_schema():
    try:
        with transaction.atomic():
            with connections['itam'].cursor() as cursor:
                # Clear existing data
                Table.objects.all().delete()
                Column.objects.all().delete()
                Relationship.objects.all().delete()

                # Get all tables
                cursor.execute("""
                    SELECT TABLE_NAME, TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                """)
                tables = cursor.fetchall()

                table_objects = []
                column_objects = []
                relationship_objects = []

                for table_name, schema in tables:
                    table = Table(name=table_name, schema=schema)
                    table_objects.append(table)

                Table.objects.bulk_create(table_objects)
                table_dict = {table.name: table for table in Table.objects.all()}

                for table_name, schema in tables:
                    # Get columns for each table
                    cursor.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                    """, [table_name])
                    columns = cursor.fetchall()

                    for column_name, data_type in columns:
                        column = Column(name=column_name, table=table_dict[table_name], data_type=data_type)
                        column_objects.append(column)

                Column.objects.bulk_create(column_objects)

                # Get relationships
                cursor.execute("""
                    SELECT 
                        TABLE_NAME,
                        COLUMN_NAME,
                        REFERENCED_TABLE_NAME,
                        REFERENCED_COLUMN_NAME
                    FROM
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE
                        TABLE_SCHEMA = DATABASE()
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                """)
                relationships = cursor.fetchall()

                column_dict = {(col.table.name, col.name): col for col in Column.objects.all()}

                for table_name, column_name, ref_table_name, ref_column_name in relationships:
                    from_table = table_dict[table_name]
                    to_table = table_dict[ref_table_name]
                    from_column = column_dict[(table_name, column_name)]
                    to_column = column_dict[(ref_table_name, ref_column_name)]

                    relationship = Relationship(
                        from_table=from_table,
                        from_column=from_column,
                        to_table=to_table,
                        to_column=to_column
                    )
                    relationship_objects.append(relationship)

                Relationship.objects.bulk_create(relationship_objects)

        logger.info("Database schema mapping completed successfully.")
    except Exception as e:
        logger.error(f"Error mapping database schema: {str(e)}")
        raise