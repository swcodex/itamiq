# job_execution.py

import tempfile
import os
import subprocess
import logging
import time
from django.shortcuts import get_object_or_404
from django.utils import timezone
from datetime import timedelta
from .models import Job
from connector.models import Table, Column
import pandas as pd
import numpy as np
from django.conf import settings
from django.db.models import Q
from django.db import connections

logger = logging.getLogger(__name__)
 

def apply_foreign_key_constraints():
    try:
        logger.info("Applying foreign key constraints across all tables")

        # Get all columns with foreign key references
        fk_columns = Column.objects.filter(foreign_key_reference__isnull=False)

        if not fk_columns:
            logger.info("No foreign key constraints to apply")
            return True, None

        with connections['itam'].cursor() as cursor:
            for fk_column in fk_columns:
                referenced_column = fk_column.foreign_key_reference
                constraint_name = f"fk_{fk_column.table_name}_{fk_column.column_name}"

                # Check if the constraint already exists
                cursor.execute("""
                    SELECT CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = %s AND CONSTRAINT_NAME = %s
                """, [fk_column.table_name, constraint_name])

                if cursor.fetchone():
                    # If the constraint exists, drop it first
                    cursor.execute(f"""
                        ALTER TABLE `{fk_column.table_name}`
                        DROP FOREIGN KEY {constraint_name}
                    """)

                # Apply the foreign key constraint
                cursor.execute(f"""
                    ALTER TABLE `{fk_column.table_name}`
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY (`{fk_column.column_name}`)
                    REFERENCES `{referenced_column.table_name}` (`{referenced_column.column_name}`)
                """)

                logger.info(f"Applied foreign key constraint for {fk_column.table_name}.{fk_column.column_name} "
                            f"referencing {referenced_column.table_name}.{referenced_column.column_name}")

        logger.info("Successfully applied all foreign key constraints")
        return True, None
    except Exception as e:
        logger.error(f"Error applying foreign key constraints: {str(e)}", exc_info=True)
        return False, f"Error applying foreign key constraints: {str(e)}"
 

def execute_job_core(job_id):
    job = get_object_or_404(Job, id=job_id)
    
    start_time = time.time()
    
    success = True
    output = ""
    error = None
    sql_import_scripts = []

    # New step: Apply foreign key constraints

    fk_success, fk_error = apply_foreign_key_constraints()
    if not fk_success:
        error = f"Error applying foreign key constraints: {fk_error}"
        success = False
    else:
        output += "Successfully applied all foreign key constraints.\n"


    # Iterate over each script in the parent job
    for script in job.scripts.all().order_by('order'):
        # Execute the job script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as temp_file:
            temp_file.write(script.content)
            temp_file_path = temp_file.name

        try:
            result = subprocess.run(["python", "-X", "utf8", temp_file_path], check=True, capture_output=True, text=True)
            script_output = result.stdout
            script_error = None
            script_success = True
        except subprocess.CalledProcessError as e:
            script_output = e.stdout
            script_error = e.stderr
            script_success = False
        finally:
            os.unlink(temp_file_path)

        output += f"Script {script.name} output:\n{script_output}\n"
        if script_error:
            error = f"Script {script.name} error:\n{script_error}"
            success = False






    end_time = time.time()
    duration = timedelta(seconds=end_time - start_time)

    job.last_execution_time = timezone.now()
    job.last_execution_success = success
    job.last_execution_error = error
    job.last_execution_duration = duration
    job.save()

    logger.info(f"Executed job {job.id}: {job.name}")
    logger.info(f"Output: {output}")
    if error:
        logger.error(f"Error: {error}")

    return job, success, output, error



def scheduled_job_execution(job_id):
    execute_job_core(job_id)