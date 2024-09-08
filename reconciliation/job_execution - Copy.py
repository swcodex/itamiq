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
import pandas as pd
import numpy as np
import psycopg2
from django.conf import settings
from django.db.models import Q
from django.db import connection

logger = logging.getLogger(__name__)


def find_latest_data_file():
    base_dir = settings.BASE_DIR
    now = time.time()
    one_minute_ago = now - 10
    latest_file = None
    latest_time = 0

    for filename in os.listdir(base_dir):
        if filename.lower().endswith(('.xlsx', '.csv', '.json')):
            file_path = os.path.join(base_dir, filename)
            creation_time = os.path.getmtime(file_path)
            if one_minute_ago <= creation_time <= now:
                if creation_time > latest_time:
                    latest_file = file_path
                    latest_time = creation_time

    return latest_file


# First, get the associated Table object using a more robust method
def get_table(script):
    return script.tables.filter(Q(table_name__isnull=False) & ~Q(table_name=''))\
                        .order_by('-last_import')\
                        .first()


def get_column_names(script):
    file_path = find_latest_data_file()
    if not file_path:
        raise ValueError("No suitable data file found")

    if file_path.lower().endswith('.csv'):
        with open(file_path, 'r') as f:
            header = f.readline().strip().split(',')
    elif file_path.lower().endswith('.xlsx'):
        df = pd.read_excel(file_path, nrows=0)
        header = df.columns.tolist()
    elif file_path.lower().endswith('.json'):
        df = pd.read_json(file_path, nrows=0)
        header = df.columns.tolist()
    else:
        raise ValueError("Invalid file type")

    return header


def get_override_column_names(script, original_column_names):
    # Fetch all columns for this script
    columns = Column.objects.filter(script=script)
    
    # Create a dictionary mapping original column names to override names
    override_dict = {col.column_name: col.override_column_name for col in columns if col.override_column_name}
    
    # Apply overrides where they exist, otherwise keep the original name
    final_column_names = [override_dict.get(col, col) for col in original_column_names]
    
    return final_column_names


def set_table_primary_key(script, job, original_column_names):
    if not script.table_name or not script.table_name.strip():
        logger.warning(f"Skipping primary key setting for script {script.name}: table_name is empty or None")
        return True, None

    try:
        logger.info(f"Setting primary key for table {script.table_name}")

        # Get original column names
        # original_column_names = get_column_names(script)
        
        # Get final column names (with overrides applied)
        final_column_names = get_override_column_names(script, original_column_names)
        
        # Create a mapping between original and final column names
        column_mapping = dict(zip(original_column_names, final_column_names))

        # Get all columns marked as primary key for this script and table
        primary_key_columns = Column.objects.filter(
            script=script,
            table_name=script.table_name,
            primary_key=True
        ).values_list('column_name', flat=True)

        if not primary_key_columns:
            logger.warning(f"No primary key columns found for table {script.table_name}")
            return True, None

        # Map the primary key columns to their final names (with overrides)
        final_pk_columns = [column_mapping.get(col, col) for col in primary_key_columns]

        # Convert final column names to a comma-separated string for the SQL query
        pk_columns_str = ', '.join(f'"{col}"' for col in final_pk_columns)

        # Connect to the database using Django's connection
        with connection.cursor() as cursor:
            # Check if the table exists
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, [script.table_name])
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                logger.error(f"Table {script.table_name} does not exist")
                return False, f"Table {script.table_name} does not exist"

            # Remove existing primary key constraint if it exists
            cursor.execute(f"""
                DO $$ 
                BEGIN 
                    IF EXISTS (
                        SELECT 1 FROM information_schema.table_constraints 
                        WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        EXECUTE (
                            SELECT 'ALTER TABLE "' || %s || '" DROP CONSTRAINT "' || constraint_name || '"'
                            FROM information_schema.table_constraints
                            WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
                        );
                    END IF;
                END $$;
            """, [script.table_name, script.table_name, script.table_name])

            # Add new primary key constraint using the final (possibly overridden) column names
            cursor.execute(f"""
                ALTER TABLE "{script.table_name}" 
                ADD PRIMARY KEY ({pk_columns_str});
            """)

        logger.info(f"Successfully set primary key for table {script.table_name}: {pk_columns_str}")
        return True, None
    except Exception as e:
        logger.error(f"Error setting primary key for job {job.id}, script {script.name}: {str(e)}", exc_info=True)
        return False, f"Error setting primary key: {str(e)}"


def execute_transform_script(script, job):
    try:
        table = get_table(script)    

        if not table.run_transform or not table.transform_script:
            return True, None, None

        logger.info(f"Executing transform script for {script.name}")
        
        # Connect to the database using Django's connection
        with connection.cursor() as cursor:
            # Execute the transform script
            cursor.execute(table.transform_script)
        
        logger.info(f"Successfully executed transform script for {script.name}")
        return True, f"Transform script for {script.name} executed successfully", None
    except Exception as e:
        logger.error(f"Error during transform script execution for job {job.id}, script {script.name}: {str(e)}", exc_info=True)
        return False, None, f"Error during transform script execution: {str(e)}"


def update_table_metadata(script, job):
    if not script.table_name or not script.table_name.strip():
        logger.warning(f"Skipping table metadata update for script {script.name}: table_name is empty or None")
        return True, None
    try:
        logger.info(f"Updating table metadata for script {script.name}")
        
        # Get or create the Table object
        table, created = Table.objects.get_or_create(
            script=script,
            table_name=script.table_name
        )
        
        # 1. Update last_import datetime
        table.last_import = timezone.now()
        
        # 2. Update row_count_prev if row_count exists
        if table.row_count:
            table.row_count_prev = table.row_count
        
        # 3. Get current row count from the imported table
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{script.table_name}"')
            current_row_count = cursor.fetchone()[0]
        
        # Update row_count with the current count
        table.row_count = current_row_count
        
        # Save the changes
        table.save()
        
        logger.info(f"Table metadata updated successfully for {script.name}")
        return True, None
    except Exception as e:
        logger.error(f"Error updating table metadata for job {job.id}, script {script.name}: {str(e)}", exc_info=True)
        return False, f"Error updating table metadata: {str(e)}"


def update_column_metadata(script, job, original_column_names, file_path):
    if not script.table_name or not script.table_name.strip():
        logger.warning(f"Skipping column metadata update for script {script.name}: table_name is empty or None")
        return True, None
    
    try:
        logger.info(f"Updating column metadata for script {script.name}")
        
        # Read the CSV file
        df = pd.read_csv(file_path, nrows=None, dtype=str, encoding='utf-8-sig', header=0)

        # Dictionary to store results of uniqueness check
        unique_columns = {}

        # Iterate through each column and check for uniqueness
        for column in df.columns:
            unique_columns[column] = df[column].nunique() == len(df)

        current_column_names = get_override_column_names(script, original_column_names)
        logger.info(f"Current column names (with overrides): {current_column_names}")

        # Get existing Column objects for this script and table
        existing_columns = Column.objects.filter(script=script, table_name=script.table_name)
        logger.info(f"Existing columns in database: {[col.column_name for col in existing_columns]}")
        
        # Create a dictionary of existing columns, keyed by column_name
        existing_column_dict = {col.column_name: col for col in existing_columns}
        logger.info(f"Existing column dict keys: {list(existing_column_dict.keys())}")

        # Identify columns to be added, updated, or removed
        columns_to_add = []
        columns_to_update = []
        columns_to_remove = set(existing_column_dict.keys()) - set(original_column_names)

        for orig_name, curr_name in zip(original_column_names, current_column_names):
            if orig_name not in existing_column_dict:
                columns_to_add.append((orig_name, curr_name))
            elif curr_name != orig_name:
                columns_to_update.append((orig_name, curr_name))

        #update the function 

        logger.info(f"Columns to add: {columns_to_add}")
        logger.info(f"Columns to update: {columns_to_update}")
        logger.info(f"Columns to remove: {columns_to_remove}")

        # Get the actual data types from the database
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
            """, [script.table_name])
            db_column_types = dict(cursor.fetchall())

        # Create new Column objects for new columns
        for orig_name, curr_name in columns_to_add:
            Column.objects.create(
                script=script,
                table_name=script.table_name,
                column_name=orig_name,
                override_column_name=curr_name if curr_name != orig_name else '',
                is_unique=unique_columns.get(orig_name, False),  # Add uniqueness information,
                detected_data_type=db_column_types.get(curr_name, 'UNKNOWN')
            )
            logger.info(f"Created new Column object for {script.table_name}.{orig_name}")

        # Update existing Column objects
        for orig_name, curr_name in columns_to_update:
            column = existing_column_dict[orig_name]
            column.override_column_name = curr_name
            column.is_unique = unique_columns.get(orig_name, False)  # Update uniqueness information
            column.detected_data_type = db_column_types.get(curr_name, 'UNKNOWN')
            column.save()
            logger.info(f"Updated column: {column.column_name} (override: {column.override_column_name}, (detected: {column.detected_data_type}, is_unique: {column.is_unique})")

        # Remove Column objects for columns that no longer exist
        for column_name in columns_to_remove:
            col_to_remove = existing_column_dict[column_name]
            logger.info(f"Removing column: {col_to_remove.column_name} (override: {col_to_remove.override_column_name})")
            col_to_remove.delete()

        # Final check
        final_columns = Column.objects.filter(script=script, table_name=script.table_name)
        logger.info(f"Final columns in database: {[col.column_name for col in final_columns]}")

        logger.info(f"Column metadata updated successfully for {script.name}")
        return True, None
    except Exception as e:
        logger.error(f"Error updating column metadata for job {job.id}, script {script.name}: {str(e)}", exc_info=True)
        return False, f"Error updating column metadata: {str(e)}"


def execute_sql_import(script, job):
    if not script.table_name or not script.table_name.strip():
        logger.warning(f"Skipping SQL import for script {script.name}: table_name is empty or None")
        return True, "SQL import skipped: no table name provided", None

    try:
        # Find the latest data file
        file_path = find_latest_data_file()
        if not file_path:
            raise ValueError("No suitable data file found")

        logger.info(f"File path: {file_path}")

        # Read the first 1000 rows to infer data types
        if file_path.lower().endswith('.csv'):
            df_sample = pd.read_csv(file_path, dtype=str, nrows=10000, encoding='utf-8-sig', header=0)
            logger.info("Reading CSV file (first 1000 rows for type inference)")
        elif file_path.lower().endswith('.xlsx'):
            df_sample = pd.read_excel(file_path, nrows=10000, dtype=str)
            logger.info("Reading Excel file (first 1000 rows for type inference)")
        elif file_path.lower().endswith('.json'):
            df_sample = pd.read_json(file_path, nrows=10000, dtype=str, encoding='utf-8-sig', header=0)
            logger.info("Reading JSON file (first 1000 rows for type inference)")
        else:
            raise ValueError("Invalid file type")

        # Get original column names
        original_column_names = get_column_names(script)
        
        # Get final column names (with overrides applied)
        final_column_names = get_override_column_names(script, original_column_names)

        # Create a mapping between original and final column names
        column_mapping = dict(zip(original_column_names, final_column_names))

        # Rename columns in the sample DataFrame
        df_sample.columns = original_column_names

        # Function to determine if a column should be an integer
        def is_integer_column(series):
            try:
                series = pd.to_numeric(series, errors='coerce')
                non_null = series.dropna()
                return (non_null.astype(float).mod(1) == 0).all() and non_null.min() >= -2147483648 and non_null.max() <= 2147483647
            except:
                return False

        # Function to determine if a column should be a float
        def is_float_column(series):
            try:
                series = pd.to_numeric(series, errors='coerce')
                return not series.isnull().all()
            except:
                return False

        # Function to determine appropriate integer type
        def determine_integer_type(series):
            series = pd.to_numeric(series, errors='coerce')
            non_null = series.dropna()
            min_val = non_null.min()
            max_val = non_null.max()
            if min_val >= -2147483648 and max_val <= 2147483647:
                return 'INTEGER'
            elif min_val >= -9223372036854775808 and max_val <= 9223372036854775807:
                return 'BIGINT'
            else:
                return 'NUMERIC'  # For values outside BIGINT range

        # Function to determine if a datetime column contains only dates
        def is_date_only(col):
            try:
                return pd.to_datetime(df_sample[col], errors='coerce').dt.time.eq(pd.Timestamp('00:00:00')).all()
            except:
                return False

        # Function to check if a column contains mixed data types
        def contains_mixed_data(series):
            numeric_count = pd.to_numeric(series, errors='coerce').notna().sum()
            non_numeric_count = series.str.contains('[a-zA-Z]', na=False).sum()
            return numeric_count > 0 and non_numeric_count > 0

        # Infer column types using the sample
        inferred_types = {}
        for col in df_sample.columns:
            if df_sample[col].isnull().all():
                inferred_types[col] = 'TEXT'  # All null columns default to TEXT
            elif contains_mixed_data(df_sample[col]):
                inferred_types[col] = 'TEXT'  # Mixed data types default to TEXT
            elif is_integer_column(df_sample[col]):
                inferred_types[col] = determine_integer_type(df_sample[col])
            elif is_float_column(df_sample[col]):
                inferred_types[col] = 'NUMERIC'
            elif pd.to_datetime(df_sample[col], errors='coerce').notna().any():
                if is_date_only(col):
                    inferred_types[col] = 'DATE'
                else:
                    inferred_types[col] = 'TIMESTAMP'
            else:
                inferred_types[col] = 'TEXT'

        logger.info(f"Inferred types: {inferred_types}")

        # Now read the entire file using the inferred dtypes
        dtype_dict = {col: 'string' for col in original_column_names}  # Start with all columns as string
        parse_dates = [col for col, dtype in inferred_types.items() if dtype in ['DATE', 'TIMESTAMP']]
        
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path, dtype=dtype_dict, parse_dates=parse_dates, keep_default_na=False, na_values=[''], encoding='utf-8-sig')
        elif file_path.lower().endswith('.xlsx'):
            df = pd.read_excel(file_path, dtype=dtype_dict, parse_dates=parse_dates, keep_default_na=False, na_values=[''])
        elif file_path.lower().endswith('.json'):
            df = pd.read_json(file_path, dtype=dtype_dict, parse_dates=parse_dates, encoding='utf-8-sig')

        # Rename columns in the full DataFrame using the mapping
        df.rename(columns=column_mapping, inplace=True)

        # Convert columns to appropriate types after reading
        for orig_col, final_col in column_mapping.items():
            dtype = inferred_types[orig_col]
            if dtype in ['INTEGER', 'BIGINT']:
                df[final_col] = pd.to_numeric(df[final_col], errors='coerce').astype('Int64')
            elif dtype == 'NUMERIC':
                df[final_col] = pd.to_numeric(df[final_col], errors='coerce')
            elif dtype in ['DATE', 'TIMESTAMP']:
                df[final_col] = pd.to_datetime(df[final_col], errors='coerce')

        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")

        # Connect to the database using Django's connection
        with connection.cursor() as cursor:
            # Set the client encoding to UTF-8
            cursor.execute("SET client_encoding TO 'UTF8';")
            
            # Check if the table exists and delete it if it does
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, [script.table_name])
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                logger.info(f"Table '{script.table_name}' already exists. Deleting it.")
                cursor.execute(f'DROP TABLE IF EXISTS "{script.table_name}"')

            # Create the table with inferred types and final column names
            columns = [f'"{col}" {inferred_types[orig_col]}' for orig_col, col in column_mapping.items()]
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{script.table_name}" ({", ".join(columns)})'
            logger.info(f"Creating table with SQL: {create_table_sql}")
            cursor.execute(create_table_sql)

            # Insert data
            df = df.replace({pd.NaT: None, '<NA>': None})  # Replace NaT and '<NA>' with None
            df = df.where(pd.notnull(df), None)  # Replace all other null values with None
            if len(df.columns) == 1 and df.dtypes.iloc[0] in ['int64', 'Int64']:
                data = [(int(x),) for x in df.iloc[:, 0]]
            else:
                data = [tuple(x) for x in df.to_numpy()]
            placeholders = ','.join(['%s' for _ in df.columns])
            insert_sql = f'INSERT INTO "{script.table_name}" VALUES ({placeholders})'
            logger.info(f"Inserting data with SQL: {insert_sql}")
            logger.info(f"Number of rows to insert: {len(data)}")
            cursor.executemany(insert_sql, data)

        # Verify the data was inserted
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{script.table_name}"')
            row_count = cursor.fetchone()[0]
        logger.info(f"Rows in table after insert: {row_count}")

        logger.info(f"Successfully imported {len(df)} rows into {script.table_name}")
        return True, f"Successfully imported {len(df)} rows into {script.table_name}", None
    except Exception as e:
        logger.error(f"Error during import for job {job.id}: {str(e)}", exc_info=True)
        return False, None, f"Error during import: {str(e)}"
 

def execute_job_core(job_id):
    job = get_object_or_404(Job, id=job_id)
    
    start_time = time.time()
    
    success = True
    output = ""
    error = None
    sql_import_scripts = []

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

        # If the script executed successfully, proceed with the additional tasks
        if script_success:
            try:
                # Lines 300-389: Execute this block after each job script
                file_path= find_latest_data_file()

                column_names= get_column_names(script)                
                # Get or create the Table object
                table, created = Table.objects.get_or_create(
                    script=script,
                    table_name=script.table_name
                )

                # Write column names to Table.default_column_names
                table.default_column_names = column_names
                table.save()

                # If table_name is populated, add to sql_import_scripts for later execution
                if script.table_name and script.table_name.strip():
                    sql_import_scripts.append(script)

                # Execute SQL import
                sql_success, script_output, script_error = execute_sql_import(script, job)
                output += f"SQL Import {script.name} output:\n{script_output}\n"
                if not sql_success:
                    error = f"SQL Import {script.name} error:\n{script_error}"
                    success = False
                    break

                # Execute transform script
                transform_success, transform_output, transform_error = execute_transform_script(script, job)
                if transform_output:
                    output += f"Transform Script {script.name} output:\n{transform_output}\n"
                if not transform_success:
                    error = f"Transform Script {script.name} error:\n{transform_error}"
                    success = False
                    break

                # Update table metadata
                metadata_success, metadata_error = update_table_metadata(script, job)
                if not metadata_success:
                    error = f"Error updating table metadata for {script.name}: {metadata_error}"
                    success = False
                    break
                
                # Update column metadata
                column_metadata_success, column_metadata_error = update_column_metadata(script, job, column_names, file_path)
                if not column_metadata_success:
                    error = f"Error updating column metadata for {script.name}: {column_metadata_error}"
                    success = False
                    break

                # New step: Set primary key
                pk_success, pk_error = set_table_primary_key(script, job, column_names)
                if not pk_success:
                    error = f"Error setting primary key for {script.name}: {pk_error}"
                    success = False
                    break


            except Exception as e:
                logger.error(f"Error in post-script execution steps for script {script.name}: {str(e)}", exc_info=True)
                error = f"Error in post-script execution steps for script {script.name}: {str(e)}"
                success = False
                break
        else:
            success = False
            break

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