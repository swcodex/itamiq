# job_execution.py

import tempfile
import os
import subprocess
import logging
import time
from django.shortcuts import get_object_or_404
from django.utils import timezone
from datetime import timedelta
from .models import Job, Table, Column
import pandas as pd
import numpy as np
import re
import psycopg2
from dateutil.parser import parse, ParserError
from django.conf import settings
from django.db.models import Q
from django.db import connections
import csv

logger = logging.getLogger(__name__)


def find_latest_data_file():
    base_dir = settings.BASE_DIR
    now = time.time()
    one_minute_ago = now - 120
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
        encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1']
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', newline='', encoding=encoding) as f:
                    reader = csv.reader(f)
                    header = next(reader)
                return header
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Unable to read CSV file header with any of the attempted encodings: {encodings_to_try}")
    
    elif file_path.lower().endswith('.xlsx'):
        try:
            df = pd.read_excel(file_path, nrows=0)
            return df.columns.tolist()
        except Exception as e:
            raise ValueError(f"Error reading Excel file header: {str(e)}")
    
    elif file_path.lower().endswith('.json'):
        try:
            df = pd.read_json(file_path, nrows=0)
            return df.columns.tolist()
        except Exception as e:
            raise ValueError(f"Error reading JSON file structure: {str(e)}")
    
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def get_override_column_names(script, original_column_names):
    # Fetch all columns for this script
    columns = Column.objects.filter(script=script)
    
    # Create a dictionary mapping original column names to override names
    override_dict = {col.column_name: col.override_column_name for col in columns if col.override_column_name}
    
    # Apply overrides where they exist, otherwise keep the original name
    final_column_names = [override_dict.get(col, col) for col in original_column_names]
    
    return final_column_names


def set_table_primary_key(script, job, original_column_names):
    if not script.table_name or script.import_enabled == 0 or not script.table_name.strip():
        logger.warning(f"Skipping SQL import for script {script.name}: table_name is empty or None")
        return True, "SQL import skipped: no table name provided", None

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
        pk_columns_str = ', '.join(f'`{col}`' for col in final_pk_columns)

        # Connect to the database using Django's connection
        with connections['itam'].cursor() as cursor:
            # Check if the table exists
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_name = %s
            """, [script.table_name])
            table_exists = cursor.fetchone()[0] > 0

            if not table_exists:
                logger.error(f"Table {script.table_name} does not exist")
                return False, f"Table {script.table_name} does not exist"

            # Remove existing primary key constraint if it exists
            cursor.execute(f"""
                SELECT CONSTRAINT_NAME
                FROM information_schema.TABLE_CONSTRAINTS
                WHERE TABLE_NAME = %s AND CONSTRAINT_TYPE = 'PRIMARY KEY'
            """, [script.table_name])
            constraint = cursor.fetchone()
            
            if constraint:
                cursor.execute(f"""
                    ALTER TABLE `{script.table_name}`
                    DROP PRIMARY KEY
                """)

            # Add new primary key constraint using the final (possibly overridden) column names
            cursor.execute(f"""
                ALTER TABLE `{script.table_name}` 
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
        with connections['itam'].cursor() as cursor:
            # Execute the transform script
            cursor.execute(table.transform_script)
        
        logger.info(f"Successfully executed transform script for {script.name}")
        return True, f"Transform script for {script.name} executed successfully", None
    except Exception as e:
        logger.error(f"Error during transform script execution for job {job.id}, script {script.name}: {str(e)}", exc_info=True)
        return False, None, f"Error during transform script execution: {str(e)}"


def update_table_metadata(script, job):
    if not script.table_name or script.import_enabled == 0 or not script.table_name.strip():
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
        with connections['itam'].cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM `{script.table_name}`')
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
    if not script.table_name or script.import_enabled == 0 or not script.table_name.strip():
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
            else:
                # Include all existing columns in columns_to_update, regardless of name changes
                columns_to_update.append((orig_name, curr_name))

        logger.info(f"Columns to add: {columns_to_add}")
        logger.info(f"Columns to update: {columns_to_update}")
        logger.info(f"Columns to remove: {columns_to_remove}")

        # Get the actual data types from the database
        with connections['itam'].cursor() as cursor:
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
            column.override_column_name = curr_name if curr_name != orig_name else ''
            column.is_unique = unique_columns.get(orig_name, False)  # Update uniqueness information
            column.detected_data_type = db_column_types.get(curr_name, 'UNKNOWN')
            column.save()
            logger.info(f"Updated column: {column.column_name} (override: {column.override_column_name}, detected: {column.detected_data_type}, is_unique: {column.is_unique})")

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
    if not script.table_name or script.import_enabled == 0 or not script.table_name.strip():
        logger.warning(f"Skipping SQL import for script {script.name}: table_name is empty or None")
        return True, "SQL import skipped: no table name provided", None

    try:
        # Find the latest data file
        file_path = find_latest_data_file()
        if not file_path:
            raise ValueError("No suitable data file found")

        logger.info(f"File path: {file_path}")

        # Read the sample data
        df_sample = read_sample_data(file_path)

        # Get original and final column names
        original_column_names = get_column_names(script)
        final_column_names = get_override_column_names(script, original_column_names)
        column_mapping = dict(zip(original_column_names, final_column_names))

        # Rename columns in the sample DataFrame
        df_sample.columns = original_column_names

        # Infer column types using the sample
        inferred_types = infer_column_types(df_sample)

        # Now read the entire file using the inferred dtypes
        df = read_full_data(file_path, original_column_names, inferred_types)

        # Rename columns in the full DataFrame using the mapping
        df.rename(columns=column_mapping, inplace=True)

        # Convert columns to appropriate types after reading
        for orig_col, final_col in column_mapping.items():
            df[final_col] = convert_column_type(df[final_col], inferred_types[orig_col])

        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")

        # Database operations
        with connections['itam'].cursor() as cursor:
            # Set the character set to UTF-8
            cursor.execute("SET NAMES utf8mb4;")
            
            # Check if the table exists and delete it if it does
            if table_exists(cursor, script.table_name):
                logger.info(f"Table '{script.table_name}' already exists. Deleting it.")
                cursor.execute(f'DROP TABLE IF EXISTS `{script.table_name}`')

            # Create the table with inferred types and final column names
            create_table(cursor, script.table_name, column_mapping, inferred_types)

            # Insert data
            insert_data(cursor, df, script.table_name)

        # Verify the data was inserted
        row_count = get_row_count(script.table_name)
        logger.info(f"Rows in table after insert: {row_count}")

        logger.info(f"Successfully imported {len(df)} rows into {script.table_name}")
        return True, f"Successfully imported {len(df)} rows into {script.table_name}", None
    except Exception as e:
        logger.error(f"Error during import for job {job.id}: {str(e)}", exc_info=True)
        return False, None, f"Error during import: {str(e)}"

def read_sample_data(file_path, nrows=500000):
    if file_path.lower().endswith('.csv'):
        return pd.read_csv(file_path, nrows=nrows, dtype=str, encoding='utf-8-sig')
    elif file_path.lower().endswith('.xlsx'):
        return pd.read_excel(file_path, nrows=nrows, dtype=str)
    elif file_path.lower().endswith('.json'):
        return pd.read_json(file_path, nrows=nrows, dtype=str, encoding='utf-8-sig')
    else:
        raise ValueError("Invalid file type")

def infer_column_types(df_sample):
    inferred_types = {}
    for col in df_sample.columns:
        if df_sample[col].isnull().all():
            inferred_types[col] = 'TEXT'
        elif is_likely_integer(df_sample[col]):
            inferred_types[col] = determine_integer_type(df_sample[col])
        elif is_likely_float(df_sample[col]):
            inferred_types[col] = 'DOUBLE'
        elif is_likely_date(df_sample[col]):
            # Determine if it's a DATETIME or DATE
            sample = df_sample[col].head(100)  # Sample for performance
            if pd.to_datetime(sample, errors='coerce').dt.time.ne(pd.Timestamp('00:00:00').time()).any():
                inferred_types[col] = 'DATETIME'
            else:
                inferred_types[col] = 'DATE'
        else:
            inferred_types[col] = determine_string_type(df_sample[col])
    return inferred_types

def is_likely_integer(series):
    # Remove any completely empty entries
    series = series.dropna()
    
    if series.empty:
        return False

    try:
        # Try to convert to numeric
        numeric_series = pd.to_numeric(series, errors='raise')
        
        # Check if all values are integers
        is_integer = numeric_series.apply(lambda x: x.is_integer()).all()
        
        # Check if all values are within the INT range
        in_range = ((numeric_series >= -2147483648) & (numeric_series <= 2147483647)).all()
        
        return is_integer and in_range
    except ValueError:
        # If conversion to numeric fails, it's not an integer column
        return False

def is_likely_float(series):
    try:
        # Remove any completely empty entries
        series = series.dropna()
        
        if series.empty:
            return False
        
        # Check for dashes in any non-null value
        if series.astype(str).str.contains('-', regex=False).any():
            return False
        
        float_series = pd.to_numeric(series, errors='coerce')
        
        # Check if all non-null values can be converted to float
        all_float = not float_series.isnull().any()
        
        # Check if there are any alphabetic characters
        no_alpha = not series.astype(str).str.contains(r'[a-zA-Z]').any()
        
        # Check if it's not an integer series
        not_integer = not is_likely_integer(series)
        
        return all_float and no_alpha and not_integer
    except:
        return False

def is_likely_date(series):
    # Remove any completely empty entries
    series = series.dropna()
    
    if series.empty:
        return False

    # Sample the series to reduce processing time
    sample_size = min(1000, len(series))
    sample = series.sample(n=sample_size) if len(series) > sample_size else series

    # Common date and datetime formats to try
    date_formats = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
        '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S',
    ]

    # Try parsing with specific formats first
    for date_format in date_formats:
        if pd.to_datetime(sample, format=date_format, errors='coerce').notna().all():
            return True

    # If specific formats fail, use a more flexible approach on a smaller sample
    small_sample = sample.head(100)  # Limit to 100 items for the expensive check

    def is_date(x):
        try:
            if re.match(r'^\d+(\.\d+)?$', str(x)):
                float_val = float(x)
                if 0 <= float_val <= 3155760000:
                    return True
            parse(str(x), fuzzy=False)
            return True
        except (ValueError, OverflowError, ParserError):
            return False

    # Check if at least 90% of the small sample are valid dates
    valid_dates = small_sample.apply(is_date)
    return valid_dates.sum() / len(valid_dates) >= 0.9

def is_date_only(series):
    # Remove any completely empty entries
    series = series.dropna()
    
    if series.empty:
        return False

    try:
        date_series = pd.to_datetime(series, errors='coerce')
        return date_series.notna().any() and (date_series.dt.time == pd.Timestamp('00:00:00').time()).all()
    except:
        return False

def determine_integer_type(series):
    series = pd.to_numeric(series, errors='coerce')
    non_null = series.dropna()
    min_val, max_val = non_null.min(), non_null.max()
    if min_val >= -2147483648 and max_val <= 2147483647:
        return 'INT'
    else:
        return 'BIGINT'

def determine_string_type(series):
    max_length = series.str.len().max()
    if max_length <= 255:
        return f'VARCHAR({255})'
    elif max_length <= 65535:
        return 'TEXT'
    elif max_length <= 16777215:
        return 'MEDIUMTEXT'
    else:
        return 'LONGTEXT'

def read_full_data(file_path, original_column_names, inferred_types):
    dtype_dict = {}
    for col, dtype in inferred_types.items():
        if dtype in ['TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'BIGINT']:
            dtype_dict[col] = 'Int64'  # Use pandas nullable integer type
        elif dtype == 'DOUBLE':
            dtype_dict[col] = 'float64'
        else:
            dtype_dict[col] = 'object'

    parse_dates = [col for col, dtype in inferred_types.items() if dtype in ['DATE', 'DATETIME']]
    
    if file_path.lower().endswith('.csv'):
        return pd.read_csv(file_path, dtype=dtype_dict, parse_dates=parse_dates, keep_default_na=False, na_values=[''], encoding='utf-8-sig')
    elif file_path.lower().endswith('.xlsx'):
        return pd.read_excel(file_path, dtype=dtype_dict, parse_dates=parse_dates, keep_default_na=False, na_values=[''])
    elif file_path.lower().endswith('.json'):
        return pd.read_json(file_path, dtype=dtype_dict, parse_dates=parse_dates, encoding='utf-8-sig')
    else:
        raise ValueError("Unsupported file type")

def convert_column_type(series, dtype):
    if dtype in ['TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'BIGINT']:
        return pd.to_numeric(series, errors='coerce').astype('Int64')
    elif dtype == 'DOUBLE':
        return pd.to_numeric(series, errors='coerce')
    elif dtype in ['DATE', 'DATETIME']:
        return pd.to_datetime(series, errors='coerce')
    else:
        return series

def table_exists(cursor, table_name):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables 
        WHERE table_name = %s
    """, [table_name])
    return cursor.fetchone()[0] > 0

def create_table(cursor, table_name, column_mapping, inferred_types):
    columns = [f'`{col}` {inferred_types[orig_col]} NULL' for orig_col, col in column_mapping.items()]
    create_table_sql = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({", ".join(columns)})'
    logger.info(f"Creating table with SQL: {create_table_sql}")
    cursor.execute(create_table_sql)

def insert_data(cursor, df, table_name):
    # Replace NaN, NaT, and '<NA>' with None
    df = df.replace({pd.NaT: None, '<NA>': None})
    df = df.where(pd.notnull(df), None)
    
    # Convert DataFrame to list of tuples
    if len(df.columns) == 1 and df.dtypes.iloc[0] in ['int64', 'Int64']:
        data = [(int(x) if pd.notnull(x) else None,) for x in df.iloc[:, 0]]
    else:
        data = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
    
    placeholders = ','.join(['%s' for _ in df.columns])
    insert_sql = f'INSERT INTO `{table_name}` VALUES ({placeholders})'
    logger.info(f"Inserting data with SQL: {insert_sql}")
    logger.info(f"Number of rows to insert: {len(data)}")
    
    # Insert data in chunks to avoid potential memory issues
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        cursor.executemany(insert_sql, chunk)

def get_row_count(table_name):
    with connections['itam'].cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM `{table_name}`')
        return cursor.fetchone()[0]
 

def execute_job_core(job_id):
    job = get_object_or_404(Job, id=job_id)
    
    start_time = time.time()
    
    success = True
    output = ""
    error = None
    sql_import_scripts = []

    # Iterate over each script in the parent job
    for script in job.scripts.all().order_by('order_exec'):
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
                logger.info(f"Starting post-script execution steps for {script.name}")

                file_path = find_latest_data_file()
                logger.info(f"Found latest data file: {file_path}")
                
                column_names = get_column_names(script)
                logger.info(f"Retrieved column names: {column_names}")

                # Get or create the Table object
                table, created = Table.objects.get_or_create(
                    script=script,
                    table_name=script.table_name
                )
                logger.info(f"{'Created' if created else 'Retrieved'} Table object for {script.table_name}")

                # Write column names to Table.default_column_names
                table.default_column_names = column_names
                table.save()
                logger.info("Updated Table.default_column_names")

                if script.table_name and script.table_name.strip():
                    sql_import_scripts.append(script)
                    logger.info(f"Added {script.name} to sql_import_scripts")

                # Execute SQL import
                logger.info("Starting SQL import")
                sql_success, script_output, script_error = execute_sql_import(script, job)
                output += f"SQL Import {script.name} output:\n{script_output}\n"
                if not sql_success:
                    raise Exception(f"SQL Import failed: {script_error}")

                # Execute transform script
                logger.info("Starting transform script execution")
                transform_success, transform_output, transform_error = execute_transform_script(script, job)
                if transform_output:
                    output += f"Transform Script {script.name} output:\n{transform_output}\n"
                if not transform_success:
                    raise Exception(f"Transform script failed: {transform_error}")

                # Update table metadata
                logger.info("Updating table metadata")
                metadata_success, metadata_error = update_table_metadata(script, job)
                if not metadata_success:
                    raise Exception(f"Failed to update table metadata: {metadata_error}")

                # Update column metadata
                logger.info("Updating column metadata")
                column_metadata_success, column_metadata_error = update_column_metadata(script, job, column_names, file_path)
                if not column_metadata_success:
                    raise Exception(f"Failed to update column metadata: {column_metadata_error}")

                # Set primary key
                logger.info("Setting primary key")
                pk_success, pk_error = set_table_primary_key(script, job, column_names)
                if not pk_success:
                    raise Exception(f"Failed to set primary key: {pk_error}")

                logger.info(f"Successfully completed all post-script execution steps for {script.name}")

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