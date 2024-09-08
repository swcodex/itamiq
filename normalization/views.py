import pandas as pd
from django.http import JsonResponse
from django.db import connection
import json
import logging
import numpy as np
from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView, PasswordResetView, PasswordChangeView, PasswordResetConfirmView
from django.contrib.auth import logout
from django.views.generic import CreateView
from django.db import connection
from django.db.utils import IntegrityError
from django.views.decorators.csrf import csrf_exempt
import sqlite3
import csv
import io
import os
from django.urls import reverse
from django.views import View
from django.conf import settings
logger = logging.getLogger(__name__)


# Software Normalization

def sanitize_table_name(name):
    return name.replace(' ', '_')

def desanitize_table_name(name):
    return name.replace('_', ' ')

def datasource_catalog(request):
    db_path = os.path.join(settings.BASE_DIR, 'normalization.sqlite3')
    
    tables = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        
        for table in table_names:
            table_name = table[0]
            # Get row count for each table
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            row_count = cursor.fetchone()[0]
            
            tables.append({
                'name': table_name,
                'displayName': desanitize_table_name(table_name),
                'rowCount': row_count
            })
        
        conn.close()
    except Exception as e:
        print(f"Error fetching tables: {str(e)}")
    
    context = {
        'parent': 'datasource_catalog',
        'existing_tables': tables
    }
    return render(request, 'pages/normalization/datasource-catalog.html', context)

@csrf_exempt
def software_match(request):
  context = {
    'parent': 'software_match',
  }
  return render(request, 'pages/normalization/software-match.html', context)



def upload_file(request):
    logger.debug("Upload file function called")
    logger.debug(f"Method: {request.method}")
    logger.debug(f"POST data: {request.POST}")
    logger.debug(f"FILES: {request.FILES}")
    if request.method == 'POST':
        logger.debug("POST request received")
        try:
            file = request.FILES.get('file')
            logger.debug(f"File received: {file}")
            if not file:
                logger.error("No file uploaded")
                return JsonResponse({'error': 'No file uploaded'}, status=400)

            datasource_name = request.POST.get('datasourceName')
            logger.debug(f"POST data: {request.POST}")
            logger.debug(f"Datasource name: {datasource_name}")

            if not datasource_name:
                logger.error("Data source name is missing")
                return JsonResponse({'error': 'Data source name is required'}, status=400)

            # Create a sanitized version of the datasource name for the table
            table_name = datasource_name.replace(' ', '_')

            # Read the file
            logger.debug(f"File name: {file.name}")
            if file.name.endswith('.csv'):
                logger.debug("Reading CSV file")
                df = pd.read_csv(file)
            elif file.name.endswith('.xlsx'):
                logger.debug("Reading Excel file")
                df = pd.read_excel(file)
            else:
                logger.error("Invalid file type")
                return JsonResponse({'error': 'Invalid file type'}, status=400)

            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")

            # Replace NaN values with None
            df = df.replace({np.nan: None})

            # Create or recreate the table
            with connection.cursor() as cursor:
                logger.debug(f"Dropping table if exists: {datasource_name}")
                cursor.execute(f"DROP TABLE IF EXISTS [{datasource_name}]")
                
                columns = [f"[{col}] TEXT" for col in df.columns]
                create_table_sql = f"CREATE TABLE [{datasource_name}] ({', '.join(columns)})"
                logger.debug(f"Create table SQL: {create_table_sql}")
                cursor.execute(create_table_sql)

            # Bulk insert data
            logger.debug("Inserting data into table")
            db_path = os.path.join(settings.BASE_DIR, 'normalization.sqlite3')
            
            # Create a connection to the new database (this will create the file if it doesn't exist)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create or recreate the table
            logger.debug(f"Dropping table if exists: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
            
            columns = [f"[{col}] TEXT" for col in df.columns]
            create_table_sql = f"CREATE TABLE [{table_name}] ({', '.join(columns)})"
            logger.debug(f"Create table SQL: {create_table_sql}")
            cursor.execute(create_table_sql)

            # Bulk insert data
            logger.debug("Inserting data into table")
            
            # Convert DataFrame to list of tuples
            data = [tuple(x) for x in df.to_numpy()]
            
            # Use SQLite's executemany
            placeholders = ','.join(['?' for _ in df.columns])
            cursor.executemany(f"INSERT INTO [{table_name}] VALUES ({placeholders})", data)
            
            # Commit changes and close the connection
            conn.commit()
            conn.close()

            logger.debug("File upload and table creation successful")
            return JsonResponse({
                'message': 'File uploaded and table created successfully',
                'tableName': table_name,
                'displayName': datasource_name,  # Send both table name and display name
                'rowCount': len(df)
            })

        except Exception as e:
            logger.exception(f"Error in upload_file: {str(e)}")
            return JsonResponse({'error': str(e)}, status=500)

    logger.error("Invalid request method")
    return JsonResponse({'error': 'Invalid request method'}, status=400)


@csrf_exempt
def delete_datasource(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        table_name = data.get('tableName').replace(' ', '_')  # Sanitize the table name

        db_path = os.path.join(settings.BASE_DIR, 'normalization.sqlite3')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
        
        conn.commit()
        conn.close()

        return JsonResponse({'success': True})

    return JsonResponse({'error': 'Invalid request'}, status=400)