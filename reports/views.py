from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.db import connections, DatabaseError
from .models import Table, Column, Relationship, ReportConfiguration
from .graph_processor import get_all_related_tables
from .query_builder import build_query, get_paginated_results, translate_query_builder_rules, execute_query
from datetime import datetime, date
import json
import logging
import csv
import openpyxl


logger = logging.getLogger(__name__)

def run_query(request):
    return render(request, 'pages/reports/query_form.html')

def run_query_sql(request):
    return render(request, 'pages/reports/query_form_sql.html')

def get_device_counts():
    with connections['itam'].cursor() as cursor:
        sql_query = """
        SELECT cdate, ComplianceComputerTypeID as DomainID, SUM(ndcount) as ndcount, SUM(rdcount) as rdcount 
        FROM devicecountstype where cdate > '2024-08-10'
        GROUP BY cdate, ComplianceComputerTypeID
        ORDER BY cdate DESC, ComplianceComputerTypeID
        """
        
        try:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            
            if not results:
                print("Query executed successfully but returned no results.")
                return []
            
            if cursor.description is None:
                print("cursor.description is None. This is unexpected.")
                return []
            
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            print(f"Error executing SQL query: {str(e)}")
            return None

def computer_chart(request):
    # Fetch data from the data warehouse
    data = get_device_counts()
    
    if data is None:
        return HttpResponse("Error fetching data from the database. Please check the server logs.", status=500)
    
    if not data:
        return HttpResponse("No data returned from the query. The view might be empty.", status=204)

    # Process the data
    processed_data = {}
    for item in data:
        cdate = item['cdate'].strftime('%Y-%m-%d') if isinstance(item['cdate'], (date, datetime)) else item['cdate']
        if cdate not in processed_data:
            processed_data[cdate] = []
        processed_data[cdate].append({
            'DomainID': item['DomainID'],
            'ndcount': int(item['ndcount']),  # Convert to integer
            'rdcount': -abs(int(item['rdcount']))  # Convert to integer and make rdcount negative
        })

    # Convert the processed data to a list of dictionaries
    chart_data = [{'cdate': date, 'domains': domains} for date, domains in processed_data.items()]

    # Sort the data by date (oldest first)
    chart_data.sort(key=lambda x: x['cdate'])

    # Custom JSON encoder to handle date objects
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (date, datetime)):
                return obj.strftime('%Y-%m-%d')
            return super().default(obj)

    # Convert the data to JSON using the custom encoder
    chart_data_json = json.dumps(chart_data, cls=DateEncoder)
   
    return render(request, 'pages/reports/computer_chart.html', {'chart_data': chart_data_json})



@require_http_methods(["GET"])
def get_tables(request):
    try:
        tables = Table.objects.all().values('id', 'name')
        return JsonResponse(list(tables), safe=False)
    except Exception as e:
        logger.error(f"Error fetching tables: {str(e)}")
        return JsonResponse({'error': 'An error occurred while fetching tables'}, status=500)

@require_http_methods(["GET"])
def get_columns(request, table_id):
    try:
        columns = Column.objects.filter(table_id=table_id).values('id', 'name', 'data_type')
        return JsonResponse(list(columns), safe=False)
    except Exception as e:
        logger.error(f"Error fetching columns for table {table_id}: {str(e)}")
        return JsonResponse({'error': 'An error occurred while fetching columns'}, status=500)

@require_http_methods(["GET"])
def get_related_tables(request, table_id):
    logger.debug(f"Fetching related tables for table_id: {table_id}")
    try:
        related_tables = get_all_related_tables(int(table_id))
        
        logger.debug(f"Number of related tables found: {len(related_tables)}")
        related_tables_data = [{'id': t.id, 'name': t.name} for t in related_tables]
        return JsonResponse(related_tables_data, safe=False)
    except ValueError as e:
        logger.error(f"Error: {str(e)}")
        return JsonResponse({'error': str(e)}, status=404)
    except Exception as e:
        logger.error(f"Error fetching related tables for table {table_id}: {str(e)}")
        return JsonResponse({'error': 'An error occurred while fetching related tables'}, status=500)
    

@require_http_methods(["POST"])
@csrf_exempt
def generate_report(request):
    logger.debug("generate_report view called")
    try:
        data = json.loads(request.body)
        selected_column_ids = data.get('columns', [])
        main_table_id = data.get('main_table_id')
        filters = data.get('filters')
        
        page = data.get('page', 1)
        per_page = data.get('per_page', 10)

        logger.debug(f"Received filters: {filters}")

        if not selected_column_ids:
            return JsonResponse({'error': 'No columns selected'}, status=400)
        
        if not main_table_id:
            return JsonResponse({'error': 'No main table selected'}, status=400)

        selected_columns = Column.objects.filter(id__in=selected_column_ids)

        try:
            where_clause, params = translate_query_builder_rules(filters)
            logger.debug(f"Generated WHERE clause: {where_clause}")
            logger.debug(f"Generated params: {params}")
            paginated_results = get_paginated_results(selected_columns, main_table_id, where_clause, params, page, per_page)
        except ValueError as e:
            return JsonResponse({'error': str(e)}, status=400)
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            return JsonResponse({'error': f'Error generating report: {str(e)}'}, status=500)

        return JsonResponse({
            'results': paginated_results['results'] or [],
            'total_count': paginated_results['total_count'],
            'current_page': paginated_results['page_obj'].number,
            'num_pages': paginated_results['page_obj'].paginator.num_pages,
            'has_next': paginated_results['page_obj'].has_next(),
            'has_previous': paginated_results['page_obj'].has_previous(),
        })
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON in request body'}, status=400)
    except Exception as e:
        logger.error(f"Unexpected error in generate_report: {str(e)}")
        return JsonResponse({'error': f'Unexpected error: {str(e)}'}, status=500)
    

@csrf_exempt    
def generate_report_sql(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        sql_query = data.get('sql_query')
        page = data.get('page', 1)
        per_page = data.get('per_page', 10)

        try:
            with connections['itam'].cursor() as cursor:
                cursor.execute(sql_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

            total_count = len(rows)
            start = (page - 1) * per_page
            end = start + per_page
            paginated_rows = rows[start:end]

            results = [dict(zip(columns, row)) for row in paginated_rows]

            return JsonResponse({
                'results': results,
                'total_count': total_count
            })
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

    return JsonResponse({'error': 'Invalid request method'}, status=405)


@require_http_methods(["GET"])
def get_filter_options(request):
    try:
        operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL']
        return JsonResponse({'operators': operators})
    except Exception as e:
        logger.error(f"Error fetching filter options: {str(e)}")
        return JsonResponse({'error': 'An error occurred while fetching filter options'}, status=500)
    

@require_http_methods(["POST"])
@csrf_exempt
def export_report(request):
    try:
        data = json.loads(request.body)
        selected_column_ids = data.get('columns', [])
        main_table_id = data.get('main_table_id')
        export_type = data.get('export_type', 'csv')
        column_order = data.get('column_order', [])
        filters = data.get('filters')  # Add this line to get the filters

        if not selected_column_ids:
            return JsonResponse({'error': 'No columns selected'}, status=400)
        
        if not main_table_id:
            return JsonResponse({'error': 'No main table selected'}, status=400)

        # Get selected columns
        selected_columns = list(Column.objects.filter(id__in=selected_column_ids))

        # Create a dictionary mapping column names to Column objects
        column_dict = {col.name: col for col in selected_columns}

        # Sort columns based on the received order
        ordered_columns = [column_dict[col_name] for col_name in column_order if col_name in column_dict]

        # Add any columns that were selected but not in the order (shouldn't happen, but just in case)
        ordered_columns.extend([col for col in selected_columns if col not in ordered_columns])

        # Translate the filters to a WHERE clause and params
        where_clause, params = translate_query_builder_rules(filters)

        # Build the query with filters
        query = build_query(ordered_columns, main_table_id, where_clause=where_clause)

        # Execute the query with filters
        results = execute_query(query, params)

        if export_type == 'csv':
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="report_export.csv"'
            writer = csv.writer(response)
            writer.writerow([col.name for col in ordered_columns])
            for row in results:
                writer.writerow([row.get(col.name, '') for col in ordered_columns])
        elif export_type == 'excel':
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.append([col.name for col in ordered_columns])
            for row in results:
                worksheet.append([row.get(col.name, '') for col in ordered_columns])
            response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="report_export.xlsx"'
            workbook.save(response)
        else:
            return JsonResponse({'error': 'Invalid export type'}, status=400)

        return response
    except Exception as e:
        logger.error(f"Unexpected error in export_report: {str(e)}")
        return JsonResponse({'error': f'Unexpected error: {str(e)}'}, status=500)
    


@require_http_methods(["POST"])
@csrf_exempt
def export_report_sql(request):
    try:
        data = json.loads(request.body)
        sql_query = data.get('sql_query')
        export_type = data.get('export_type', 'csv')
        column_order = data.get('column_order', [])

        if not sql_query:
            return JsonResponse({'error': 'SQL query is required'}, status=400)

        # Execute the SQL query
        with connections['itam'].cursor() as cursor:
            cursor.execute(sql_query)
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        # If column_order is provided, use it to order the columns
        if column_order:
            ordered_columns = [col for col in column_order if col in columns]
            # Add any columns that were in the query but not in the order
            ordered_columns.extend([col for col in columns if col not in ordered_columns])
        else:
            ordered_columns = columns

        if export_type == 'csv':
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="report_export.csv"'
            writer = csv.writer(response)
            writer.writerow(ordered_columns)
            for row in results:
                writer.writerow([row.get(col, '') for col in ordered_columns])
        elif export_type == 'excel':
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.append(ordered_columns)
            for row in results:
                worksheet.append([row.get(col, '') for col in ordered_columns])
            response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="report_export.xlsx"'
            workbook.save(response)
        else:
            return JsonResponse({'error': 'Invalid export type'}, status=400)

        return response
    except Exception as e:
        logger.error(f"Unexpected error in export_report: {str(e)}")
        return JsonResponse({'error': f'Unexpected error: {str(e)}'}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def save_configuration(request):
    try:
        data = json.loads(request.body)
        name = data.get('name')
        config = data.get('configuration')
        sql_report = data.get('sql_report', False)  # Get sql_report value, default to False if not provided
        
        if not name or not config:
            return JsonResponse({'error': 'Name and configuration are required'}, status=400)
        
        report_config, created = ReportConfiguration.objects.update_or_create(
            name=name,
            defaults={
                'configuration': json.dumps(config),
                'sql_report': sql_report  # Add this line to save the sql_report value
            }
        )
        
        return JsonResponse({'message': 'Configuration saved successfully', 'id': report_config.id})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_configurations(request):
    configs = ReportConfiguration.objects.all().values('id', 'name')
    return JsonResponse(list(configs), safe=False)


@require_http_methods(["GET"])
def load_configuration(request, config_id):
    try:
        config = ReportConfiguration.objects.get(id=config_id)
        return JsonResponse(config.get_configuration())
    except ReportConfiguration.DoesNotExist:
        return JsonResponse({'error': 'Configuration not found'}, status=404)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def report_configurations(request):
    configurations = ReportConfiguration.objects.all().order_by('-updated_at')
    return render(request, 'pages/reports/report_configurations.html', {'configurations': configurations})


def view_report_configuration(request, config_id):
    config = get_object_or_404(ReportConfiguration, id=config_id)
    
    if config.sql_report:
        template_name = 'pages/reports/view_report_configuration_sql.html'
    else:
        template_name = 'pages/reports/view_report_configuration.html'
    
    return render(request, template_name, {'config_id': config_id})



def delete_configuration(request, config_id):
    config = get_object_or_404(ReportConfiguration, id=config_id)
    if request.method == 'POST':
        config.delete()
        return redirect('reports:report_configurations')  # Use the URL name here
    return redirect('reports:report_configurations')  # Use the URL name here as well