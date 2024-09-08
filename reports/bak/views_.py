from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.db import connections, DatabaseError
from .models import Table, Column, Relationship, ReportConfiguration
from .graph_processor import get_all_related_tables
from .query_builder import build_query, get_paginated_results, translate_query_builder_rules, execute_query
import json
import logging
import csv
import openpyxl
from datetime import datetime


logger = logging.getLogger(__name__)

def run_query(request):
    return render(request, 'pages/reports/query_form.html')

 

def chart_view(request):
    # This is example data. In a real application, you'd fetch this from your database.
    data = [
        {"cdate": "2024-08-30", "ndcount": 95, "rdcount": -27},
        {"cdate": "2024-08-29", "ndcount": 150, "rdcount": -39},
        {"cdate": "2024-08-28", "ndcount": 147, "rdcount": -43},
        {"cdate": "2024-08-27", "ndcount": 156, "rdcount": -36},
        {"cdate": "2024-08-26", "ndcount": 169, "rdcount": -42},
        {"cdate": "2024-08-25", "ndcount": 160, "rdcount": -28},
        {"cdate": "2024-08-24", "ndcount": 168, "rdcount": -42},
        {"cdate": "2024-08-23", "ndcount": 174, "rdcount": -36},
        {"cdate": "2024-08-22", "ndcount": 136, "rdcount": -32},
        {"cdate": "2024-08-21", "ndcount": 144, "rdcount": -26},
        {"cdate": "2024-08-20", "ndcount": 169, "rdcount": -30},
        {"cdate": "2024-08-19", "ndcount": 157, "rdcount": -40},
    ]
    
    # Convert the data to JSON
    chart_data = json.dumps(data)
    
    return render(request, 'pages/reports/chart_view.html', {'chart_data': chart_data})



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
    

from .query_builder import build_query, get_paginated_results, translate_query_builder_rules

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
            # Get all column names for the main table and its related tables
            all_columns = set(Column.objects.filter(table_id=main_table_id).values_list('name', flat=True))
            related_tables = get_all_related_tables(int(main_table_id))
            for related_table in related_tables:
                all_columns.update(Column.objects.filter(table_id=related_table.id).values_list('name', flat=True))

            # Filter out rules that reference non-existent columns
            if filters and 'rules' in filters:
                filters['rules'] = [rule for rule in filters['rules'] if rule.get('field') in all_columns]

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
def save_configuration(request):
    try:
        data = json.loads(request.body)
        name = data.get('name')
        config = data.get('configuration')
        
        if not name or not config:
            return JsonResponse({'error': 'Name and configuration are required'}, status=400)
        
        report_config, created = ReportConfiguration.objects.update_or_create(
            name=name,
            defaults={'configuration': json.dumps(config)}
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
