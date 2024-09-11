from django.urls import path
from . import views

app_name = 'reports'

urlpatterns = [
    path('run_query/', views.run_query, name='run_query'),
    path('run_query_sql/', views.run_query_sql, name='run_query_sql'),
    path('get_tables/', views.get_tables, name='get_tables'),
    path('get_columns/<int:table_id>/', views.get_columns, name='get_columns'),
    path('get_related_tables/<int:table_id>/', views.get_related_tables, name='get_related_tables'),
    path('generate_report/', views.generate_report, name='generate_report'),
    path('generate_report_sql/', views.generate_report_sql, name='generate_report_sql'),
    path('get_filter_options/', views.get_filter_options, name='get_filter_options'),
    path('export_report/', views.export_report, name='export_report'),
    path('export_report_sql/', views.export_report_sql, name='export_report_sql'),
    path('save-configuration/', views.save_configuration, name='save_configuration'),
    path('get-configurations/', views.get_configurations, name='get_configurations'),
    path('load-configuration/<int:config_id>/', views.load_configuration, name='load_configuration'),
    path('computer_chart/', views.computer_chart, name='computer_chart'),
    path('report-configurations/', views.report_configurations, name='report_configurations'),
    path('view-report-configuration/<int:config_id>/', views.view_report_configuration, name='view_report_configuration'),
    path('delete-configuration/<int:config_id>/', views.delete_configuration, name='delete_configuration'),
]
