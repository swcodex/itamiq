from django.urls import path
from . import views

app_name = 'reconciliation'

urlpatterns = [
    path('', views.home, name='home'),
    path('jobs/', views.job_list, name='job_list'),
    path('tables/', views.table_list, name='table_list'),
    path('save_table_list/', views.save_table_list, name='save_table_list'),
    path('add-job/', views.add_job, name='add_job'),
    path('table/<int:table_id>/edit/', views.edit_table, name='table_edit'),
    path('edit-job/<int:job_id>/', views.edit_job, name='edit_job'),
    path('execute-job/<int:job_id>/', views.execute_job, name='execute_job'),
    path('job/<int:job_id>/delete/', views.delete_job, name='delete_job'),
    path('table/<int:table_id>/view/', views.table_view, name='table_view'),
]