from django.urls import path
from . import views

app_name = 'normalization'

urlpatterns = [
    path("", views.datasource_catalog, name='datasource_catalog'),
    path('delete-datasource/', views.delete_datasource, name='delete_datasource'),
    path('upload-file/', views.upload_file, name='upload_file'),
]
