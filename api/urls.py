from django.urls import path
from .views import Edit, Init, Sign, Edit_Sign, BindUpdateViewV2

app_name = 'api'

urlpatterns = [
    path('edit/', Edit.as_view(), name='edit'),
    path('sign/', Sign.as_view(), name='sign'),
    path('init/', Init.as_view(), name='init'),
    path('edit-sign/', Edit_Sign.as_view(), name='edit-sign'),
    path('update-bind/v2/', BindUpdateViewV2.as_view(), name='bind_update_api_v2'),
]