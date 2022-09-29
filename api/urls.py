from django.urls import path
from .views import Edit, Init, Sign, EditandSign

app_name = 'api'

urlpatterns = [
    path('edit/', Edit.as_view(), name='edit'),
    path('sign/', Sign.as_view(), name='sign'),
    path('init/', Init.as_view(), name='init'),
    path('edit-sign/', EditandSign.as_view(), name='edit-sign'),
]