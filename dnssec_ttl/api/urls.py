from django.urls import path
from .views import Edit, Initialize, Sign

app_name = 'api'

urlpatterns = [
    path('edit/', Edit.as_view(), name='edit'),
    path('sign/', Sign.as_view(), name='sign'),
    path('init/', Initialize.as_view(), name='init'),
]