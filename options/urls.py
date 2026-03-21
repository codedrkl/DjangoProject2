from django.urls import path
from .views import option_chain, outcome_view, footprint_view, pnl_test_view

app_name = 'options'

urlpatterns = [
    path('chain/', option_chain, name='option_chain'),
    path('outcome/', outcome_view, name='outcome_view'),
    path('footprint/', footprint_view, name='footprint_view'), # Dedicated URL
    path('test-pnl/', pnl_test_view, name='test_pnl'),
]