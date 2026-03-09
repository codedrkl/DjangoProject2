from django.urls import path
from .views import option_chain, outcome_view, footprint_view

urlpatterns = [
    path('', option_chain, name='option_chain'),
    path('outcome/', outcome_view, name='outcome_view'),
    path('footprint/', footprint_view, name='footprint_view'), # Dedicated URL
]