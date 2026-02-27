"""
URL Configuration for Voters API

Maps URL patterns to views.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from voters.detail import views

# Create router for ViewSets
router = DefaultRouter()
router.register(r'voters', views.VoterViewSet, basename='voter')
router.register(r'admin/upload-history', views.UploadHistoryViewSet, basename='upload-history')
router.register(r'admin/surnames', views.SurnameMappingViewSet, basename='surname-mapping')

# URL patterns
urlpatterns = [
    # Include router URLs
    path('', include(router.urls)),
    
    # Analysis Endpoints
    path('analysis/overview/', views.OverviewStatsView.as_view(), name='analysis-overview'),
    path('analysis/age-distribution/',views.AgeDistributionView.as_view(), name='analysis-age-distribution'),
    
    # Admin Endpoints
    path('admin/upload/', views.upload_csv, name='admin-upload-csv'),
    path('admin/upload-zip/', views.upload_zip, name='admin-upload-zip'),
]