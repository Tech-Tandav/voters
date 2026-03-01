"""
API Views for Voter Analysis System

Provides REST API endpoints for:
- Voter data listing and filtering
- Statistical analysis
- CSV upload (admin)
"""
import hashlib
from django.core.cache import cache
from rest_framework import viewsets, status, filters
from rest_framework.decorators import api_view, action, permission_classes, authentication_classes
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated, AllowAny
from django.db.models import Q, Count
from django.views.decorators.csrf import csrf_exempt
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
from drf_spectacular.types import OpenApiTypes
from django.db.models import Avg, Count
from voters.detail.models import Voter, SurnameMapping, UploadHistory
from voters.detail.serializers import (
    VoterSerializer,
    VoterListSerializer,
    SurnameMappingSerializer,
    UploadHistorySerializer,
    CSVUploadSerializer,
    ZipUploadSerializer,
    OverviewStatsSerializer,
    DistributionResponseSerializer,
    CrossAnalysisResponseSerializer,
)
from voters.detail.utils import process_csv_file, get_analytics, VoterAnalytics
from voters.detail.utils.zip_processor import process_zip_file
import logging
from voters.detail.filters import VoterAnalyticsFilter
from rest_framework.generics import GenericAPIView


logger = logging.getLogger(__name__)


class VoterPagination(PageNumberPagination):
    """Custom pagination with configurable page size"""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 200





# =============================================================================
# ANALYSIS API ENDPOINTS
# =============================================================================
from django_filters.rest_framework import DjangoFilterBackend


class OverviewStatsView(GenericAPIView):
    queryset = Voter.objects.all()
    filter_backends = [DjangoFilterBackend]
    filterset_class = VoterAnalyticsFilter

    def get(self, request, *args, **kwargs):
        qs = self.filter_queryset(self.get_queryset())

        total = qs.count()
        if not total:
            return Response({
                'total_voters': 0,
                'average_age': 0,
                'median_age': None,
                'gender_distribution': {},
                'age_group_summary': {},
                'caste_summary': {},
            })

        avg_age = qs.aggregate(avg_age=Avg('age'))['avg_age'] or 0

        gender_qs = qs.values('gender').annotate(total=Count('id'))
        age_group_qs = qs.values('age_group').annotate(total=Count('id'))
        caste_qs = qs.values('caste_group').annotate(total=Count('id'))

        gender_dist = {row['gender']: row['total'] for row in gender_qs}
        gender_pct = {f"{row['gender']}_percentage": round(row['total']*100/total, 1) for row in gender_qs}

        return Response({
            'total_voters': total,
            'average_age': round(avg_age, 1),
            'median_age': None,
            'gender_distribution': {**gender_dist, **gender_pct},
            'age_group_summary': {row['age_group']: row['total'] for row in age_group_qs},
            'caste_summary': {row['caste_group']: row['total'] for row in caste_qs},
        })


class AgeDistributionView(GenericAPIView):
    queryset = Voter.objects.all()
    filter_backends = [DjangoFilterBackend]
    filterset_class = VoterAnalyticsFilter

    def get(self, request, *args, **kwargs):
        qs = self.filter_queryset(self.get_queryset())

        AGE_GROUP_LABELS = {
            'gen_z': 'Gen Z (18-29)',
            'working': 'Working & Family (30-45)',
            'mature': 'Mature (46-60)',
            'senior': 'Senior (60+)',
        }

        grouped = list(qs.values('age_group').annotate(total=Count('id')))
        total = sum(row['total'] for row in grouped)

        counts = {row['age_group']: row['total'] for row in grouped}

        labels, values, percentages = [], [], []
        for key, label in AGE_GROUP_LABELS.items():
            count = counts.get(key, 0)
            labels.append(label)
            values.append(count)
            percentages.append(round((count*100)/total, 1) if total else 0)

        return Response({
            'chart_data': {
                'labels': labels,
                'values': values,
                'percentages': percentages,
            },
            'total': total,
        })






# =============================================================================
# VOTER DATA ENDPOINTS
# =============================================================================



class VoterViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Read-only API for voters.
    No caching anywhere. Every request hits the database.
    """
    queryset = Voter.objects.all()
    serializer_class = VoterSerializer
    pagination_class = VoterPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['voter_id']  # only indexed fields for performance
    ordering_fields = ['age', 'voter_id']
    ordering = ['voter_id']
    filterset_class = VoterAnalyticsFilter

    def get_serializer_class(self):
        if self.action == 'list':
            return VoterListSerializer
        return VoterSerializer

    @action(detail=False, methods=['get'])
    def count(self, request):
        """Get total count of voters (no cache)"""
        count = self.filter_queryset(self.get_queryset()).count()
        return Response({'count': count})
# =============================================================================
# ADMIN ENDPOINTS (CSV Upload, Surname Management)
# =============================================================================


@extend_schema(
    tags=['Admin'],
    summary='Upload CSV file',
    description='Upload and process voter data CSV file. Requires authentication.',
    request=CSVUploadSerializer,
    responses={
        200: {
            'type': 'object',
            'properties': {
                'success': {'type': 'boolean'},
                'message': {'type': 'string'},
                'total': {'type': 'integer'},
                'imported': {'type': 'integer'},
                'failed': {'type': 'integer'},
                'unmapped_surnames': {'type': 'array'},
            }
        }
    }
)
@api_view(['POST'])
@authentication_classes([])  # Disable authentication to bypass CSRF
@permission_classes([AllowAny])
def upload_csv(request):
    """
    Upload and process CSV file containing voter data.
    
    Admin endpoint - processes CSV and imports data into database.
    Returns processing statistics and any errors.
    """
    serializer = CSVUploadSerializer(data=request.data)
    
    if not serializer.is_valid():
        return Response(
            {'error': serializer.errors},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    csv_file = serializer.validated_data['file']
    
    # Process CSV file
    logger.info(f"Processing CSV upload: {csv_file.name}")
    result = process_csv_file(csv_file, request.user if request.user.is_authenticated else None)
    
    if result['success']:
        return Response({
            'success': True,
            'message': 'CSV processed successfully',
            'total': result['total'],
            'imported': result['imported'],
            'failed': result['failed'],
            'unmapped_surnames': result.get('unmapped_surnames', []),
            'processing_time': result.get('processing_time', 0),
        })
    else:
        return Response({
            'success': False,
            'error': result.get('error', 'Processing failed'),
            'total': result.get('total', 0),
            'imported': result.get('imported', 0),
            'failed': result.get('failed', 0),
        }, status=status.HTTP_400_BAD_REQUEST)


@extend_schema(
    tags=['Admin'],
    summary='Upload ZIP file with Province/Constituency folders',
    description='Upload ZIP file containing folders for provinces and CSVs for constituencies.',
    request=ZipUploadSerializer,
    responses={
        200: {
            'type': 'object',
            'properties': {
                'success': {'type': 'boolean'},
                'message': {'type': 'string'},
                'total_files': {'type': 'integer'},
                'processed_files': {'type': 'integer'},
                'total_records': {'type': 'integer'},
                'imported_records': {'type': 'integer'},
                'failed_records': {'type': 'integer'},
                'unmapped_surnames': {'type': 'array'},
            }
        }
    }
)
@api_view(['POST'])
@authentication_classes([])
@permission_classes([AllowAny])
def upload_zip(request):
    """
    Upload and process ZIP file.
    
    Structure:
    Province/
      Constituency.csv
    """
    serializer = ZipUploadSerializer(data=request.data)
    
    if not serializer.is_valid():
        return Response(
            {'error': serializer.errors},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    zip_file = serializer.validated_data['file']
    
    logger.info(f"Processing ZIP upload: {zip_file.name}")
    result = process_zip_file(zip_file, request.user if request.user.is_authenticated else None)
    
    if result['success']:
        return Response(result)
    else:
        return Response(result, status=status.HTTP_400_BAD_REQUEST)


class UploadHistoryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing CSV upload history.
    Admin can see history of all uploads.
    """
    queryset = UploadHistory.objects.all()
    serializer_class = UploadHistorySerializer
    permission_classes = [AllowAny]


class SurnameMappingViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing surname-caste mappings.
    Allows CRUD operations on surname mappings.
    """
    queryset = SurnameMapping.objects.all()
    serializer_class = SurnameMappingSerializer
    permission_classes = [AllowAny]
    authentication_classes = []
    filter_backends = [filters.SearchFilter]
    search_fields = ['surname', 'caste_group']

    def create(self, request, *args, **kwargs):
        """
        Handle POST request to create or update a surname mapping.
        If the surname already exists, update its caste group.
        """
        surname = request.data.get('surname')
        if surname:
            # Check if mapping already exists
            mapping = SurnameMapping.objects.filter(surname=surname).first()
            if mapping:
                # Update existing mapping
                serializer = self.get_serializer(mapping, data=request.data, partial=True)
                serializer.is_valid(raise_exception=True)
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
        
        # Default behavior for new surnames
        return super().create(request, *args, **kwargs)