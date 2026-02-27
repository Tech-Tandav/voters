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

logger = logging.getLogger(__name__)

CACHE_TTL = 60 * 5  # 5 minutes
# Custom pagination for voter lists
class VoterPagination(PageNumberPagination):
    """Custom pagination with configurable page size"""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 200


PROVINCE_MAPPING = {
    # 'कोशी': 'Koshi',
    'कोशी प्रदेश': 'Koshi',
    # 'मधेश': 'Madhesh',
    'मधेश प्रदेश': 'Madhesh',
    # 'बागमती': 'Bagmati',
    'बागमती प्रदेश': 'Bagmati',
    # 'गण्डकी': 'Gandaki',
    'गण्डकी प्रदेश': 'Gandaki',
    # 'लुम्बिनी': 'Lumbini',
    'लुम्बिनी प्रदेश': 'Lumbini',
    # 'कर्णाली': 'Karnali',
    'कर्णाली प्रदेश': 'Karnali',
    # 'सुदूरपश्चिम': 'Sudurpashchim',
    'सुदूरपश्चिम प्रदेश': 'Sudurpashchim',
}


def apply_filters(queryset, request):
    params = request.query_params

    age_min = params.get('age_min')
    age_max = params.get('age_max')
    age_group = params.get('age_group')
    gender = params.get('gender')
    caste_group = params.get('caste_group')
    province = params.get('province')
    district = params.get('district')
    constituency = params.get('constituency')
    ward = params.get('ward')
    search = params.get('search')

    if age_min and age_min.isdigit():
        queryset = queryset.filter(age__gte=int(age_min))

    if age_max and age_max.isdigit():
        queryset = queryset.filter(age__lte=int(age_max))

    if age_group:
        queryset = queryset.filter(age_group=age_group)

    if gender:
        queryset = queryset.filter(gender=gender)

    if caste_group:
        queryset = queryset.filter(caste_group=caste_group)

    if province:
        mapped = PROVINCE_MAPPING.get(province.strip(), province.strip())
        queryset = queryset.filter(province=mapped)  # exact match = index friendly

    if district:
        queryset = queryset.filter(district=district)

    if constituency:
        queryset = queryset.filter(constituency=constituency)

    if ward and ward.isdigit():
        queryset = queryset.filter(ward=int(ward))

    if search:
        queryset = queryset.filter(name__icontains=search)

    return queryset
# =============================================================================
# ANALYSIS API ENDPOINTS
# =============================================================================

@extend_schema(
    tags=['Analysis'],
    summary='Get overview statistics',
    description='Returns comprehensive overview of voter demographics',
    parameters=[
        OpenApiParameter('age_min', OpenApiTypes.INT, description='Minimum age'),
        OpenApiParameter('age_max', OpenApiTypes.INT, description='Maximum age'),
        OpenApiParameter('age_group', OpenApiTypes.STR, description='Age group filter'),
        OpenApiParameter('gender', OpenApiTypes.STR, description='Gender filter'),
        OpenApiParameter('caste_group', OpenApiTypes.STR, description='Caste group filter'),
        OpenApiParameter('province', OpenApiTypes.STR, description='Province filter'),
        OpenApiParameter('constituency', OpenApiTypes.STR, description='Constituency filter'),
        OpenApiParameter('ward', OpenApiTypes.INT, description='Ward number'),
    ],
    responses={200: OverviewStatsSerializer}
)


@api_view(['GET'])
def overview_stats(request):
    """
    Get overall demographic statistics.
    Cached per filter combination.
    """
    params = sorted(request.query_params.items())
    raw_key = f"overview_stats:{params}"
    cache_key = "overview_stats:" + hashlib.md5(raw_key.encode()).hexdigest()

    cached = cache.get(cache_key)
    if cached:
        return Response({**cached, "cached": True})

    # ⚡ Only fetch needed column for count & aggs
    queryset = Voter.objects.only('id')
    queryset = apply_filters(queryset, request)

    analytics = VoterAnalytics(queryset)
    data = analytics.get_overview_stats()

    cache.set(cache_key, data, CACHE_TTL)

    return Response({**data, "cached": False})


@extend_schema(
    tags=['Analysis'],
    summary='Get age group distribution',
    description='Returns age group distribution in chart-ready format',
    parameters=[
        OpenApiParameter('gender', OpenApiTypes.STR, description='Filter by gender'),
        OpenApiParameter('caste_group', OpenApiTypes.STR, description='Filter by caste'),
        OpenApiParameter('ward', OpenApiTypes.INT, description='Filter by ward'),
    ],
    responses={200: DistributionResponseSerializer}
)
@api_view(['GET'])
def age_distribution(request):
    """
    Get age group distribution.
    Returns data ready for pie/bar charts.
    """
    queryset = Voter.objects.all()
    queryset = apply_filters(queryset, request)
    
    analytics = get_analytics(queryset)
    data = analytics.get_age_distribution()
    
    return Response(data)




# =============================================================================
# VOTER DATA ENDPOINTS
# =============================================================================


# class VoterViewSet(viewsets.ReadOnlyModelViewSet):
#     """
#     ViewSet for viewing voter data.
#     Supports filtering, search, and pagination.
    
#     list: Get paginated list of voters
#     retrieve: Get details of a specific voter
#     count: Get count of voters (filtered)
#     """
    
#     queryset = Voter.objects.all()
#     serializer_class = VoterSerializer
#     pagination_class = VoterPagination
#     filter_backends = [filters.SearchFilter, filters.OrderingFilter]
#     search_fields = ['name', 'voter_id']
#     ordering_fields = ['age', 'name', 'voter_id']
#     ordering = ['name']
    
#     def get_serializer_class(self):
#         """Use lightweight serializer for list view"""
#         if self.action == 'list':
#             return VoterListSerializer
#         return VoterSerializer
    
#     def get_queryset(self):
#         """Apply filters to queryset"""
#         queryset = super().get_queryset()
#         return apply_filters(queryset, self.request)
    
#     @extend_schema(
#         tags=['Voters'],
#         summary='Get voter count',
#         description='Returns total count of voters (with filters applied)',
#     )
#     @action(detail=False, methods=['get'])
#     def count(self, request):
#         """Get count of voters (respects filters)"""
#         count = self.get_queryset().count()
#         return Response({'count': count})

class VoterViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for viewing voter data.
    Supports filtering, search, and pagination.

    list: Get paginated list of voters
    retrieve: Get details of a specific voter
    count: Get count of voters (filtered, cached)
    """

    queryset = Voter.objects.all()
    serializer_class = VoterSerializer
    pagination_class = VoterPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'voter_id']
    ordering_fields = ['age', 'name', 'voter_id']
    ordering = ['name']

    def get_serializer_class(self):
        if self.action == 'list':
            return VoterListSerializer
        return VoterSerializer

    def get_queryset(self):
        queryset = super().get_queryset().only('id')  # makes count faster
        return apply_filters(queryset, self.request)

    def _build_cache_key(self, request):
        """
        Build a stable cache key based on query params.
        Different filters = different cache entries.
        """
        params = sorted(request.query_params.items())
        raw_key = f"voter_count:{params}"
        return "voter_count:" + hashlib.md5(raw_key.encode()).hexdigest()

    @extend_schema(
        tags=['Voters'],
        summary='Get voter count (cached)',
        description='Returns total count of voters (with filters applied, cached in Redis)',
    )
    @action(detail=False, methods=['get'])
    def count(self, request):
        cache_key = self._build_cache_key(request)

        cached_count = cache.get(cache_key)
        if cached_count is not None:
            return Response({
                'count': cached_count,
                'cached': True
            })

        queryset = self.get_queryset()
        count = queryset.count()

        # Cache for 5 minutes (tune based on how fresh data must be)
        cache.set(cache_key, count, timeout=60 * 10)

        return Response({
            'count': count,
            'cached': False
        })
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