"""
Django REST Framework Serializers

Define how model data is converted to/from JSON for API responses.
"""

from rest_framework import serializers
from voters.detail.models import Voter, SurnameMapping, UploadHistory


class VoterSerializer(serializers.ModelSerializer):
    """
    Serializer for Voter model.
    Used for listing and detail views.
    """
    
    age_group_display = serializers.CharField(source='get_age_group_display', read_only=True)
    gender_display = serializers.CharField(source='get_gender_display', read_only=True)
    
    class Meta:
        model = Voter
        fields = [
            'id',
            'voter_id',
            'name',
            'surname',
            'age',
            'age_group',
            'age_group_display',
            'gender',
            'gender_display',
            'caste_group',
            'province',
            'district',
            'constituency',
            'municipality',
            'ward',
            'center',
            'spouse',
            'parent',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']


class VoterListSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for voter lists (paginated).
    Only includes essential fields for better performance.
    """
    
    age_group_display = serializers.CharField(source='get_age_group_display', read_only=True)
    
    class Meta:
        model = Voter
        fields = [
            'id',
            'voter_id',
            'name',
            'age',
            'age_group',
            'age_group_display',
            'gender',
            'surname',
            'caste_group',
            'ward',
            'constituency',
        ]


class SurnameMappingSerializer(serializers.ModelSerializer):
    """
    Serializer for SurnameMapping model.
    Used for admin API to manage surname-caste mappings.
    """
    
    caste_group_display = serializers.CharField(source='get_caste_group_display', read_only=True)
    
    class Meta:
        model = SurnameMapping
        fields = [
            'id',
            'surname',
            'caste_group',
            'caste_group_display',
            'is_active',
            'notes',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class UploadHistorySerializer(serializers.ModelSerializer):
    """
    Serializer for UploadHistory model.
    Shows CSV upload history and processing status.
    """
    
    uploaded_by_username = serializers.CharField(
        source='uploaded_by.username',
        read_only=True,
        allow_null=True
    )
    
    class Meta:
        model = UploadHistory
        fields = [
            'id',
            'file_name',
            'uploaded_by_username',
            'upload_date',
            'total_records',
            'success_count',
            'error_count',
            'status',
            'error_log',
            'unmapped_surnames',
            'processing_time',
        ]
        read_only_fields = ['id', 'upload_date']


class CSVUploadSerializer(serializers.Serializer):
    """
    Serializer for CSV file upload.
    Validates file format and size.
    """
    
    file = serializers.FileField(
        help_text="CSV file containing voter data"
    )
    
    def validate_file(self, value):
        """
        Validate uploaded file.
        Check file extension and size.
        """
        # Check file extension
        if not value.name.endswith('.csv'):
            raise serializers.ValidationError("File must be a CSV file (.csv)")
        
        # # Check file size (max 10MB)
        # if value.size > 10 * 1024 * 1024:
        #     raise serializers.ValidationError("File size must be less than 10MB")
        
        return value


class ZipUploadSerializer(serializers.Serializer):
    """
    Serializer for ZIP file upload (Province/Constituency folders).
    Validates file format and size.
    """
    
    file = serializers.FileField(
        help_text="ZIP file containing province folders and constituency CSVs"
    )
    
    def validate_file(self, value):
        """
        Validate uploaded file.
        Check file extension and size.
        """
        # Check file extension
        if not value.name.lower().endswith('.zip'):
            raise serializers.ValidationError("File must be a ZIP file (.zip)")
        
        # Check file size (max 55MB)
        if value.size > 55 * 1024 * 1024:
            raise serializers.ValidationError("File size must be less than 55MB")
        
        return value


# Serializers for chart-ready data responses
# These are not model-based, just for API response structure

class ChartDataSerializer(serializers.Serializer):
    """Generic chart data format"""
    labels = serializers.ListField(child=serializers.CharField())
    values = serializers.ListField(child=serializers.IntegerField())
    percentages = serializers.ListField(child=serializers.FloatField())


class DatasetSerializer(serializers.Serializer):
    """For multi-dataset charts (stacked/grouped bars)"""
    label = serializers.CharField()
    values = serializers.ListField(child=serializers.IntegerField())


class CrossAnalysisChartSerializer(serializers.Serializer):
    """For cross-analysis chart data"""
    labels = serializers.ListField(child=serializers.CharField())
    datasets = DatasetSerializer(many=True)


class TableDataSerializer(serializers.Serializer):
    """For table format data"""
    headers = serializers.ListField(child=serializers.CharField())
    rows = serializers.ListField()


class OverviewStatsSerializer(serializers.Serializer):
    """Overview statistics response"""
    total_voters = serializers.IntegerField()
    average_age = serializers.FloatField()
    median_age = serializers.FloatField()
    gender_distribution = serializers.DictField()
    age_group_summary = serializers.DictField()
    caste_summary = serializers.DictField()


class DistributionResponseSerializer(serializers.Serializer):
    """Standard distribution response format"""
    chart_data = ChartDataSerializer()
    total = serializers.IntegerField()


class CrossAnalysisResponseSerializer(serializers.Serializer):
    """Cross-analysis response format"""
    chart_data = CrossAnalysisChartSerializer()
    table_data = TableDataSerializer()
    total = serializers.IntegerField()