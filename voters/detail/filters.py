import django_filters
from .models import Voter
from voters.detail.utils.analytics import PROVINCE_MAPPING



class VoterAnalyticsFilter(django_filters.FilterSet):
    age_min = django_filters.NumberFilter(field_name='age', lookup_expr='gte')
    age_max = django_filters.NumberFilter(field_name='age', lookup_expr='lte')

    age_group = django_filters.CharFilter(field_name='age_group')
    gender = django_filters.CharFilter(field_name='gender')
    caste_group = django_filters.CharFilter(field_name='caste_group')

    province = django_filters.CharFilter(method='filter_province')
    district = django_filters.CharFilter(field_name='district')
    constituency = django_filters.CharFilter(field_name='constituency')
    ward = django_filters.NumberFilter(field_name='ward')

    search = django_filters.CharFilter(field_name='name', lookup_expr='icontains')

    class Meta:
        model = Voter
        fields = []

    def filter_province(self, queryset, name, value):
        mapped = PROVINCE_MAPPING.get(value.strip(), value.strip())
        return queryset.filter(province=mapped)