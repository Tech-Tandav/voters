"""
Analytics Utility using Pandas

Professional data analysis for voter demographics.
Generates chart-ready data and statistical summaries.
"""

import pandas as pd
import logging
from django.db.models import Q, Count
from voters.detail.models import Voter

logger = logging.getLogger(__name__)


from django.db.models import Count, Avg
from voters.detail.models import Voter


class VoterAnalytics:
    """
    Perform demographic analysis using pure SQL aggregations.
    Optimized for very large datasets (25M+ rows).
    """

    AGE_GROUP_LABELS = {
        'gen_z': 'Gen Z (18-29)',
        'working': 'Working & Family (30-45)',
        'mature': 'Mature (46-60)',
        'senior': 'Senior (60+)',
    }

    GENDER_LABELS = {
        'male': 'Male (पुरुष)',
        'female': 'Female (महिला)',
        'other': 'Other (अन्य)',
    }

    CASTE_LABELS = {
        'brahmin': 'Brahmin (ब्राह्मण)',
        'chhetri': 'Chhetri (क्षेत्री)',
        'janajati': 'Janajati (जनजाति)',
        'dalit': 'Dalit (दलित)',
        'madhesi': 'Madhesi/Tharu (मधेसी/थारू)',
        'muslim': 'Muslim (मुस्लिम)',
        'other': 'Other (अन्य)',
        'unknown': 'Unknown (अज्ञात)',
    }

    def __init__(self, queryset=None):
        self.queryset = queryset if queryset is not None else Voter.objects.all()

    # ---------------- OVERVIEW ---------------- #

    def get_overview_stats(self):
        total = self.queryset.count()

        if total == 0:
            return {
                'total_voters': 0,
                'average_age': 0,
                'median_age': 0,
                'gender_distribution': {},
                'age_group_summary': {},
                'caste_summary': {},
            }

        avg_age = self.queryset.aggregate(avg=Avg('age'))['avg'] or 0

        gender_qs = self.queryset.values('gender').annotate(total=Count('id'))
        gender_dist = {}
        for row in gender_qs:
            count = row['total']
            gender = row['gender']
            gender_dist[gender] = count
            gender_dist[f"{gender}_percentage"] = round(count / total * 100, 1)

        age_group_qs = self.queryset.values('age_group').annotate(total=Count('id'))
        age_group_summary = {
            self.AGE_GROUP_LABELS.get(row['age_group'], row['age_group']): row['total']
            for row in age_group_qs
        }

        caste_qs = self.queryset.values('caste_group').annotate(total=Count('id'))
        caste_summary = {
            row['caste_group']: row['total']
            for row in caste_qs
        }

        return {
            'total_voters': total,
            'average_age': round(avg_age, 1),
            'median_age': 0,  # median at DB level is expensive; can add later if needed
            'gender_distribution': gender_dist,
            'age_group_summary': age_group_summary,
            'caste_summary': caste_summary,
        }

    # ---------------- DISTRIBUTIONS ---------------- #

    def get_age_distribution(self):
        qs = self.queryset.values('age_group').annotate(total=Count('id'))
        total = self.queryset.count()

        labels, values, percentages = [], [], []

        for key in ['gen_z', 'working', 'mature', 'senior']:
            count = next((x['total'] for x in qs if x['age_group'] == key), 0)
            labels.append(self.AGE_GROUP_LABELS[key])
            values.append(count)
            percentages.append(round(count / total * 100, 1) if total else 0)

        return {
            'chart_data': {'labels': labels, 'values': values, 'percentages': percentages},
            'total': total,
        }

    def get_gender_distribution(self):
        qs = self.queryset.values('gender').annotate(total=Count('id'))
        total = self.queryset.count()

        labels, values, percentages = [], [], []

        for row in qs:
            label = self.GENDER_LABELS.get(row['gender'], row['gender'])
            count = row['total']
            labels.append(label)
            values.append(count)
            percentages.append(round(count / total * 100, 1))

        return {
            'chart_data': {'labels': labels, 'values': values, 'percentages': percentages},
            'total': total,
        }

    def get_caste_distribution(self):
        qs = self.queryset.values('caste_group').annotate(total=Count('id')).order_by('-total')
        total = self.queryset.count()

        labels, values, percentages = [], [], []

        for row in qs:
            label = self.CASTE_LABELS.get(row['caste_group'], row['caste_group'])
            count = row['total']
            labels.append(label)
            values.append(count)
            percentages.append(round(count / total * 100, 1))

        return {
            'chart_data': {'labels': labels, 'values': values, 'percentages': percentages},
            'total': total,
        }

    # ---------------- CROSSTABS ---------------- #

    def get_age_gender_cross(self):
        qs = self.queryset.values('age_group', 'gender').annotate(total=Count('id'))

        age_groups = ['gen_z', 'working', 'mature', 'senior']
        genders = ['male', 'female', 'other']

        matrix = {ag: {g: 0 for g in genders} for ag in age_groups}

        for row in qs:
            matrix[row['age_group']][row['gender']] = row['total']

        datasets = [
            {
                'label': self.GENDER_LABELS[g],
                'values': [matrix[ag][g] for ag in age_groups]
            }
            for g in genders
        ]

        return {
            'chart_data': {
                'labels': [self.AGE_GROUP_LABELS[ag] for ag in age_groups],
                'datasets': datasets
            },
            'total': self.queryset.count(),
        }

    def get_gender_caste_cross(self):
        qs = self.queryset.values('caste_group', 'gender').annotate(total=Count('id'))

        top_castes = (
            self.queryset.values('caste_group')
            .annotate(total=Count('id'))
            .order_by('-total')[:6]
        )

        castes = [c['caste_group'] for c in top_castes]
        genders = ['male', 'female', 'other']

        matrix = {c: {g: 0 for g in genders} for c in castes}

        for row in qs:
            if row['caste_group'] in matrix:
                matrix[row['caste_group']][row['gender']] = row['total']

        datasets = [
            {
                'label': self.GENDER_LABELS[g],
                'values': [matrix[c][g] for c in castes]
            }
            for g in genders
        ]

        return {
            'chart_data': {
                'labels': [self.CASTE_LABELS.get(c, c) for c in castes],
                'datasets': datasets
            },
            'total': self.queryset.count(),
        }

# Convenience functions for API views
def get_analytics(queryset=None):
    """
    Get VoterAnalytics instance for a queryset.
    
    Args:
        queryset: Optional filtered queryset
    
    Returns:
        VoterAnalytics instance
    """
    return VoterAnalytics(queryset)

