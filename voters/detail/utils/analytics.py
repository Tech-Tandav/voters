"""
Analytics Utility using Pandas

Professional data analysis for voter demographics.
Generates chart-ready data and statistical summaries.
"""

import pandas as pd
import logging
from django.db.models import Q
from voters.detail.models import Voter

logger = logging.getLogger(__name__)


class VoterAnalytics:
    """
    Perform demographic analysis on voter data using Pandas.
    Provides chart-ready JSON responses for frontend.
    """
    
    # Age group labels (user-friendly names)
    AGE_GROUP_LABELS = {
        'gen_z': 'Gen Z (18-29)',
        'working': 'Working & Family (30-45)',
        'mature': 'Mature (46-60)',
        'senior': 'Senior (60+)',
    }
    
    # Gender labels
    GENDER_LABELS = {
        'male': 'Male (पुरुष)',
        'female': 'Female (महिला)',
        'other': 'Other (अन्य)',
    }
    
    # Caste group labels
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
        """
        Initialize analytics with optional filtered queryset.
        
        Args:
            queryset: Django QuerySet of Voter objects (None = all voters)
        """
        self.queryset = queryset if queryset is not None else Voter.objects.all()
        self.df = None
    
    def _load_dataframe(self):
        """
        Load queryset data into Pandas DataFrame for analysis.
        Uses only necessary fields for performance.
        """
        if self.df is None:
            values = self.queryset.values(
                'voter_id', 'name', 'age', 'age_group', 
                'gender', 'caste_group', 'ward'
            )
            self.df = pd.DataFrame(list(values))
            logger.debug(f"Loaded {len(self.df)} records into DataFrame")
        return self.df
    
    def get_overview_stats(self):
        """
        Get overall statistics summary.
        
        Returns:
            dict: Overview statistics
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'total_voters': 0,
                'average_age': 0,
                'median_age': 0,
                'gender_distribution': {},
                'age_group_summary': {},
                'caste_summary': {},
            }
        
        # Gender distribution
        gender_counts = df['gender'].value_counts()
        total = len(df)
        
        gender_dist = {}
        for gender in ['male', 'female', 'other']:
            count = int(gender_counts.get(gender, 0))
            gender_dist[gender] = count
            gender_dist[f'{gender}_percentage'] = round(count / total * 100, 1) if total > 0 else 0
        
        # Age group summary
        age_group_counts = df['age_group'].value_counts()
        age_group_summary = {
            self.AGE_GROUP_LABELS[group]: int(age_group_counts.get(group, 0))
            for group in ['gen_z', 'working', 'mature', 'senior']
        }
        
        # Caste summary
        caste_counts = df['caste_group'].value_counts()
        caste_summary = {
            caste: int(caste_counts.get(caste, 0))
            for caste in ['brahmin', 'chhetri', 'janajati', 'dalit', 'madhesi', 'muslim', 'other', 'unknown']
        }
        
        return {
            'total_voters': int(total),
            'average_age': float(round(df['age'].mean(), 1)),
            'median_age': float(df['age'].median()),
            'gender_distribution': gender_dist,
            'age_group_summary': age_group_summary,
            'caste_summary': caste_summary,
        }
    
    def get_age_distribution(self):
        """
        Get age group distribution in chart-ready format.
        
        Returns:
            dict: Chart data with labels and values
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {
                    'labels': [],
                    'values': [],
                    'percentages': [],
                },
                'total': 0,
            }
        
        # Count by age group
        age_counts = df['age_group'].value_counts()
        total = len(df)
        
        # Maintain order: gen_z, working, mature, senior
        labels = []
        values = []
        percentages = []
        
        for group in ['gen_z', 'working', 'mature', 'senior']:
            count = int(age_counts.get(group, 0))
            labels.append(self.AGE_GROUP_LABELS[group])
            values.append(count)
            percentages.append(round(count / total * 100, 1) if total > 0 else 0)
        
        return {
            'chart_data': {
                'labels': labels,
                'values': values,
                'percentages': percentages,
            },
            'total': int(total),
        }
    
    def get_gender_distribution(self):
        """
        Get gender distribution in chart-ready format.
        
        Returns:
            dict: Chart data with labels and values
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {
                    'labels': [],
                    'values': [],
                    'percentages': [],
                },
                'total': 0,
            }
        
        gender_counts = df['gender'].value_counts()
        total = len(df)
        
        labels = []
        values = []
        percentages = []
        
        for gender in ['male', 'female', 'other']:
            count = int(gender_counts.get(gender, 0))
            if count > 0:  # Only include if there are voters
                labels.append(self.GENDER_LABELS[gender])
                values.append(count)
                percentages.append(round(count / total * 100, 1))
        
        return {
            'chart_data': {
                'labels': labels,
                'values': values,
                'percentages': percentages,
            },
            'total': int(total),
        }
    
    def get_caste_distribution(self):
        """
        Get caste group distribution in chart-ready format.
        
        Returns:
            dict: Chart data with labels and values
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {
                    'labels': [],
                    'values': [],
                    'percentages': [],
                },
                'total': 0,
            }
        
        caste_counts = df['caste_group'].value_counts()
        total = len(df)
        
        labels = []
        values = []
        percentages = []
        
        # Sort by count (descending) and include all with voters
        for caste, count in caste_counts.items():
            if count > 0:
                label = self.CASTE_LABELS.get(caste, caste)
                labels.append(label)
                values.append(int(count))
                percentages.append(round(count / total * 100, 1))
        
        return {
            'chart_data': {
                'labels': labels,
                'values': values,
                'percentages': percentages,
            },
            'total': int(total),
        }
    
    def get_age_gender_cross(self):
        """
        Get Age Group × Gender cross-analysis.
        Returns both chart data (for stacked bar charts) and table data.
        
        Returns:
            dict: Chart data and table data
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {'labels': [], 'datasets': []},
                'table_data': {'headers': [], 'rows': []},
                'total': 0,
            }
        
        # Create cross-tabulation
        crosstab = pd.crosstab(df['age_group'], df['gender'])
        
        # Prepare chart data (for stacked bar charts)
        age_groups = ['gen_z', 'working', 'mature', 'senior']
        labels = [self.AGE_GROUP_LABELS[g] for g in age_groups if g in crosstab.index]
        
        datasets = []
        for gender in ['male', 'female', 'other']:
            if gender in crosstab.columns:
                values = [int(crosstab.loc[g, gender]) if g in crosstab.index else 0 
                         for g in age_groups]
                datasets.append({
                    'label': self.GENDER_LABELS[gender],
                    'values': values
                })
        
        # Prepare table data
        headers = ['Age Group', 'Male', 'Female', 'Other', 'Total']
        rows = []
        
        for group in age_groups:
            if group in crosstab.index:
                male = int(crosstab.loc[group, 'male']) if 'male' in crosstab.columns else 0
                female = int(crosstab.loc[group, 'female']) if 'female' in crosstab.columns else 0
                other = int(crosstab.loc[group, 'other']) if 'other' in crosstab.columns else 0
                total = male + female + other
                
                rows.append([
                    self.AGE_GROUP_LABELS[group],
                    male,
                    female,
                    other,
                    total
                ])
        
        return {
            'chart_data': {
                'labels': labels,
                'datasets': datasets
            },
            'table_data': {
                'headers': headers,
                'rows': rows
            },
            'total': int(len(df)),
        }
    
    def get_age_caste_cross(self):
        """
        Get Age Group × Caste cross-analysis.
        Shows which caste groups dominate each age bracket.
        
        Returns:
            dict: Chart data and table data
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {'labels': [], 'datasets': []},
                'table_data': {'headers': [], 'rows': []},
                'total': 0,
            }
        
        # Create cross-tabulation
        crosstab = pd.crosstab(df['age_group'], df['caste_group'])
        
        # Get top caste groups (by total count)
        caste_totals = df['caste_group'].value_counts()
        top_castes = caste_totals.head(5).index.tolist()
        
        # Prepare chart data
        age_groups = ['gen_z', 'working', 'mature', 'senior']
        labels = [self.AGE_GROUP_LABELS[g] for g in age_groups if g in crosstab.index]
        
        datasets = []
        for caste in top_castes:
            if caste in crosstab.columns:
                values = [int(crosstab.loc[g, caste]) if g in crosstab.index else 0 
                         for g in age_groups]
                datasets.append({
                    'label': self.CASTE_LABELS.get(caste, caste),
                    'values': values
                })
        
        # Prepare table data
        headers = ['Age Group'] + [self.CASTE_LABELS.get(c, c) for c in top_castes] + ['Total']
        rows = []
        
        for group in age_groups:
            if group in crosstab.index:
                row = [self.AGE_GROUP_LABELS[group]]
                total = 0
                for caste in top_castes:
                    count = int(crosstab.loc[group, caste]) if caste in crosstab.columns else 0
                    row.append(count)
                    total += count
                row.append(total)
                rows.append(row)
        
        return {
            'chart_data': {
                'labels': labels,
                'datasets': datasets
            },
            'table_data': {
                'headers': headers,
                'rows': rows
            },
            'total': int(len(df)),
        }
    
    def get_gender_caste_cross(self):
        """
        Get Gender × Caste cross-analysis.
        Shows gender distribution within each caste group.
        
        Returns:
            dict: Chart data and table data
        """
        df = self._load_dataframe()
        
        if df.empty:
            return {
                'chart_data': {'labels': [], 'datasets': []},
                'table_data': {'headers': [], 'rows': []},
                'total': 0,
            }
        
        # Create cross-tabulation
        crosstab = pd.crosstab(df['caste_group'], df['gender'])
        
        # Get top caste groups
        caste_totals = df['caste_group'].value_counts()
        top_castes = caste_totals.head(6).index.tolist()
        
        # Prepare chart data
        labels = [self.CASTE_LABELS.get(c, c) for c in top_castes if c in crosstab.index]
        
        datasets = []
        for gender in ['male', 'female', 'other']:
            if gender in crosstab.columns:
                values = [int(crosstab.loc[c, gender]) if c in crosstab.index else 0 
                         for c in top_castes]
                datasets.append({
                    'label': self.GENDER_LABELS[gender],
                    'values': values
                })
        
        # Prepare table data
        headers = ['Caste Group', 'Male', 'Female', 'Other', 'Total']
        rows = []
        
        for caste in top_castes:
            if caste in crosstab.index:
                male = int(crosstab.loc[caste, 'male']) if 'male' in crosstab.columns else 0
                female = int(crosstab.loc[caste, 'female']) if 'female' in crosstab.columns else 0
                other = int(crosstab.loc[caste, 'other']) if 'other' in crosstab.columns else 0
                total = male + female + other
                
                rows.append([
                    self.CASTE_LABELS.get(caste, caste),
                    male,
                    female,
                    other,
                    total
                ])
        
        return {
            'chart_data': {
                'labels': labels,
                'datasets': datasets
            },
            'table_data': {
                'headers': headers,
                'rows': rows
            },
            'total': int(len(df)),
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