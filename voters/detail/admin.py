"""
Django Admin Configuration

Custom admin interfaces for managing voters, surname mappings, and upload history.
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from voters.detail.models import Voter, SurnameMapping, UploadHistory


@admin.register(Voter)
class VoterAdmin(admin.ModelAdmin):
    """
    Admin interface for Voter model.
    Allows viewing, searching, and filtering voter records.
    """
    
    list_display = [
        'voter_id',
        'name',
        'age',
        'age_group_badge',
        'gender_badge',
        'caste_group',
        'ward',
    ]
    
    list_filter = [
        'age_group',
        'gender',
        'caste_group',
        'ward',
        'district',
    ]
    
    search_fields = [
        'voter_id',
        'name',
        'surname',
    ]
    
    readonly_fields = [
        'voter_id',
        'surname',
        'age_group',
        'created_at',
        'updated_at',
    ]
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('voter_id', 'name', 'surname', 'age', 'age_group', 'gender')
        }),
        ('Classification', {
            'fields': ('caste_group',)
        }),
        ('Location', {
            'fields': ('province', 'district', 'municipality', 'ward', 'center')
        }),
        ('Family', {
            'fields': ('spouse', 'parent'),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    list_per_page = 50
    
    def age_group_badge(self, obj):
        """Display age group with color badge"""
        colors = {
            'gen_z': '#22c55e',      # Green
            'working': '#3b82f6',    # Blue
            'mature': '#f97316',     # Orange
            'senior': '#6366f1',     # Purple
        }
        color = colors.get(obj.age_group, '#gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; '
            'border-radius: 3px; font-size: 12px;">{}</span>',
            color,
            obj.get_age_group_display()
        )
    age_group_badge.short_description = 'Age Group'
    
    def gender_badge(self, obj):
        """Display gender with icon"""
        icons = {
            'male': '👨',
            'female': '👩',
            'other': '🧑',
        }
        icon = icons.get(obj.gender, '')
        return format_html('{} {}', icon, obj.get_gender_display())
    gender_badge.short_description = 'Gender'
    
    def has_add_permission(self, request):
        """Disable manual addition - use CSV upload instead"""
        return False


@admin.register(SurnameMapping)
class SurnameMappingAdmin(admin.ModelAdmin):
    """
    Admin interface for SurnameMapping model.
    Allows managing surname-to-caste mappings.
    """
    
    list_display = [
        'surname',
        'caste_group_badge',
        'is_active',
        'voter_count',
        'updated_at',
    ]
    
    list_filter = [
        'caste_group',
        'is_active',
    ]
    
    search_fields = [
        'surname',
        'notes',
    ]
    
    list_editable = ['is_active']
    
    fieldsets = (
        (None, {
            'fields': ('surname', 'caste_group', 'is_active')
        }),
        ('Additional Information', {
            'fields': ('notes',),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at', 'updated_at']
    
    list_per_page = 100
    
    def caste_group_badge(self, obj):
        """Display caste group with color"""
        colors = {
            'brahmin': '#8b5cf6',    # Purple
            'chhetri': '#3b82f6',    # Blue
            'janajati': '#10b981',   # Green
            'dalit': '#f59e0b',      # Amber
            'madhesi': '#ef4444',    # Red
            'muslim': '#06b6d4',     # Cyan
            'other': '#6b7280',      # Gray
            'unknown': '#9ca3af',    # Light gray
        }
        color = colors.get(obj.caste_group, '#gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; '
            'border-radius: 3px; font-size: 12px;">{}</span>',
            color,
            obj.get_caste_group_display()
        )
    caste_group_badge.short_description = 'Caste Group'
    
    def voter_count(self, obj):
        """Show how many voters have this surname"""
        count = Voter.objects.filter(surname=obj.surname).count()
        if count > 0:
            url = reverse('admin:voters_voter_changelist') + f'?surname={obj.surname}'
            return format_html('<a href="{}">{} voters</a>', url, count)
        return '0 voters'
    voter_count.short_description = 'Voters with Surname'


@admin.register(UploadHistory)
class UploadHistoryAdmin(admin.ModelAdmin):
    """
    Admin interface for UploadHistory model.
    Shows history of CSV uploads and their processing status.
    """
    
    list_display = [
        'file_name',
        'uploaded_by',
        'upload_date',
        'status_badge',
        'total_records',
        'success_rate',
        'processing_time_display',
    ]
    
    list_filter = [
        'status',
        'upload_date',
    ]
    
    search_fields = [
        'file_name',
        'uploaded_by__username',
    ]
    
    readonly_fields = [
        'file_name',
        'uploaded_by',
        'upload_date',
        'total_records',
        'success_count',
        'error_count',
        'status',
        'error_log_display',
        'unmapped_surnames_display',
        'processing_time',
    ]
    
    fieldsets = (
        ('Upload Information', {
            'fields': ('file_name', 'uploaded_by', 'upload_date', 'status')
        }),
        ('Processing Results', {
            'fields': ('total_records', 'success_count', 'error_count', 'processing_time')
        }),
        ('Error Details', {
            'fields': ('error_log_display', 'unmapped_surnames_display'),
            'classes': ('collapse',)
        }),
    )
    
    date_hierarchy = 'upload_date'
    
    def has_add_permission(self, request):
        """Disable manual addition"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """Disable editing"""
        return False
    
    def status_badge(self, obj):
        """Display status with color"""
        colors = {
            'pending': '#f59e0b',       # Amber
            'processing': '#3b82f6',    # Blue
            'completed': '#22c55e',     # Green
            'failed': '#ef4444',        # Red
        }
        color = colors.get(obj.status, '#gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; '
            'border-radius: 3px; font-size: 12px; font-weight: bold;">{}</span>',
            color,
            obj.status.upper()
        )
    status_badge.short_description = 'Status'
    
    def success_rate(self, obj):
        """Calculate and display success rate"""
        if obj.total_records > 0:
            rate = (obj.success_count / obj.total_records) * 100
            color = '#22c55e' if rate >= 95 else '#f59e0b' if rate >= 80 else '#ef4444'
            return format_html(
                '<span style="color: {}; font-weight: bold;">{}%</span>',
                color,
                f"{rate:.1f}"
            )
        return '0%'
    success_rate.short_description = 'Success Rate'
    
    def processing_time_display(self, obj):
        """Display processing time in readable format"""
        if obj.processing_time:
            return f"{obj.processing_time:.2f}s"
        return '-'
    processing_time_display.short_description = 'Processing Time'
    
    def error_log_display(self, obj):
        """Display error log with formatting"""
        if obj.error_log:
            return format_html(
                '<pre style="background: #f3f4f6; padding: 10px; '
                'border-radius: 5px; max-height: 400px; overflow-y: auto;">{}</pre>',
                obj.error_log
            )
        return '-'
    error_log_display.short_description = 'Error Log'
    
    def unmapped_surnames_display(self, obj):
        """Display unmapped surnames"""
        if obj.unmapped_surnames:
            import json
            try:
                surnames = json.loads(obj.unmapped_surnames)
                if surnames:
                    return format_html(
                        '<div style="background: #fef3c7; padding: 10px; '
                        'border-radius: 5px;"><strong>⚠️ {} unmapped surnames:</strong><br>{}</div>',
                        len(surnames),
                        ', '.join(surnames[:50])  # Show first 50
                    )
            except:
                return obj.unmapped_surnames
        return 'All surnames mapped'
    unmapped_surnames_display.short_description = 'Unmapped Surnames'


# Customize admin site header
admin.site.site_header = "Voter Analysis Administration"
admin.site.site_title = "Voter Analysis Admin"
admin.site.index_title = "Manage Voter Data"