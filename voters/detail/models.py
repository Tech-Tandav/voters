"""
Database Models for Voter Analysis System

This module defines three main models:
1. Voter - Individual voter records with demographic data
2. SurnameMapping - Mapping between surnames and caste groups
3. UploadHistory - Track CSV upload history and status
"""

from django.db import models
from django.conf import settings
from django.core.validators import MinValueValidator, MaxValueValidator
from voters.core.models import BaseModel


class SurnameMapping(BaseModel):
    """
    Maps Nepali surnames to caste/ethnic groups.
    This is editable via admin panel and used for automatic caste classification.
    """
    
    # Caste group choices
    CASTE_CHOICES = [
        ('brahmin', 'Brahmin (ब्राह्मण)'),
        ('chhetri', 'Chhetri (क्षेत्री)'),
        ('janajati', 'Janajati (जनजाति)'),
        ('dalit', 'Dalit (दलित)'),
        ('madhesi', 'Madhesi/Tharu (मधेसी/थारू)'),
        ('muslim', 'Muslim (मुस्लिम)'),
        ('other', 'Other (अन्य)'),
        ('unknown', 'Unknown (अज्ञात)'),
    ]
    
    surname = models.CharField(
        max_length=100,
        unique=True,
        db_index=True,
        help_text="Nepali surname (e.g., के.सी., थापा, मगर)"
    )
    
    caste_group = models.CharField(
        max_length=50,
        choices=CASTE_CHOICES,
        db_index=True,
        help_text="Caste/ethnic group this surname belongs to"
    )
    
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this mapping is currently in use"
    )
    
    notes = models.TextField(
        blank=True,
        null=True,
        help_text="Notes about ambiguous cases or variations"
    )
    

    
    class Meta:
        ordering = ['surname']
        verbose_name = "Surname Mapping"
        verbose_name_plural = "Surname Mappings"
    
    def __str__(self):
        return f"{self.surname} → {self.get_caste_group_display()}"


class Voter(BaseModel):
    """
    Individual voter record with demographic information.
    One row from CSV = One Voter instance.
    """
    
    # Age group choices (based on your requirement)
    AGE_GROUP_CHOICES = [
        ('gen_z', 'Gen Z / Young Voters (18-29)'),
        ('working', 'Working & Family (30-45)'),
        ('mature', 'Mature / Politically Active (46-60)'),
        ('senior', 'Senior Voters (60+)'),
    ]
    
    # Gender choices (Nepali format from CSV)
    GENDER_CHOICES = [
        ('male', 'पुरुष (Male)'),
        ('female', 'महिला (Female)'),
        ('other', 'अन्य (Other)'),
    ]
    
    # Voter ID from CSV (unique identifier)
    voter_id = models.BigIntegerField(
        unique=True,
        db_index=True,
        help_text="Unique voter ID from election commission"
    )
    
    # Basic Information
    name = models.CharField(
        max_length=200,
        help_text="Full name in Nepali"
    )
    
    surname = models.CharField(
        max_length=100,
        db_index=True,
        help_text="Extracted surname (last word from name)"
    )
    
    age = models.IntegerField(
        validators=[MinValueValidator(18), MaxValueValidator(150)],
        db_index=True,
        help_text="Age in years"
    )
    
    age_group = models.CharField(
        max_length=50,
        choices=AGE_GROUP_CHOICES,
        db_index=True,
        help_text="Categorized age group"
    )
    
    gender = models.CharField(
        max_length=20,
        choices=GENDER_CHOICES,
        db_index=True,
        help_text="Gender"
    )
    
    # Derived field from surname mapping
    caste_group = models.CharField(
        max_length=50,
        db_index=True,
        blank=True,
        null=True,
        help_text="Caste group derived from surname"
    )
    
    # Location Information
    province = models.CharField(max_length=100)
    district = models.CharField(max_length=100)
    constituency = models.CharField(
        max_length=100, 
        db_index=True, 
        null=True, 
        blank=True,
        help_text="Constituency/Area name (derived from filename)"
    )
    municipality = models.CharField(max_length=100)
    ward = models.IntegerField(db_index=True)
    center = models.CharField(max_length=200)
    
    # Family Information
    spouse = models.CharField(
        max_length=200,
        blank=True,
        null=True,
        help_text="Spouse name"
    )
    
    parent = models.CharField(
        max_length=200,
        blank=True,
        null=True,
        help_text="Parent names"
    )
    

    
    class Meta:
        ordering = ['name']
        verbose_name = "Voter"
        verbose_name_plural = "Voters"
        
        # Composite index for common filter combinations
        indexes = [
            models.Index(fields=['age_group', 'gender']),
            models.Index(fields=['age_group', 'caste_group']),
            models.Index(fields=['gender', 'caste_group']),
            models.Index(fields=['ward', 'age_group']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.voter_id})"
    
    def save(self, *args, **kwargs):
        """
        Override save to automatically set age_group before saving.
        """
        # Categorize age group
        if 18 <= self.age <= 29:
            self.age_group = 'gen_z'
        elif 30 <= self.age <= 45:
            self.age_group = 'working'
        elif 46 <= self.age <= 60:
            self.age_group = 'mature'
        else:
            self.age_group = 'senior'
        
        super().save(*args, **kwargs)


class UploadHistory(BaseModel):
    """
    Track CSV upload history and processing status.
    Useful for debugging and audit trail.
    """
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    file_name = models.CharField(max_length=255)
    
    uploaded_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="Admin user who uploaded the file"
    )
    
    upload_date = models.DateTimeField(auto_now_add=True)
    
    total_records = models.IntegerField(
        default=0,
        help_text="Total rows in CSV"
    )
    
    success_count = models.IntegerField(
        default=0,
        help_text="Successfully imported records"
    )
    
    error_count = models.IntegerField(
        default=0,
        help_text="Failed records"
    )
    
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending'
    )
    
    error_log = models.TextField(
        blank=True,
        null=True,
        help_text="Detailed error messages"
    )
    
    unmapped_surnames = models.TextField(
        blank=True,
        null=True,
        help_text="Surnames not found in mapping (JSON format)"
    )
    
    processing_time = models.FloatField(
        null=True,
        blank=True,
        help_text="Processing time in seconds"
    )
    
    class Meta:
        ordering = ['-upload_date']
        verbose_name = "Upload History"
        verbose_name_plural = "Upload Histories"
    
    def __str__(self):
        return f"{self.file_name} - {self.status} ({self.upload_date.strftime('%Y-%m-%d %H:%M')})"