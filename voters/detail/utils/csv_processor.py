"""
CSV Processor for Voter Data

Handles importing voter data from CSV files into the database.
Includes validation, error handling, and progress tracking.
"""

import pandas as pd
import logging
import time
from django.db import transaction
from django.core.exceptions import ValidationError
from voters.models import Voter, UploadHistory
from voters.utils import extract_surname, normalize_surname, map_surname_to_caste
import json
import os

logger = logging.getLogger(__name__)


class CSVProcessor:
    """
    Process CSV files containing voter data.
    Validates, transforms, and imports data into database.
    """
    
    # Expected CSV columns (from your dataset)
    REQUIRED_COLUMNS = [
        'Province', 'District', 'Municipality', 'Ward', 'Center',
        'VoterID', 'Name', 'Age', 'Gender', 'Spouse', 'Parent'
    ]
    
    # Gender mapping (Nepali to English)
    GENDER_MAPPING = {
        'पुरुष': 'male',
        'महिला': 'female',
        'अन्य': 'other',
        'male': 'male',
        'female': 'female',
        'other': 'other',
    }
    
    def __init__(self, csv_file, user=None):
        """
        Initialize CSV processor.
        
        Args:
            csv_file: File object or path to CSV
            user: Django User object (for tracking who uploaded)
        """
        self.csv_file = csv_file
        self.user = user
        self.df = None
        self.upload_history = None
        self.errors = []
        self.unmapped_surnames = set()
    
    def validate_csv(self):
        """
        Validate CSV file structure and content.
        
        Returns:
            tuple: (is_valid: bool, error_message: str or None)
        """
        try:
            # Read CSV
            self.df = pd.read_csv(self.csv_file)
            
            # Check if empty
            if self.df.empty:
                return False, "CSV file is empty"
            
            # Check required columns
            missing_columns = set(self.REQUIRED_COLUMNS) - set(self.df.columns)
            if missing_columns:
                return False, f"Missing required columns: {', '.join(missing_columns)}"
            
            # Check data types
            if not pd.api.types.is_numeric_dtype(self.df['Age']):
                return False, "Age column must contain numeric values"
            
            if not pd.api.types.is_numeric_dtype(self.df['VoterID']):
                return False, "VoterID column must contain numeric values"
            
            # Check for required data
            if self.df['Name'].isnull().any():
                return False, "Name column contains empty values"
            
            if self.df['Age'].isnull().any():
                return False, "Age column contains empty values"
            
            if self.df['Gender'].isnull().any():
                return False, "Gender column contains empty values"
            
            logger.info(f"CSV validation passed: {len(self.df)} rows found")
            return True, None
        
        except Exception as e:
            logger.error(f"CSV validation error: {e}")
            return False, str(e)
    
    def process(self):
        """
        Process CSV file and import data into database.
        
        Returns:
            dict: Processing results with statistics
        """
        start_time = time.time()
        
        # Validate first
        is_valid, error_msg = self.validate_csv()
        if not is_valid:
            return {
                'success': False,
                'error': error_msg,
                'total': 0,
                'imported': 0,
                'failed': 0,
            }
        
        # Create upload history record
        if isinstance(self.csv_file, str):
            file_name = os.path.basename(self.csv_file)
        else:
            file_name = getattr(self.csv_file, 'name', 'unknown.csv')
        self.upload_history = UploadHistory.objects.create(
            file_name=file_name,
            uploaded_by=self.user,
            total_records=len(self.df),
            status='processing'
        )
        
        # Process records
        imported_count = 0
        error_count = 0
        
        try:
            with transaction.atomic():
                for index, row in self.df.iterrows():
                    try:
                        self._process_row(row)
                        imported_count += 1
                    except Exception as e:
                        error_count += 1
                        error_msg = f"Row {index + 2}: {str(e)}"
                        self.errors.append(error_msg)
                        logger.warning(error_msg)
            
            # Update upload history
            processing_time = time.time() - start_time
            self.upload_history.success_count = imported_count
            self.upload_history.error_count = error_count
            self.upload_history.status = 'completed'
            self.upload_history.processing_time = processing_time
            self.upload_history.error_log = '\n'.join(self.errors) if self.errors else None
            self.upload_history.unmapped_surnames = json.dumps(list(self.unmapped_surnames))
            self.upload_history.save()
            
            logger.info(
                f"CSV processing completed: {imported_count} imported, "
                f"{error_count} failed in {processing_time:.2f}s"
            )
            
            return {
                'success': True,
                'total': len(self.df),
                'imported': imported_count,
                'failed': error_count,
                'unmapped_surnames': list(self.unmapped_surnames),
                'processing_time': processing_time,
                'errors': self.errors,
            }
        
        except Exception as e:
            # Mark as failed
            self.upload_history.status = 'failed'
            self.upload_history.error_log = str(e)
            self.upload_history.save()
            
            logger.error(f"CSV processing failed: {e}")
            
            return {
                'success': False,
                'error': str(e),
                'total': len(self.df),
                'imported': imported_count,
                'failed': error_count,
            }
    
    def _process_row(self, row):
        """
        Process a single CSV row and create Voter object.
        
        Args:
            row: Pandas Series representing one row
        """
        # Extract and clean data
        name = str(row['Name']).strip()
        age = int(row['Age'])
        gender_nepali = str(row['Gender']).strip()
        
        # Extract surname
        surname = extract_surname(name)
        normalized_surname = normalize_surname(surname)
        
        # Map to caste group
        caste_group = map_surname_to_caste(normalized_surname)
        
        # Track unmapped surnames
        if caste_group == 'unknown':
            self.unmapped_surnames.add(surname)
        
        # Map gender
        gender = self.GENDER_MAPPING.get(gender_nepali, 'other')
        
        # Handle nullable fields
        spouse = row.get('Spouse')
        if pd.isna(spouse) or spouse == '-':
            spouse = None
        
        parent = row.get('Parent')
        if pd.isna(parent):
            parent = None
        
        if hasattr(self, 'province_override'):
             province_val = self.province_override
        else:
             province_val = str(row['Province'])
             
        if hasattr(self, 'constituency_override'):
             constituency_val = self.constituency_override
        else:
             # Default fallback if no constituency provided and not in CSV
             constituency_val = None

        # Create or update voter
        Voter.objects.update_or_create(
            voter_id=int(row['VoterID']),
            defaults={
                'name': name,
                'surname': surname,
                'age': age,
                'gender': gender,
                'caste_group': caste_group,
                'province': province_val,
                'district': str(row['District']),
                'municipality': str(row['Municipality']),
                'ward': int(row['Ward']),
                'constituency': constituency_val,  # New field
                'center': str(row['Center']),
                'spouse': spouse,
                'parent': parent,
            }
        )


def process_csv_file(csv_file, user=None):
    """
    Convenience function to process a CSV file.
    
    Args:
        csv_file: File object or path
        user: Django User object
    
    Returns:
        dict: Processing results
    """
    processor = CSVProcessor(csv_file, user)
    return processor.process()