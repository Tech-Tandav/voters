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
from voters.detail.models import Voter, UploadHistory
from voters.detail.utils import extract_surname, normalize_surname, map_surname_to_caste
import json
import os

from django.db import transaction
from django.utils.timezone import now

from celery.exceptions import SoftTimeLimitExceeded
logger = logging.getLogger(__name__)






import os
import json
import pandas as pd
import logging
from django.db import transaction
from django.utils.timezone import now
from voters.detail.models import Voter, UploadHistory

logger = logging.getLogger(__name__)

class CSVProcessor:
    REQUIRED_COLUMNS = [
        'Province', 'District', 'Municipality', 'Ward', 'Center',
        'VoterID', 'Name', 'Age', 'Gender', 'Spouse', 'Parent'
    ]

    GENDER_MAPPING = {
        'पुरुष': 'male',
        'महिला': 'female',
        'अन्य': 'other',
        'male': 'male',
        'female': 'female',
        'other': 'other',
    }

    BATCH_SIZE = 1000

    def __init__(self, csv_file, user=None, rows=None):
        self.csv_file = csv_file
        self.rows = rows
        self.user = user
        self.df = None
        self.upload_history = None
        self.errors = []
        self.unmapped_surnames = set()

    @staticmethod
    def read_rows(csv_file):
        df = pd.read_csv(csv_file)
        return df.to_dict("records")

    def process_batch(self, rows):
        voters_to_create = []
        voters_to_update = {}
        failed = 0

        voter_ids = [int(r['VoterID']) for r in rows if r.get('VoterID')]
        existing = {v.voter_id: v for v in Voter.objects.filter(voter_id__in=voter_ids)}

        for row in rows:
            try:
                voter = self._build_voter(row, existing.get(int(row['VoterID'])))
                if voter.pk:
                    voters_to_update[voter.voter_id] = voter
                else:
                    voters_to_create.append(voter)
            except Exception as e:
                failed += 1
                self.errors.append(str(e))
                logger.exception(f"Row failed: {row}, error: {e}")

        try:
            with transaction.atomic():
                if voters_to_create:
                    Voter.objects.bulk_create(voters_to_create, batch_size=1000)
                if voters_to_update:
                    Voter.objects.bulk_update(
                        voters_to_update.values(),
                        fields=[
                            'name', 'surname', 'age', 'gender', 'caste_group',
                            'province', 'district', 'municipality', 'ward',
                            'constituency', 'center', 'spouse', 'parent'
                        ],
                        batch_size=1000
                    )
        except Exception as e:
            logger.exception(f"DB insert failed for batch: {e}")
            raise

        return len(voters_to_create) + len(voters_to_update), failed

    def _build_voter(self, row, existing=None):
        from voters.detail.utils import extract_surname, normalize_surname, map_surname_to_caste

        name = str(row['Name']).strip()
        age = int(row['Age'])
        gender_raw = str(row['Gender']).strip()

        surname = extract_surname(name)
        normalized_surname = normalize_surname(surname)
        caste_group = map_surname_to_caste(normalized_surname)

        if caste_group == 'unknown':
            self.unmapped_surnames.add(surname)

        gender = self.GENDER_MAPPING.get(gender_raw, 'other')

        spouse = row.get('Spouse')
        spouse = None if pd.isna(spouse) or spouse == '-' else str(spouse).strip()

        parent = row.get('Parent')
        parent = None if pd.isna(parent) else str(parent).strip()

        province_val = getattr(self, 'province_override', str(row['Province']))
        constituency_val = getattr(self, 'constituency_override', None)

        voter = existing or Voter(voter_id=int(row['VoterID']))
        voter.name = name
        voter.surname = surname
        voter.age = age
        voter.gender = gender
        voter.caste_group = caste_group
        voter.province = province_val
        voter.district = str(row['District'])
        voter.municipality = str(row['Municipality'])
        voter.ward = int(row['Ward'])
        voter.constituency = constituency_val
        voter.center = str(row['Center'])
        voter.spouse = spouse
        voter.parent = parent

        return voter
    
    
    
def process_csv_file(csv_file, user=None): 
    processor = CSVProcessor(csv_file, user) 
    return processor.process()