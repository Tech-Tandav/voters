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




import logging
logger = logging.getLogger(__name__)


import os
import json
import time
import pandas as pd
from django.db import transaction
from django.utils.timezone import now
from voters.detail.models import Voter, UploadHistory
from voters.detail.utils import extract_surname, normalize_surname, map_surname_to_caste
import logging

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

    # ---------- IO ----------
    @staticmethod
    def read_rows(csv_file):
        df = pd.read_csv(csv_file)
        return df.to_dict("records")

    # ---------- Validation ----------
    def validate_csv(self):
        try:
            self.df = pd.read_csv(self.csv_file)
            if self.df.empty:
                return False, "CSV file is empty"
            missing_columns = set(self.REQUIRED_COLUMNS) - set(self.df.columns)
            if missing_columns:
                return False, f"Missing columns: {', '.join(missing_columns)}"
            return True, None
        except Exception as e:
            logger.exception("CSV validation error")
            return False, str(e)

    # ---------- Orchestrator ----------
    def process(self):
        start_time = time.time()
        is_valid, error_msg = self.validate_csv()
        if not is_valid:
            return self._fail(error_msg)

        file_name = os.path.basename(self.csv_file)
        self.upload_history = UploadHistory.objects.create(
            file_name=file_name,
            uploaded_by=self.user,
            total_records=len(self.df),
            status='processing',
            started_at=now(),
        )

        rows = self.df.to_dict("records")
        total_imported, total_failed = 0, 0

        try:
            for batch in self._chunk(rows, self.BATCH_SIZE):
                imported, failed = self.process_batch(batch)
                total_imported += imported
                total_failed += failed

            processing_time = time.time() - start_time
            self.upload_history.success_count = total_imported
            self.upload_history.error_count = total_failed
            self.upload_history.status = 'completed'
            self.upload_history.processing_time = processing_time
            self.upload_history.unmapped_surnames = json.dumps(list(self.unmapped_surnames))
            self.upload_history.save()

            return {
                'success': True,
                'total': len(rows),
                'imported': total_imported,
                'failed': total_failed,
                'unmapped_surnames': list(self.unmapped_surnames),
                'processing_time': processing_time,
            }

        except Exception as e:
            return self._fail(str(e))

    # ---------- Batch Processor ----------
    def process_batch(self, rows):
        voters_to_create, voters_to_update, failed = [], {}, 0
        voter_ids = [int(r['VoterID']) for r in rows]
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

        return len(voters_to_create) + len(voters_to_update), failed

    # ---------- Row Builder ----------
    def _build_voter(self, row, existing=None):
        name = str(row['Name']).strip()
        age = int(row['Age'])
        gender_raw = str(row['Gender']).strip()
        surname = extract_surname(name)
        normalized_surname = normalize_surname(surname)
        caste_group = map_surname_to_caste(normalized_surname)
        if caste_group == 'unknown':
            self.unmapped_surnames.add(surname)
        gender = self.GENDER_MAPPING.get(gender_raw, 'other')
        spouse = None if pd.isna(row.get('Spouse')) or row.get('Spouse') == '-' else str(row.get('Spouse')).strip()
        parent = None if pd.isna(row.get('Parent')) else str(row.get('Parent')).strip()
        province_val = getattr(self, 'province_override', str(row['Province']))
        constituency_val = getattr(self, 'constituency_override', None)
        voter = existing or Voter(voter_id=int(row['VoterID']))
        voter.name, voter.surname, voter.age = name, surname, age
        voter.gender, voter.caste_group = gender, caste_group
        voter.province, voter.constituency = province_val, constituency_val
        voter.district, voter.municipality, voter.ward = str(row['District']), str(row['Municipality']), int(row['Ward'])
        voter.center, voter.spouse, voter.parent = str(row['Center']), spouse, parent
        return voter

    # ---------- Helpers ----------
    def _chunk(self, rows, size):
        for i in range(0, len(rows), size):
            yield rows[i:i + size]

    def _fail(self, error_msg):
        if self.upload_history:
            self.upload_history.status = 'failed'
            self.upload_history.error_log = error_msg
            self.upload_history.save()
        logger.error(f"CSV processing failed: {error_msg}")
        return {'success': False, 'error': error_msg, 'total': 0, 'imported': 0, 'failed': 0}
    
    
    
def process_csv_file(csv_file, user=None): 
    processor = CSVProcessor(csv_file, user) 
    return processor.process()