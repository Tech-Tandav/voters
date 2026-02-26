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


class CSVProcessor:
    """
    Process CSV files containing voter data.
    Optimized for large-scale imports with batching + bulk operations.
    """

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
                return False, f"Missing required columns: {', '.join(missing_columns)}"

            if not pd.api.types.is_numeric_dtype(self.df['Age']):
                return False, "Age column must contain numeric values"

            if not pd.api.types.is_numeric_dtype(self.df['VoterID']):
                return False, "VoterID column must contain numeric values"

            logger.info(f"CSV validation passed: {len(self.df)} rows found")
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

        total_imported = 0
        total_failed = 0

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

            logger.info(
                f"CSV processing completed: {total_imported} imported, "
                f"{total_failed} failed in {processing_time:.2f}s"
            )

            return {
                'success': True,
                'total': len(rows),
                'imported': total_imported,
                'failed': total_failed,
                'unmapped_surnames': list(self.unmapped_surnames),
                'processing_time': processing_time,
            }

        except SoftTimeLimitExceeded:
            logger.warning("Soft time limit exceeded during CSV import")
            raise

        except Exception as e:
            return self._fail(str(e))

    # ---------- Batch Processor ----------

    def process_batch(self, rows):
        voters_to_create = []
        voters_to_update = {}
        failed = 0

        voter_ids = [int(r['VoterID']) for r in rows]
        existing = {
            v.voter_id: v
            for v in Voter.objects.filter(voter_id__in=voter_ids)
        }

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
        return {
            'success': False,
            'error': error_msg,
            'total': 0,
            'imported': 0,
            'failed': 0,
        }


def process_csv_file(csv_file, user=None):
    processor = CSVProcessor(csv_file, user)
    return processor.process()