from celery import shared_task
from django.contrib.auth import get_user_model
from voters.detail.utils.csv_processor import CSVProcessor
from celery.exceptions import SoftTimeLimitExceeded


import os
import json
import time
import pandas as pd
from celery import shared_task, group
from celery.exceptions import SoftTimeLimitExceeded
from django.db import transaction
from django.utils.timezone import now
from django.contrib.auth import get_user_model
from voters.detail.models import Voter, UploadHistory
from voters.detail.utils import extract_surname, normalize_surname, map_surname_to_caste
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def import_voters_csv(self, file_path, province, constituency, user_id=None):
    """
    Orchestrator task: reads CSV and schedules chunk tasks.
    This task finishes quickly and never touches the DB.
    """
    rows = CSVProcessor.read_rows(file_path)
    BATCH_SIZE = 1000
    jobs = []

    for batch in (rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)):
        jobs.append(
            import_voters_csv_chunk.s(
                rows=batch,
                province=province,
                constituency=constituency,
                user_id=user_id,
            )
        )

    for job in jobs:
        job.apply_async(queue="imports")

    return {
        "file": file_path,
        "province": province,
        "constituency": constituency,
        "chunks": len(jobs),
    }


@shared_task(bind=True, autoretry_for=(), retry_kwargs={'max_retries': 0})
def import_voters_csv_chunk(self, rows, province, constituency, user_id=None):
    """
    Worker task: imports a single batch.
    """
    logger.info(f"Processing chunk of {len(rows)} rows for {province} / {constituency}")

    user = None
    if user_id:
        User = get_user_model()
        user = User.objects.filter(id=user_id).first()

    processor = CSVProcessor(csv_file=None, user=user, rows=rows)
    processor.province_override = province
    processor.constituency_override = constituency

    try:
        imported, failed = processor.process_batch(rows)
        logger.info(f"Chunk processed: {imported} imported, {failed} failed")
        return imported, failed
    except Exception as e:
        logger.exception(f"Chunk processing failed: {e}")
        raise