from celery import shared_task
from django.contrib.auth import get_user_model
from voters.detail.utils.csv_processor import CSVProcessor
from celery.exceptions import SoftTimeLimitExceeded



@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=10, retry_kwargs={'max_retries': 3})
def import_voters_csv(self, file_path, province, constituency, user_id=None):
    """
    Celery task to process a CSV file.
    """
    user = None
    if user_id:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        user = User.objects.filter(id=user_id).first()

    processor = CSVProcessor(file_path, user=user)
    processor.province_override = province
    processor.constituency_override = constituency

    # Process and return results
    result = processor.process()
    return result


@shared_task(bind=True, autoretry_for=(), retry_kwargs={'max_retries': 0})
def import_voters_csv_chunk(self, rows, province, constituency, user_id=None):
    """
    Worker task: imports a single batch.
    """
    user = None
    if user_id:
        User = get_user_model()
        user = User.objects.filter(id=user_id).first()

    processor = CSVProcessor(csv_file=None, user=user, rows=rows)
    processor.province_override = province
    processor.constituency_override = constituency

    try:
        return processor.process_batch(rows)
    except SoftTimeLimitExceeded:
        raise