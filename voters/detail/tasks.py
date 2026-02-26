from celery import shared_task
from django.contrib.auth import get_user_model
from voters.detail.utils.csv_processor import CSVProcessor


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=10, retry_kwargs={'max_retries': 3})
def import_voters_csv(self, file_path, province, constituency, user_id=None):
    user = None
    if user_id:
        User = get_user_model()
        user = User.objects.filter(id=user_id).first()

    processor = CSVProcessor(file_path, user=user)
    processor.province_override = province
    processor.constituency_override = constituency

    return processor.process()