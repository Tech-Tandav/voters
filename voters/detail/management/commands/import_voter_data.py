"""
Management Command to Import Voter Data from Folder

Usage:
    python manage.py import_voter_data <folder_path>

This command traverses the given folder, identifies Province from subdirectories,
Constituency from filenames, and imports voter data into the database.
"""

import os
import time
from django.core.management.base import BaseCommand, CommandError
from voters.detail.tasks import import_voters_csv

class Command(BaseCommand):
    help = 'Import voter data from a folder structure (Province/Constituency.csv)'

    def add_arguments(self, parser):
        parser.add_argument('folder_path', type=str, help='Path to the root folder containing province directories')

    def handle(self, *args, **options):
        folder_path = options['folder_path']

        if not os.path.exists(folder_path):
            raise CommandError(f"Folder does not exist: {folder_path}")

        self.stdout.write(self.style.SUCCESS(f"Starting import from: {folder_path}"))

        start_time = time.time()
        stats = {
            'files_found': 0,
            'files_queued': 0,
        }

        for root, dirs, files in os.walk(folder_path):
            province = os.path.basename(root)

            if root == folder_path and not province:
                province = os.path.basename(os.path.dirname(root))

            if province.endswith('Province'):
                province = province.replace('Province', '').strip()

            for filename in files:
                if not filename.lower().endswith('.csv'):
                    continue

                stats['files_found'] += 1
                file_path = os.path.join(root, filename)
                constituency = os.path.splitext(filename)[0]

                task = import_voters_csv.delay(
                    file_path=file_path,
                    province=province,
                    constituency=constituency,
                    user_id=None
                )

                stats['files_queued'] += 1
                self.stdout.write(self.style.SUCCESS(
                    f"Queued: Province='{province}', Constituency='{constituency}' → Task ID: {task.id}"
                ))

        duration = time.time() - start_time
        self.stdout.write("\n" + "=" * 40)
        self.stdout.write("IMPORT QUEUE SUMMARY")
        self.stdout.write("=" * 40)
        self.stdout.write(f"Time: {duration:.2f}s")
        self.stdout.write(f"Files Found: {stats['files_found']}")
        self.stdout.write(f"Files Queued: {stats['files_queued']}")
        self.stdout.write("=" * 40)
