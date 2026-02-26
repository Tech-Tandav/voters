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
from voters.detail.utils.csv_processor import CSVProcessor

# class Command(BaseCommand):
#     help = 'Import voter data from a folder structure (Province/Constituency.csv)'

#     def add_arguments(self, parser):
#         parser.add_argument('folder_path', type=str, help='Path to the root folder containing province directories')

#     def handle(self, *args, **options):
#         folder_path = options['folder_path']
        
#         if not os.path.exists(folder_path):
#             raise CommandError(f"Folder does not exist: {folder_path}")
        
#         self.stdout.write(self.style.SUCCESS(f"Starting import from: {folder_path}"))
        
#         start_time = time.time()
#         stats = {
#             'files_found': 0,
#             'files_processed': 0,
#             'files_failed': 0,
#             'records_total': 0,
#             'records_imported': 0,
#             'records_failed': 0,
#         }
        
#         # Walk through the directory
#         for root, dirs, files in os.walk(folder_path):
#             # Determine Province from directory name
#             province = os.path.basename(root)

#             # If user passed a Province folder directly, handle files in the root
#             if root == folder_path:
#                 # If folder_path ends with slash, basename might be empty
#                 if not province:
#                     province = os.path.basename(os.path.dirname(root))
            
#             # Clean Province Name (remove 'Province' suffix if present)
#             if province.endswith('Province'):
#                 province = province.replace('Province', '')
            
#             for filename in files:
#                 if not filename.lower().endswith('.csv'):
#                     continue
                
#                 stats['files_found'] += 1
#                 file_path = os.path.join(root, filename)
                
#                 # Determine Constituency from filename
#                 constituency = os.path.splitext(filename)[0]
                
#                 self.stdout.write(f"Processing: Province='{province}', Constituency='{constituency}' ({filename})...")
                
#                 try:
#                     # Process CSV
#                     processor = CSVProcessor(file_path, user=None) # System upload, no specific user
#                     processor.province_override = province
#                     processor.constituency_override = constituency
                    
#                     result = processor.process()
                    
#                     if result['success']:
#                         stats['files_processed'] += 1
#                         stats['records_total'] += result['total']
#                         stats['records_imported'] += result['imported']
#                         stats['records_failed'] += result['failed']
#                         self.stdout.write(self.style.SUCCESS(f"  Done: {result['imported']} imported"))
#                     else:
#                         stats['files_failed'] += 1
#                         self.stdout.write(self.style.ERROR(f"  Failed: {result['error']}"))
                        
#                 except Exception as e:
#                     stats['files_failed'] += 1
#                     self.stdout.write(self.style.ERROR(f"  Error processing {filename}: {str(e)}"))

#         # Final Summary
#         duration = time.time() - start_time
#         self.stdout.write("\n" + "="*40)
#         self.stdout.write("IMPORT SUMMARY")
#         self.stdout.write("="*40)
#         self.stdout.write(f"Total Time: {duration:.2f} seconds")
#         self.stdout.write(f"Files Found: {stats['files_found']}")
#         self.stdout.write(f"Files Processed: {stats['files_processed']}")
#         self.stdout.write(f"Files Failed: {stats['files_failed']}")
#         self.stdout.write("-" * 20)
#         self.stdout.write(f"Total Records Scanned: {stats['records_total']}")
#         self.stdout.write(f"Records Imported: {stats['records_imported']}")
#         self.stdout.write(f"Records Failed: {stats['records_failed']}")
#         self.stdout.write("="*40)

# voters/management/commands/import_voters_folder.py

import os
import time
from django.core.management.base import BaseCommand, CommandError
from celery import group

from voters.detail.tasks import import_voters_csv


class Command(BaseCommand):
    help = 'Import voter data from a folder containing CSV files (Province folder only)'

    def add_arguments(self, parser):
        parser.add_argument(
            'folder_path', type=str,
            help='Path to the folder containing CSV files'
        )

    def handle(self, *args, **options):
        folder_path = options['folder_path']

        if not os.path.exists(folder_path):
            raise CommandError(f"Folder does not exist: {folder_path}")

        self.stdout.write(self.style.SUCCESS(f"Starting import from: {folder_path}"))

        start_time = time.time()
        stats = {'files_found': 0, 'files_queued': 0}
        jobs = []

        # Iterate only files directly in folder_path
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            if not os.path.isfile(file_path):
                continue  # skip subfolders

            if not filename.lower().endswith('.csv'):
                continue  # skip non-CSV files

            stats['files_found'] += 1
            constituency = os.path.splitext(filename)[0]
            province = os.path.basename(folder_path)  # use folder name as province

            # Create Celery signature
            task_sig = import_voters_csv.s(
                file_path=file_path,
                province=province,
                constituency=constituency,
                user_id=None
            )
            jobs.append(task_sig)

            self.stdout.write(self.style.SUCCESS(
                f"Queued task for: Province='{province}', Constituency='{constituency}'"
            ))

        # Send tasks to Celery if any
        if jobs:
            result = group(jobs).apply_async(queue="imports")
            stats['files_queued'] = len(jobs)
            self.stdout.write(self.style.SUCCESS(
                f"\nAll tasks sent to Celery queue 'imports'. Group ID: {result.id}"
            ))

        duration = time.time() - start_time
        self.stdout.write("\n" + "=" * 40)
        self.stdout.write("IMPORT QUEUE SUMMARY")
        self.stdout.write("=" * 40)
        self.stdout.write(f"Time: {duration:.2f}s")
        self.stdout.write(f"Files Found: {stats['files_found']}")
        self.stdout.write(f"Files Queued: {stats['files_queued']}")
        self.stdout.write("=" * 40)