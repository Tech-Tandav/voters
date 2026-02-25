# """
# Management Command to Import Voter Data from Folder

# Usage:
#     python manage.py import_voter_data <folder_path>

# This command traverses the given folder, identifies Province from subdirectories,
# Constituency from filenames, and imports voter data into the database.
# """

# import os
# import time
# from django.core.management.base import BaseCommand, CommandError
# from voters.detail.utils.csv_processor import CSVProcessor


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

import os
import time
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from voters.detail.utils.csv_processor import CSVProcessor

BATCH_SIZE = 5000  # Adjust if needed

class Command(BaseCommand):
    help = 'Import voter data from a folder structure (Province/Constituency.csv)'

    def add_arguments(self, parser):
        parser.add_argument('folder_path', type=str, help='Path to the root folder containing province directories')

    def handle(self, *args, **options):
        folder_path = '/app/province'  # fixed path

        if not os.path.exists(folder_path):
            raise CommandError(f"Folder does not exist: {folder_path}")

        self.stdout.write(self.style.SUCCESS(f"Starting import from: {folder_path}"))

        start_time = time.time()
        stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'records_total': 0,
            'records_imported': 0,
            'records_failed': 0,
        }

        for root, dirs, files in os.walk(folder_path):
            province = os.path.basename(root) or os.path.basename(os.path.dirname(root))
            if province.endswith('Province'):
                province = province.replace('Province', '')

            for filename in files:
                if not filename.lower().endswith('.csv'):
                    continue

                stats['files_found'] += 1
                file_path = os.path.join(root, filename)
                constituency = os.path.splitext(filename)[0]
                self.stdout.write(f"Processing: Province='{province}', Constituency='{constituency}' ({filename})...")

                try:
                    processor = CSVProcessor(file_path, user=None)
                    processor.province_override = province
                    processor.constituency_override = constituency

                    # Batch insert for large CSVs
                    result = processor.process(batch_size=BATCH_SIZE)

                    if result['success']:
                        stats['files_processed'] += 1
                        stats['records_total'] += result['total']
                        stats['records_imported'] += result['imported']
                        stats['records_failed'] += result['failed']
                        self.stdout.write(self.style.SUCCESS(f"  Done: {result['imported']} imported"))
                    else:
                        stats['files_failed'] += 1
                        self.stdout.write(self.style.ERROR(f"  Failed: {result['error']}"))

                except Exception as e:
                    stats['files_failed'] += 1
                    log_file = '/app/data/import_errors.log'
                    os.makedirs(os.path.dirname(log_file), exist_ok=True)
                    with open(log_file, 'a') as f:
                        f.write(f"{file_path} - {str(e)}\n")
                    self.stdout.write(self.style.ERROR(f"  Error processing {filename}: {str(e)}"))

        duration = time.time() - start_time
        self.stdout.write("\n" + "="*40)
        self.stdout.write("IMPORT SUMMARY")
        self.stdout.write("="*40)
        self.stdout.write(f"Total Time: {duration:.2f} seconds")
        self.stdout.write(f"Files Found: {stats['files_found']}")
        self.stdout.write(f"Files Processed: {stats['files_processed']}")
        self.stdout.write(f"Files Failed: {stats['files_failed']}")
        self.stdout.write("-" * 20)
        self.stdout.write(f"Total Records Scanned: {stats['records_total']}")
        self.stdout.write(f"Records Imported: {stats['records_imported']}")
        self.stdout.write(f"Records Failed: {stats['records_failed']}")
        self.stdout.write("="*40)