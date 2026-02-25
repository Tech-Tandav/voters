"""
Zip Processor for Voter Data

Handles importing voter data from ZIP files containing province folders.
Extracts province from folder name and constituency from filename.
"""

import os
import zipfile
import shutil
import tempfile
import logging
import time
from django.conf import settings
from voters.utils.csv_processor import CSVProcessor
from voters.models import UploadHistory

logger = logging.getLogger(__name__)


def process_zip_file(zip_file, user=None):
    """
    Process ZIP file containing province folders and constituency CSVs.
    
    Structure expected:
    Province_Name/
      Area_Name.csv
      Another_Area.csv
      
    Args:
        zip_file: Uploaded ZIP file object
        user: Django User object
        
    Returns:
        dict: Processing results
    """
    start_time = time.time()
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp()
    
    results = {
        'success': True,
        'message': 'ZIP processed successfully',
        'total_files': 0,
        'processed_files': 0,
        'total_records': 0,
        'imported_records': 0,
        'failed_records': 0,
        'errors': [],
        'unmapped_surnames': set(),
    }
    
    try:
        # Save ZIP to temp file
        zip_path = os.path.join(temp_dir, 'upload.zip')
        with open(zip_path, 'wb+') as destination:
            for chunk in zip_file.chunks():
                destination.write(chunk)
        
        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Walk through extracted files
        # We start walking from temp_dir
        for root, dirs, files in os.walk(temp_dir):
            if root == temp_dir:
                # Top level - expected to be empty or contain province folders
                # We are interested in subdirectories (Province folders)
                pass
                
            for file in files:
                if not file.lower().endswith('.csv'):
                    continue
                
                file_path = os.path.join(root, file)
                
                # Determine Province and Constituency
                # Structure: temp_dir / Province / Constituency.csv
                
                # Get relative path from temp_dir
                rel_path = os.path.relpath(file_path, temp_dir)
                parts = rel_path.split(os.sep)
                
                if len(parts) < 2:
                    # File is at root of zip, no province folder
                    # We can treat root as default or skip
                    # Requirement says: "Province must be identified from the folder name"
                    # So we should probably skip or label as "Unknown"
                    province = "Unknown"
                    constituency = os.path.splitext(file)[0]
                else:
                    # Province is the directory name
                    # parts[0] might be the province folder
                    # Note: Zip might contain a wrapper folder
                    
                     # If zip has Wrapper/Province/file.csv, parts = [Wrapper, Province, file.csv]
                     # simple logic: Parent folder of the file is the Province
                     
                    parent_dir = os.path.dirname(file_path)
                    province = os.path.basename(parent_dir)
                    
                    # If parent dir is temp_dir, it means file is at root
                    if parent_dir == temp_dir:
                        province = "Unknown"
                        
                    constituency = os.path.splitext(file)[0]

                # Process this CSV
                logger.info(f"Processing: Province={province}, Constituency={constituency}, File={file}")
                
                results['total_files'] += 1
                
                try:
                    # We need to adapt CSVProcessor to accept province and constituency
                    # For now, we'll instantiate it and modify its process method call or arguments
                    # We will update CSVProcessor to handle this.
                    
                    # Open file in text mode
                    # CSVProcessor expects a file object or path.
                    # If path, it uses pd.read_csv(self.csv_file)
                    
                    processor = CSVProcessor(file_path, user)
                    
                    # Pass context data to process method (we need to update CSVProcessor first)
                    # Or set attributes
                    processor.province_override = province
                    processor.constituency_override = constituency
                    
                    file_result = processor.process()
                    
                    if file_result['success']:
                        results['processed_files'] += 1
                        results['total_records'] += file_result['total']
                        results['imported_records'] += file_result['imported']
                        results['failed_records'] += file_result['failed']
                        if 'unmapped_surnames' in file_result:
                            results['unmapped_surnames'].update(file_result['unmapped_surnames'])
                    else:
                        results['errors'].append(f"{file}: {file_result.get('error')}")
                        
                except Exception as e:
                    logger.error(f"Error processing {file}: {e}")
                    results['errors'].append(f"{file}: {str(e)}")

        results['processing_time'] = time.time() - start_time
        results['unmapped_surnames'] = list(results['unmapped_surnames'])
        
        return results

    except Exception as e:
        logger.error(f"ZIP processing error: {e}")
        return {
            'success': False,
            'error': str(e),
            'processing_time': time.time() - start_time
        }
        
    finally:
        # Cleanup
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup temp dir: {e}")
