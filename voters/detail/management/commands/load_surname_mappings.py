"""
Django Management Command: Load Surname Mappings

Loads comprehensive surname-to-caste mappings into the database from CSV.
"""

import os
import csv
from django.core.management.base import BaseCommand
from django.conf import settings
from voters.detail.models import SurnameMapping


class Command(BaseCommand):
    help = 'Load comprehensive surname-to-caste mappings from CSV into database'

    # Mapping from Nepali caste name to internal group
    CASTE_NAME_MAPPING = {
        # BRAHMIN
        'ब्राह्मण': 'brahmin',
        'ब्राह्मण/क्षत्री': 'brahmin',
        'जङ्गम': 'brahmin',
        'भारती': 'brahmin',
        'पर्वत': 'brahmin',
        'बन': 'brahmin',
        'अरण्य': 'brahmin',

        # CHHETRI
        'क्षत्री': 'chhetri',
        'क्षेत्री': 'chhetri',
        'क्षेत्री/मगर': 'chhetri',
        'खत्री': 'chhetri',
        'ठकुरी': 'chhetri',
        'राजपूत': 'chhetri',
        'सेन': 'chhetri',
        'राजपुत': 'chhetri',

        # JANAJATI
        'नेवार': 'janajati',
        'गुरुङ': 'janajati',
        'तामाङ': 'janajati',
        'मगर': 'janajati',
        'राई': 'janajati',
        'लिम्बु': 'janajati',
        'सुनुवार': 'janajati',
        'याक्खा': 'janajati',
        'शेर्पा': 'janajati',
        'भोटे': 'janajati',
        'किराँत': 'janajati',
        'धिमाल': 'janajati',
        'मेच': 'janajati',
        'भुजेल': 'janajati',
        'हायु': 'janajati',
        'जिरेल': 'janajati',
        'जनजाति': 'janajati',
        'दनुवार': 'janajati',
        'माझी': 'janajati',
        'बोटे': 'janajati',
        'थारु': 'janajati',
        'राजवंशी': 'janajati',
        'राजबंशी': 'janajati',
        'खवास': 'janajati',
        'दराई': 'janajati',
        'कुमाल': 'janajati',
        'बलामी': 'janajati',

        # DALIT
        'दलित': 'dalit',
        'दलित ': 'dalit',
        'विश्वकर्मा': 'dalit',
        'सार्की': 'dalit',
        'दमाई': 'dalit',
        'गन्धर्व': 'dalit',
        'कामी': 'dalit',
        'लोहार': 'dalit',
        'दर्जी': 'dalit',
        'मुसहर': 'dalit',
        'डोम': 'dalit',
        'धोबी': 'dalit',
        'हजाम': 'dalit',
        'नाई': 'dalit',
        'रजक': 'dalit',
        'सोनार': 'dalit',
        'सुनार': 'dalit',
        'दास': 'dalit',
        'परियार': 'dalit',
        'चमार': 'dalit',
        'हरिजन': 'dalit',
        'दुसाध': 'dalit',
        'पासवान': 'dalit',

        # MADHESI
        'मधेशी': 'madhesi',
        'मधेसी': 'madhesi',
        'यादव': 'madhesi',
        'चौधरी': 'madhesi',
        'महतो': 'madhesi',
        'ठाकुर': 'madhesi',
        'मण्डल': 'madhesi',
        'धानुक': 'madhesi',
        'कुशवाहा': 'madhesi',
        'साह': 'madhesi',
        'तेली': 'madhesi',
        'कलवार': 'madhesi',
        'कुर्मी': 'madhesi',
        'केवट': 'madhesi',
        'नोनिया': 'madhesi',
        'मल्लाह': 'madhesi',
        'हलुवाई': 'madhesi',
        'मौर्य': 'madhesi',
        'कामत': 'madhesi',
        'बानियाँ': 'madhesi',

        # MUSLIM
        'मुसलमान': 'muslim',
    }

    def add_arguments(self, parser):
        parser.add_argument(
            '--csv',
            type=str,
            default='nepali_surnames_castes_comprehensive.csv',
            help='Path to the CSV file'
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing mappings before loading'
        )

    def handle(self, *args, **options):
        """Execute the command"""
        csv_path = options['csv']
        if not os.path.isabs(csv_path):
            csv_path = os.path.join(settings.BASE_DIR, csv_path)

        if not os.path.exists(csv_path):
            self.stdout.write(self.style.ERROR(f'CSV file not found: {csv_path}'))
            return

        if options['clear']:
            self.stdout.write('Clearing existing mappings...')
            SurnameMapping.objects.all().delete()

        self.stdout.write(f'Loading mappings from {csv_path}...')
        
        created_count = 0
        updated_count = 0
        unknown_caste_groups = set()

        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            
            for row in reader:
                if len(row) < 2:
                    continue
                
                surname = row[0].strip()
                nepali_caste = row[1].strip()
                
                if not surname or not nepali_caste:
                    continue

                # Map Nepali caste to internal group
                caste_group = self.CASTE_NAME_MAPPING.get(nepali_caste, 'other')
                
                if caste_group == 'other' and nepali_caste not in ['अन्य', 'विभिन्न', 'हिन्दु', 'बौद्ध', 'क्रिश्चियन', 'जैन', 'योगी', 'साधु', 'सन्यासी', 'उदासीन', 'बैद्य']:
                     unknown_caste_groups.add(nepali_caste)

                mapping, created = SurnameMapping.objects.update_or_create(
                    surname=surname,
                    defaults={
                        'caste_group': caste_group,
                        'is_active': True,
                    }
                )
                
                if created:
                    created_count += 1
                else:
                    updated_count += 1

        if unknown_caste_groups:
            self.stdout.write(self.style.WARNING(f'Unmapped Nepali castes (defaulted to "other"): {", ".join(unknown_caste_groups)}'))

        self.stdout.write(
            self.style.SUCCESS(
                f'\n✅ Done! Created {created_count} new mappings, '
                f'updated {updated_count} existing mappings.'
            )
        )
        
        total = SurnameMapping.objects.filter(is_active=True).count()
        self.stdout.write(
            self.style.SUCCESS(
                f'📊 Total active surname mappings: {total}'
            )
        )