from typing import Any
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model


class Command(BaseCommand):
    help='Create tenant user'
    
    def add_arguments(self, parser):
        parser.add_argument('username', type=str, help='The username for tenant')
        parser.add_argument('password', type=str, help='The password for tenant')

        
    def handle(self, *args, **kwargs) :
        username = kwargs['username']
        password = kwargs['password']
        
        try:
            User = get_user_model()
            user = User(username=username, is_superuser=True, is_active=True)
            user.set_password(password)
            user.save()
            self.stdout.write(self.style.SUCCESS(
                f'Super user  with {username} created successfully.'
            ))
            
        except ValidationError as e:
            pass
        except Exception as e:
            pass
