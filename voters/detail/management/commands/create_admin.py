import os
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

User = get_user_model()

class Command(BaseCommand):
    help = "Create superuser from environment variables"

    def handle(self, *args, **options):
        username = "tree"
        password = "tree"

        if not username or not password:
            self.stdout.write(
                self.style.ERROR("Missing environment variables")
            )
            return

        if User.objects.filter(username=username).exists():
            self.stdout.write(
                self.style.WARNING("Superuser already exists")
            )
            return

        User.objects.create_superuser(
            username=username,
            password=password
        )

        self.stdout.write(
            self.style.SUCCESS("Superuser created successfully")
        )
