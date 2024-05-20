import os
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.apps import apps
from django.contrib.auth.models import Permission
from django.db.models.query import QuerySet


User = get_user_model()


class Command(BaseCommand):
    help = 'Create a user with a username and password.'

    def add_arguments(self, parser):
        parser.add_argument('--user', type=str, help='Username and password of the user to be created. Format: username:password')

    def _get_available_permissions(self) -> QuerySet[Permission]:
        exclude_models_name = ['logentry', 'permission', 'group', 'user',
                               'contenttype', 'session', 'crontabschedule',
                               'intervalschedule', 'periodictask', 'periodictasks',
                               'solarschedule', 'clockedschedule']
        available_models_name: list[str] = list(filter(
            lambda x: x not in exclude_models_name,
            [i._meta.model_name for i in apps.get_models()]
        ))
        available_permissions = (
            Permission.objects.select_related('content_type')
            .filter(content_type__model__in=available_models_name)
        )
        return available_permissions

    def _user_handler(self, username: str, password: str) -> None:
        user = User.objects.filter(username=username).first()
        if not user:
            user = User.objects.create_user(username, password=password, is_staff=True)
            self.stdout.write(self.style.SUCCESS(f'Successfully created user {username}'))
        else:
            self.stdout.write(self.style.WARNING(f'User {username} already exists'))
        permissions = self._get_available_permissions()
        user.user_permissions.add(*permissions)
        self.stdout.write(self.style.SUCCESS(f'Successfully added permissions to user {username}'))

    def handle(self, *args, **options):
        try:
            if options['user']:
                username, password = options['user'].split(':')
                self._user_handler(username, password)
            else:
                users: str = os.environ.get('USERS')
                if not users:
                    raise CommandError('No users provided')
                for user in users.split(','):
                    username, password = user.split(':')
                    self._user_handler(username, password)
        except Exception as e:
            raise CommandError(f'Failed to create user: {e}')
