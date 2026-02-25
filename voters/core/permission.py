from django.utils.crypto import get_random_string
from rest_framework import permissions
from university_attendance_management.users.models import User
from university_attendance_management.core.utils import MicrosoftAuth
 
 
class DjangoUser:
    def __init__(self, username):
        self.username = username

    def is_django_user(self):
        try:
            return User.objects.filter(username=self.username).first()
            # if user.is_active and user.is_staff:
            #     return user
            # else:
            #     return False
        except User.DoesNotExist:
            return False

    def create_django_user(self,role):
        try:
            random_password = get_random_string(length=12)
            user_roles = [key for key, value in role.items() if value]
            have_permissions = {"is_staff":False}
            if any(role in user_roles for role in ["admin", "accounts", "read_only_admin"]):
                have_permissions["is_staff"]=True
            user, created = User.objects.get_or_create(username=self.username, defaults={"user_role": user_roles, **have_permissions})
            if created:
                user.set_password(random_password)
                user.save()
            return user
        except Exception as e:
            pass


class UserPermission:
    def __init__(self, user_role):
        self.user_role = user_role
        
    def has_permission(self, request, view):
        try:
            role_permissions = {
                "admin": {  
                    "ALL_METHODS": "ALL_VIEWS"
                },
                "teacher": {  
                    "GET": "ALL_VIEWS",
                    "POST": {"MarkAttendance", "AttendanceCreateView", "ScheduleStudentsAttendanceView", "PayrollLogsCreateView"},
                    "PATCH": {"AttendanceDetailView"},
                    "DELETE": {"AttendanceDetailView"},
                },
                "read_only_admin": {  
                    "GET": "ALL_VIEWS"
                },
                "account": {  
                    "ALL_METHODS": "ALL_VIEWS"
                },
            }
            action = request.method
            view_name = view.__class__.__name__
            
            for role in self.user_role:
                if role in role_permissions:
                    permissions = role_permissions[role]
                    
                    if permissions.get("ALL_METHODS") == "ALL_VIEWS":
                        return True
                    
                    if action in permissions:
                        allowed_views = permissions[action]
                        
                        if allowed_views == "ALL_VIEWS" or view_name in allowed_views:
                            return True
                    
            return False
        except Exception as e:
            return False
    

class MicrosoftPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        try:
            auth_header = request.headers.get('Authorization', '').split('Bearer ')[-1]
            microsoft_auth = MicrosoftAuth()
            user_email = microsoft_auth.get_microsoft_email(auth_header)
            django_user = DjangoUser(user_email)
            user = django_user.is_django_user()
            if not user: 
                group_ids = microsoft_auth.get_user_group_ids(auth_header)
                teacher = microsoft_auth.is_in_teacher_group(group_ids) 
                admin = microsoft_auth.is_in_admin_group(group_ids)
                read_only_admin = microsoft_auth.is_in_read_only_admin_group(group_ids)
                account = microsoft_auth.is_in_accounts_group(group_ids)
                role = {
                    "admin": admin,
                    "teacher": teacher ,
                    "read_only_admin": read_only_admin,
                    "account":account
                }
                user = django_user.create_django_user(role)
            request.user = user
            user_role = user.user_role
            return UserPermission(user_role).has_permission(request, view)

        except Exception as e:
            print(e)
            return False


class MicrosoftOrAuthenticatedPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        is_authenticated = permissions.IsAuthenticated().has_permission(request, view)
        microsoft_permission = MicrosoftPermission().has_permission(request, view)
        return microsoft_permission or is_authenticated


