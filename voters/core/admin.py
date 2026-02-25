from django.contrib.admin import ModelAdmin, SimpleListFilter
from django.contrib import admin
from allauth.account.models import EmailAddress
from allauth.mfa.models import Authenticator
from allauth.socialaccount.models import SocialToken, SocialAccount, SocialApp
from django_celery_beat.models import (PeriodicTask, IntervalSchedule, CrontabSchedule,SolarSchedule, ClockedSchedule, )



# Register your models here.
admin.site.site_header = "IELTS"
admin.site.index_title = "IELTS"
admin.site.site_title = "Simulator"


admin.site.unregister(SocialToken)
admin.site.unregister(SocialAccount)
admin.site.unregister(SocialApp)
admin.site.unregister(PeriodicTask)
admin.site.unregister(IntervalSchedule)
admin.site.unregister(CrontabSchedule)
admin.site.unregister(SolarSchedule)
admin.site.unregister(ClockedSchedule)
admin.site.unregister(EmailAddress)
# admin.site.unregister(Site)
admin.site.unregister(Authenticator)


class ArchiveMixin:
    def archive(self, request, queryset):
        queryset.archive()

    def restore(self, request, queryset):
        queryset.restore()

    archive.short_description = 'Archive selected items'
    restore.short_description = 'Restore selected items'


class PublishMixin:
    def publish(self, request, queryset):
        queryset.publish()

    def hide(self, request, queryset):
        queryset.hide()

    publish.short_description = 'Publish selected items'
    hide.short_description = 'Hide selected items'


class BaseModelAdmin(ModelAdmin, ArchiveMixin):
    list_display = (
        'updated_at',
    )
    search_fields = (
        'id',
    )
    ordering = (
        '-created_at',
    )
    list_per_page = (
        100
    )
    actions = [
        'archive',
        'restore'
    ]
    readonly_fields = (
        'created_at',
        'updated_at'
    )
    list_filter = (
        'is_archived',
    )


class BasePublishModelAdmin(BaseModelAdmin, PublishMixin):
    actions = [
        'archive',
        'restore',
        'publish',
        'hide'
    ]
    readonly_fields = (
        'created',
        'updated'
    )
    list_filter = (
        'is_archived',
        'is_published'
    )

class ArchiveFilter(SimpleListFilter):
    title = "Data"
    parameter_name = 'data'
    
    def lookups(self, request, model_admin):
        return [
            ("all", "all"),
            ("archived", "archived"),
            ("unarchived","unarchived"),
        ]
    
    def queryset(self, request, queryset):
        if self.value() == "archived":
            return queryset.filter(is_archived=True)
        elif self.value() == "unarchived" or self.value() is None: 
            return queryset.filter(is_archived=False)
        return queryset
    
