import datetime, os, random, string, uuid, environ, requests, json, math
from io import BytesIO
from datetime import datetime
from PIL import Image
from django.utils import timezone
from django.core.files import File
from django.core.files.storage import default_storage
from django.db.models import FileField
from django.utils.translation import gettext_lazy as _
from rest_framework import status
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import raise_errors_on_nested_writes
from rest_framework.utils import model_meta
from rest_framework.response import Response


env = environ.Env()


def generate_filename(filename, keyword):
    """
    Generates filename with uuid and a keyword
    :param filename: original filename
    :param keyword: keyword to be added after uuid
    :return: new filename in string
    """
    ext = filename.split('.')[-1]
    new_filename = "%s.%s" % (keyword, ext)
    return new_filename


def upload_to_folder(instance, filename, folder, keyword):
    """
    Generates the path where it should to uploaded

    :param instance: model instance
    :param filename: original filename
    :param folder: folder name where it should be stored
    :param keyword: keyword to be attached with uuid
    :return: string of new path
    """
    return os.path.join(folder, generate_filename(
        filename=filename,
        keyword=keyword
    ))


def update(instance, serializer_class, data):
    raise_errors_on_nested_writes('update', serializer_class, data)
    info = model_meta.get_field_info(instance)

    for attr, value in data.items():
        if attr in info.relations and info.relations[attr].to_many:
            field = getattr(instance, attr)
            field.set(value)
        else:
            setattr(instance, attr, value)
    instance.updated_at = datetime.datetime.now()
    instance.save()


def reduce_image_size(image, quality=70):
    image_extension = image.name.split('.')[-1]
    image_type = 'jpeg'
    if image_extension == 'png':
        image_type = 'png'
    try:
        img = Image.open(image)
    except FileNotFoundError:
        return image
    thumb_io = BytesIO()
    img.save(thumb_io, image_type, quality=quality)
    new_image = File(thumb_io, name=image.name)
    return new_image


def file_cleanup(sender, **kwargs):
    """
    File cleanup callback used to emulate the old delete
    behavior using signals. Initially django deleted linked
    files when an object containing a File/ImageField was deleted.
    """
    field_names = [f.name for f in sender._meta.get_fields()]
    for fieldname in field_names:
        try:
            field = sender._meta.get_field(fieldname)
        except:
            field = None

        if field and isinstance(field, FileField):
            inst = kwargs["instance"]
            f = getattr(inst, fieldname)
            m = inst.__class__._default_manager
            try:
                if (
                    hasattr(f, "path")
                    and os.path.exists(f.path)
                    and not m.filter(
                    **{"%s__exact" % fieldname: getattr(inst, fieldname)}
                ).exclude(pk=inst._get_pk_val())
                ):
                    default_storage.delete(f.path)
            except:
                pass


def validate_uuid(uuid_string):
    try:
        uuid.UUID(uuid_string)
    except ValueError:
        raise ValidationError({
            'non_field_errors': _('Not a valid UUID')
        })


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def is_email_disposable(email):
    response = requests.get(
        url=f'https://disposable.debounce.io/?email={email}'
    )
    print(response.json())
    if response.status_code == 200 and response.json().get('disposable') == 'true':
        return True
    return False


def generate_random_string(length=3):
    """Generate a random string of fixed length in lowercase for appending to slug."""
    letters = string.ascii_lowercase + string.digits  # Use lowercase letters
    return ''.join(random.choice(letters) for _ in range(length))


def year_choices(from_year, to_year):
    return ((r, r) for r in range(from_year, to_year + 1))

def get_class_year_choices():
    return [(i, f"Year {i}") for i in range(1, 4)]


def get_current_date():
    return datetime.today().date()

def get_current_day():
    return datetime.now().strftime("%A")

def get_current_year():
    return datetime.now().year

def get_current_time():
    return datetime.now().time()

def get_date_obj(str_date):
    try:
        return datetime.strptime(str_date, '%Y-%m-%d').date()
    except ValueError:
        raise ValidationError("Date format should be 'YYYY-MM-DD'.")

# def get_today_day():
#     today = datetime.today()  # This will use the system's local time zone

#     # Extract the day of the week
#     today_day = today.strftime('%A')  # Get the full name of the day (e.g., Monday)

#     return today_day


def is_holiday_today(today, holidays):
    for holiday in holidays:
        if holiday.date_from <= today <= holiday.date_to:
            return True
    return False


def string_to_time( time_string):
    """
    Convert time string to a datetime.time object.
    """
    try:
        # Assuming the time string format is 'HH:MM AM/PM' or 'HH:MM'
        if isinstance(time_string, str):
            time_obj = datetime.strptime(time_string, '%I:%M %p').time() if 'AM' in time_string or 'PM' in time_string else datetime.strptime(time_string, '%H:%M').time()
            return time_obj
        else:
            return "12:00"  # Return the time object as is if already in correct format
    except Exception as e:
        print(f"Error converting time: {e}")
        return None
    

class BulkAction:
    def __init__(self, request, queryset):
        self.request = request
        self.queryset = queryset
    
    def archive(self):
        """
        Custom action to archive multiple objects.
        Expects a list of IDs in the request body.
        """
        archive = self.request.data.get("archive")
        if archive == "ids":
            ids = self.request.data.get('ids', [])
            if not isinstance(ids, list):
                ids = [ids]
            queryset = self.queryset.filter(id__in=ids)
            count = queryset.count()
            if count != len(ids):
                return Response(
                    {"error": "Some IDs were not found or are invalid."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        elif archive == "all":
            queryset = self.queryset
            count = queryset.count()
        else:
            return Response(
                {"message": "Archive unsuccessfull beacuse no ids or all value was present in archive key ."},
                status=status.HTTP_200_OK
            )
        for i in queryset:
            i.archive()   
        return  Response(
                {"message": f"{count} objects archived successfully."},
                status=status.HTTP_200_OK
        )

def get_object_or_404(model, name, **kwargs):
        """
        Helper method to fetch an object or raise a 404 response.
        """
        obj = model.objects.unarchived().filter(**kwargs).first()
        if not obj:
            raise Response(
                {name: ["Not found."]},
                status=status.HTTP_404_NOT_FOUND
            )
        return obj

def ceil_to_half(x):
    if x:
        return math.ceil(x * 2) / 2
    return None