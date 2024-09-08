import json
from django.db import models
from django.contrib.auth.models import User
from django_quill.fields import QuillField

# Create your models here.

ROLE_CHOICES = (
    ('admin'  , 'Admin'),
    ('user'  , 'User'),
)

def avatar_with_id(instance, filename):
    return "{}/avatar/{}".format(f"{instance.user.id}", filename)

def convert_to_quill():
    converted_data = {
        "delta": "",
        "html": "Write something #cool about you.",
    }
    return json.dumps(converted_data)


class Profile(models.Model):
    user      = models.OneToOneField(User, on_delete=models.CASCADE)
    role      = models.CharField(max_length=20, choices=ROLE_CHOICES, default='user')
    full_name = models.CharField(max_length=255, null=True, blank=True)
    country   = models.CharField(max_length=255, null=True, blank=True)
    city      = models.CharField(max_length=255, null=True, blank=True)
    zip_code  = models.CharField(max_length=255, null=True, blank=True)
    address   = models.CharField(max_length=255, null=True, blank=True)
    phone     = models.CharField(max_length=255, null=True, blank=True)
    avatar    = models.ImageField(upload_to=avatar_with_id, null=True, blank=True)
    dark_mode = models.BooleanField(default=False)
    bio       = QuillField(default=convert_to_quill())

    def __str__(self):
        return self.user.username