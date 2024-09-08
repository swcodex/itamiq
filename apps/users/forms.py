from django import forms
from django.utils.translation import gettext_lazy as _
from apps.users.models import Profile


class ProfileForm(forms.ModelForm):
    class Meta:
        model = Profile
        exclude = ('user', 'role', 'avatar',)

    def __init__(self, *args, **kwargs):
        super(ProfileForm, self).__init__(*args, **kwargs)

        for field_name, field in self.fields.items():
            self.fields[field_name].widget.attrs['placeholder'] = field.label
            self.fields[field_name].widget.attrs['class'] = 'form-control'
            self.fields[field_name].widget.attrs['required'] = False


class QuillFieldForm(forms.ModelForm):
    class Meta:
        model = Profile
        fields = ('bio',)