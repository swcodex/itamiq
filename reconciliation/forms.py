from django import forms
from .models import Job, Script 
from django.forms.widgets import TimeInput

# Job-related forms
class JobForm(forms.ModelForm):
    DAYS_CHOICES = [
        ('SUN', 'Sunday'),
        ('MON', 'Monday'),
        ('TUE', 'Tuesday'),
        ('WED', 'Wednesday'),
        ('THU', 'Thursday'),
        ('FRI', 'Friday'),
        ('SAT', 'Saturday'),
    ]

    schedule_days = forms.MultipleChoiceField(
        choices=DAYS_CHOICES,
        widget=forms.CheckboxSelectMultiple(attrs={'class': 'form-check-input'}),
        required=False
    )

    class Meta:
        model = Job
        fields = ['name', 'description', 'schedule_time', 'schedule_days']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'description': forms.Textarea(attrs={'class': 'form-control', 'rows': 1}),
            'schedule_time': TimeInput(format='%H:%M', attrs={'class': 'form-control', 'type': 'time'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk and self.instance.schedule_days:
            self.initial['schedule_days'] = self.instance.get_schedule_days()


# Script-related forms
class ScriptForm(forms.ModelForm):
    class Meta:
        model = Script
        fields = ['name', 'content', 'table_name', 'order', 'import_enabled']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'content': forms.Textarea(attrs={'rows': 20, 'cols': 80, 'class': 'form-control'}),
            'table_name': forms.TextInput(attrs={'class': 'form-control'}),
            'order': forms.NumberInput(attrs={'class': 'form-control', 'style': 'max-width: 80px;'}),
            'import_enabled': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['name'].required = True
        self.fields['content'].required = True

        # Hide 'id' and 'job' fields
        if 'id' in self.fields:
            self.fields['id'].widget = forms.HiddenInput()
        if 'job' in self.fields:
            self.fields['job'].widget = forms.HiddenInput()
       # if 'import_enabled' in self.fields:
         #   self.fields['import_enabled'].widget = forms.HiddenInput()

ScriptFormSet = forms.inlineformset_factory(
    Job, Script,
    form=ScriptForm,
    fields=['name', 'content', 'table_name', 'order', 'import_enabled'],
    extra=1,
    can_delete=True
)