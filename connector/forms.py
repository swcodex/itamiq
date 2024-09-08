from django import forms
from .models import Job, Script, Table, Column
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
        fields = ['name', 'content', 'table_name', 'order_exec', 'import_enabled']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'content': forms.Textarea(attrs={'rows': 20, 'cols': 80, 'class': 'form-control'}),
            'table_name': forms.TextInput(attrs={'class': 'form-control'}),
            'order_exec': forms.NumberInput(attrs={'class': 'form-control', 'style': 'max-width: 80px;'}),
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
    fields=['name', 'content', 'table_name', 'order_exec', 'import_enabled'],
    extra=1,
    can_delete=True
)


# Table-related forms
class TableForm(forms.ModelForm):
    class Meta:
        model = Table
        fields = ['table_name', 'last_import', 'row_count', 'row_count_prev', 'transform_script', 'run_transform']
        widgets = {
            'table_name': forms.TextInput(attrs={'class': 'form-control', 'readonly': 'readonly'}),
            'last_import': forms.DateTimeInput(attrs={'class': 'form-control', 'type': 'datetime-local', 'readonly': 'readonly'}),
            'row_count': forms.NumberInput(attrs={'class': 'form-control', 'readonly': 'readonly'}),
            'row_count_prev': forms.NumberInput(attrs={'class': 'form-control', 'readonly': 'readonly'}),
            'transform_script': forms.Textarea(attrs={'class': 'form-control', 'rows': 15}),
            'run_transform': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['transform_script'].required = False

TableFormSet = forms.inlineformset_factory(
    Script, Table,
    form=TableForm,
    fields=['table_name', 'last_import', 'row_count', 'row_count_prev', 'transform_script', 'run_transform'],
    extra=1,
    can_delete=True
)


class ColumnForm(forms.ModelForm):
    foreign_key_reference = forms.ModelChoiceField(
        queryset=Column.objects.filter(is_unique=True),
        required=False,
        widget=forms.Select(attrs={'class': 'form-control'})
    )

    class Meta:
        model = Column
        fields = ['table_name', 'column_name', 'override_column_name', 'detected_data_type', 'override_data_type', 'primary_key', 'foreign_key_reference', 'is_unique']
        widgets = {
            'table_name': forms.TextInput(attrs={'class': 'form-control'}),
            'column_name': forms.TextInput(attrs={'class': 'form-control'}),
            'override_column_name': forms.TextInput(attrs={'class': 'form-control'}),
            'detected_data_type': forms.Select(attrs={'class': 'form-control'}),
            'override_data_type': forms.Select(attrs={'class': 'form-control'}),
            'primary_key': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'is_unique': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            # Exclude the current instance from the foreign key choices
            self.fields['foreign_key_reference'].queryset = Column.objects.filter(is_unique=True).exclude(pk=self.instance.pk)


ColumnFormSet = forms.inlineformset_factory(
    Script, Column,
    form=ColumnForm,
    fields=['table_name', 'column_name', 'override_column_name', 'detected_data_type', 'override_data_type', 'primary_key', 'foreign_key_reference', 'is_unique'],
    extra=1,
    can_delete=True
)

class CustomColumnForm(forms.ModelForm):
    column_name = forms.CharField(disabled=True, required=False)
    detected_data_type = forms.CharField(disabled=True, required=False)

    class Meta:
        model = Column
        fields = ('column_name', 'override_column_name', 'override_data_type', 'detected_data_type', 'primary_key', 'foreign_key_reference', 'is_unique')
        widgets = {
            'column_name': forms.TextInput(attrs={'class': 'form-control', 'readonly': 'readonly'}),
            'override_column_name': forms.TextInput(attrs={'class': 'form-control'}),
            'override_data_type': forms.Select(attrs={'class': 'form-control'}),
            'detected_data_type': forms.TextInput(attrs={'class': 'form-control', 'readonly': 'readonly'}),
            'foreign_key_reference': forms.Select(attrs={'class': 'form-control'}),
            'primary_key': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
            'is_unique': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

# Custom Edit-Table Form
class CustomEditTableForm(forms.Form):
    transform_script = forms.CharField(
        widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 20}),
        required=False  # Add this line to make the field optional
    )
    run_transform = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )