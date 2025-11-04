from .models import Client, ClientDatabase
from django import forms


class ClientForm(forms.ModelForm):
    class Meta:
        model = Client
        fields = [
            'name', 'email', 'phone', 'db_name', 'company_name', 'status',
            'address', 'city', 'state', 'country', 'postal_code'
        ]
        widgets = {
            'address': forms.Textarea(attrs={'rows': 3}),
        }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # If editing (instance exists), make db_name not required
        if self.instance.pk and 'db_name' in self.fields:
            self.fields['db_name'].required = False

    def clean_name(self):
        name = self.cleaned_data.get('name', '').strip()
        if len(name) < 2:
            raise forms.ValidationError("Name must be at least 2 characters long.")
        return name

    def clean_email(self):
        email = self.cleaned_data.get('email', '').strip()
        if not email:
            raise forms.ValidationError("Email is required.")
        # Check uniqueness (excluding current instance if editing)
        queryset = Client.objects.filter(email=email)
        if self.instance.pk:
            queryset = queryset.exclude(pk=self.instance.pk)
        if queryset.exists():
            raise forms.ValidationError("This email is already registered.")
        return email

    def clean_phone(self):
        phone = self.cleaned_data.get('phone', '').strip()
        if not phone.isdigit():
            raise forms.ValidationError("Phone number must contain only digits.")
        if len(phone) != 10:
            raise forms.ValidationError("Phone number must be exactly 10 digits.")
        return phone
    
    def clean_db_name(self):
        db_name = self.cleaned_data.get('db_name', '').strip()
        
        # If editing, return the existing db_name (it's not editable)
        if self.instance.pk:
            return self.instance.db_name
        
        # Validation for new clients only
        if not db_name:
            raise forms.ValidationError("Database name is required.")
        
        # Only allow alphanumeric and underscores
        if not db_name.replace('_', '').isalnum():
            raise forms.ValidationError("Database name can only contain letters, numbers, and underscores.")
        
        # Check uniqueness
        queryset = Client.objects.filter(db_name=db_name)
        if queryset.exists():
            raise forms.ValidationError("This database name is already taken.")
        
        return db_name
    

class ClientDatabaseForm(forms.ModelForm):
    class Meta:
        model = ClientDatabase
        fields = [
            'connection_name', 'db_type', 'host', 'port', 
            'username', 'password', 'database_name', 'is_primary'
        ]
        widgets = {
            'password': forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        }

    def __init__(self, *args, **kwargs):
        self.client = kwargs.pop('client', None)
        super().__init__(*args, **kwargs)
        
        # Customize field attributes
        self.fields['connection_name'].widget.attrs.update({
            'placeholder': 'e.g., Production DB'
        })
        self.fields['host'].widget.attrs.update({
            'placeholder': 'e.g., 192.168.1.100'
        })
        self.fields['database_name'].widget.attrs.update({
            'placeholder': 'e.g., my_database'
        })
        
    def clean_connection_name(self):
        connection_name = self.cleaned_data.get('connection_name', '').strip()
        if not connection_name:
            raise forms.ValidationError("Connection name is required.")
        
        # Check uniqueness for this client
        queryset = ClientDatabase.objects.filter(
            client=self.client,
            connection_name=connection_name
        )
        if self.instance.pk:
            queryset = queryset.exclude(pk=self.instance.pk)
        
        if queryset.exists():
            raise forms.ValidationError("A database connection with this name already exists for this client.")
        
        return connection_name
    
    def clean_port(self):
        port = self.cleaned_data.get('port')
        if port and (port < 1 or port > 65535):
            raise forms.ValidationError("Port must be between 1 and 65535.")
        return port