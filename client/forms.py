from client.models.client import Client
from client.models.database import ClientDatabase
from django import forms


class ClientForm(forms.ModelForm):
    class Meta:
        model = Client
        fields = [
            'name', 'email', 'phone', 'company_name', 'status',
            'address', 'city', 'state', 'country', 'postal_code'
        ]
        widgets = {
            'address': forms.Textarea(attrs={'rows': 3}),
        }

    def clean_name(self):
        name = self.cleaned_data.get('name', '').strip()
        if len(name) < 2:
            raise forms.ValidationError("Name must be at least 2 characters long.")
        return name

    def clean_email(self):
        email = self.cleaned_data.get('email', '').strip()
        if not email:
            raise forms.ValidationError("Email is required.")
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
    

class ClientDatabaseForm(forms.ModelForm):
    class Meta:
        model = ClientDatabase
        fields = [
            'connection_name', 'db_type', 'host', 'port', 
            'username', 'password', 'database_name', 
            'oracle_connection_mode',  # ✅ NEW: Oracle mode field
            'is_primary'
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
            'placeholder': 'e.g., 192.168.1.100 or localhost'
        })
        self.fields['database_name'].widget.attrs.update({
            'placeholder': 'e.g., my_database'
        })

        # ✅ Make oracle_connection_mode not required (will auto-default to 'service')
        self.fields['oracle_connection_mode'].required = False

        # ✅ Make password not required in update mode (can leave blank to keep existing)
        if self.instance and self.instance.pk:
            self.fields['password'].required = False
            self.fields['password'].widget.attrs['placeholder'] = '••••••••'

        # ✅ Add helpful help text
        self.fields['database_name'].help_text = (
            "For MySQL/PostgreSQL: database name. "
            "For Oracle Service: service name (e.g., XEPDB1). "
            "For Oracle SID: SID name (e.g., XE)"
        )
        
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
    
    def clean_db_type(self):
        """
        Validate database type based on whether this is a target database
        """
        import logging
        logger = logging.getLogger(__name__)

        db_type = self.cleaned_data.get('db_type')

        # ✅ Check if this is a target database (either existing or being created as target)
        is_target = False
        if self.instance and self.instance.pk:
            # Editing existing database - check if it's a target
            is_target = self.instance.is_target
            logger.info(f"Form validation - Instance ID: {self.instance.pk}, is_target: {is_target}, db_type: {db_type}")

        # ✅ Restrict target databases to MySQL and PostgreSQL only
        if is_target and db_type not in ['mysql', 'postgresql']:
            logger.warning(f"Validation failed - Target database with invalid type: {db_type}")
            raise forms.ValidationError(
                f"Target databases can only be MySQL or PostgreSQL. "
                f"You selected: {dict(ClientDatabase.DB_TYPE_CHOICES).get(db_type, db_type)}. "
                f"Please choose MySQL or PostgreSQL."
            )

        return db_type

    def clean(self):
        """
        Cross-field validation for Oracle connections and target database restrictions
        """
        cleaned_data = super().clean()
        db_type = cleaned_data.get('db_type')
        oracle_mode = cleaned_data.get('oracle_connection_mode')
        database_name = cleaned_data.get('database_name')

        # ✅ Auto-set oracle_connection_mode to 'service' if Oracle and not set
        if db_type == 'oracle':
            if not oracle_mode:
                cleaned_data['oracle_connection_mode'] = 'service'

            # ✅ Provide helpful hints for common Oracle database names
            if database_name:
                db_upper = database_name.upper()
                mode = oracle_mode or 'service'

                # Warn if using common PDB name with SID mode
                if mode == 'sid' and db_upper in ['XEPDB1', 'ORCLPDB1', 'PDB1']:
                    self.add_error('oracle_connection_mode',
                        f"'{database_name}' appears to be a pluggable database (PDB). "
                        f"Consider using 'Service Name' mode instead of 'SID'."
                    )

                # Warn if using common SID with service mode
                if mode == 'service' and db_upper in ['XE', 'ORCL'] and 'PDB' not in db_upper:
                    # This is just a warning, not an error
                    pass  # User might be intentionally using XE as a service

        return cleaned_data


class SinkConnectorForm(forms.ModelForm):
    """Form for adding/editing the sink connector (target database)."""

    SINK_DB_TYPE_CHOICES = [
        ("mysql", "MySQL"),
        ("postgresql", "PostgreSQL"),
    ]

    db_type = forms.ChoiceField(choices=SINK_DB_TYPE_CHOICES)

    class Meta:
        model = ClientDatabase
        fields = [
            'connection_name', 'db_type', 'host', 'port',
            'username', 'password', 'database_name',
        ]
        widgets = {
            'password': forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        }

    def __init__(self, *args, **kwargs):
        self.client = kwargs.pop('client', None)
        super().__init__(*args, **kwargs)

        self.fields['connection_name'].widget.attrs.update({
            'placeholder': 'e.g., Target Production DB'
        })
        self.fields['host'].widget.attrs.update({
            'placeholder': 'e.g., 192.168.1.100 or localhost'
        })
        self.fields['database_name'].widget.attrs.update({
            'placeholder': 'e.g., my_target_db'
        })

        if self.instance and self.instance.pk:
            self.fields['password'].required = False
            self.fields['password'].widget.attrs['placeholder'] = '••••••••'

    def clean_connection_name(self):
        connection_name = self.cleaned_data.get('connection_name', '').strip()
        if not connection_name:
            raise forms.ValidationError("Connection name is required.")

        queryset = ClientDatabase.objects.filter(
            client=self.client,
            connection_name=connection_name
        )
        if self.instance.pk:
            queryset = queryset.exclude(pk=self.instance.pk)

        if queryset.exists():
            raise forms.ValidationError("A connection with this name already exists for this client.")

        return connection_name

    def clean_port(self):
        port = self.cleaned_data.get('port')
        if port and (port < 1 or port > 65535):
            raise forms.ValidationError("Port must be between 1 and 65535.")
        return port