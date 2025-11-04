"""
Automatic table builder utility.
Auto-detects fields from queryset data and generates configuration.
"""

from django.core.paginator import Paginator
from django.db.models import Q, TextField


def format_field_label(field_name):
    """
    Convert field name to proper label.
    Example: 'company_name' -> 'Company Name'
    
    Args:
        field_name: Field name string
        
    Returns:
        str: Formatted label
    """
    return field_name.replace('_', ' ').title()


def auto_detect_field_type(model, field_name):
    """
    Automatically detect field type from Django model field.
    
    Args:
        model: Django model class
        field_name: Name of the field
        
    Returns:
        str: Field type for table rendering
    """
    try:
        field = model._meta.get_field(field_name)
        
        # Check field type
        from django.db.models import DateTimeField, DateField, BooleanField, CharField, EmailField, TextField
        
        if isinstance(field, (DateTimeField, DateField)):
            return 'date'
        elif isinstance(field, BooleanField):
            return 'badge'
        elif isinstance(field, TextField):
            return 'text'
        elif isinstance(field, (CharField, EmailField)):
            # Check if it has choices (like status field)
            if field.choices:
                return 'badge'
            return 'text'
        else:
            return 'text'
    except:
        return 'text'


def is_field_sortable(model, field_name):
    """
    Determine if a field should be sortable.
    TextFields are not sortable by default.
    
    Args:
        model: Django model class
        field_name: Name of the field
        
    Returns:
        bool: True if sortable
    """
    try:
        field = model._meta.get_field(field_name)
        return not isinstance(field, TextField)
    except:
        return True


def build_paginated_table(queryset, request, config):
    """
    Build a complete table configuration with automatic field detection from data.
    
    Args:
        queryset: Django QuerySet to paginate
        request: Django request object
        config: Minimal table configuration dict
        
    Returns:
        dict: Context data for rendering table
    """
    
    # Get model from queryset
    model = queryset.model
    
    # Get excluded fields
    exclude_fields = config.get('exclude', [])
    
    # Search functionality
    search_query = request.GET.get('search', '')
    if search_query and 'searchable' in config:
        search_filters = Q()
        for field in config['searchable']:
            search_filters |= Q(**{f"{field}__icontains": search_query})
        queryset = queryset.filter(search_filters)
    
    # Sorting
    sort_by = request.GET.get('sort', '')
    order = request.GET.get('order', 'asc')
    
    # Validate and apply sorting
    if sort_by and is_field_sortable(model, sort_by):
        if order == 'desc':
            sort_by = f'-{sort_by}'
        queryset = queryset.order_by(sort_by)
    
    # Pagination
    per_page = config.get('per_page', 10)
    paginator = Paginator(queryset, per_page)
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)
    
    # Convert queryset to list of dicts
    rows = []
    for obj in page_obj:
        row = {'id': obj.id}  # Always include ID for URLs and checkboxes
        
        for field in model._meta.get_fields():
            # Skip reverse relations and many-to-many
            if field.one_to_many or field.many_to_many:
                continue
            
            # Skip excluded fields (but we already added id above)
            if field.name in exclude_fields:
                continue
            
            field_name = field.name
            field_value = getattr(obj, field_name, None)
            
            # If it's a related object (ForeignKey), convert to string
            if hasattr(field_value, '__class__') and hasattr(field_value.__class__, '_meta'):
                field_value = str(field_value)
            
            row[field_name] = field_value
        
        rows.append(row)
    
    # Auto-detect columns from first row
    cols = list(rows[0].keys()) if rows else []
    
    # Get column overrides
    column_overrides = config.get('column_overrides', {})
    
    # Build columns configuration from detected columns
    columns = []
    for field_name in cols:
        # Check if there's an override for this field
        override = column_overrides.get(field_name, {})
        
        # Auto-detect field type if not overridden
        field_type = override.get('type', auto_detect_field_type(model, field_name))
        
        # Auto-generate label if not overridden
        label = override.get('label', format_field_label(field_name))
        
        # Determine if sortable
        sortable = override.get('sortable', is_field_sortable(model, field_name))
        
        # Build column config
        column_config = {
            'key': field_name,
            'label': label,
            'type': field_type,
            'sortable': sortable,
        }
        
        # Add additional override properties
        for key, value in override.items():
            if key not in ['label', 'type', 'sortable']:
                column_config[key] = value
        
        columns.append(column_config)
    
    # Build final table config
    table_config = {
        'columns': columns,
        'rows': rows,
        'settings': {
            'searchable': 'searchable' in config and len(config['searchable']) > 0,
            'sortable': True,
            'paginate': True,
            'per_page': per_page,
            'selectable': config.get('selectable', True),
            'empty_message': config.get('empty_message', 'No data available'),
            'detail_url_name': config.get('detail_url_name', ''),
        }
    }
    
    return {
        'config': table_config,
        'page_obj': page_obj,
    }