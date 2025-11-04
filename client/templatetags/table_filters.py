from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """
    Get item from dictionary or object attribute.
    Usage in template: {{ row|get_item:column.key }}
    """
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return getattr(dictionary, key, None)

