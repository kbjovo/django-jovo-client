from django import template

register = template.Library()


@register.simple_tag
def has_sink_connector(client):
    """Check if a client has a sink connector (target database) configured."""
    return client.client_databases.filter(is_target=True).exists()