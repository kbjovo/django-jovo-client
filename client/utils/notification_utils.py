"""
Notification and error handling utilities
"""

import logging
import traceback
from typing import Optional, Dict, Any, List
from django.core.mail import send_mail, EmailMultiAlternatives
from django.conf import settings
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from datetime import datetime

logger = logging.getLogger(__name__)


class EmailNotificationError(Exception):
    """Raised when email notification fails"""
    pass


def send_error_notification(
    error_title: str,
    error_message: str,
    context: Optional[Dict[str, Any]] = None,
    recipients: Optional[List[str]] = None,
    include_traceback: bool = True
) -> bool:
    """
    Send error notification email to administrators
    
    Args:
        error_title: Brief title of the error
        error_message: Detailed error message
        context: Additional context information
        recipients: List of email addresses (defaults to settings.ADMINS)
        include_traceback: Whether to include stack trace
        
    Returns:
        bool: True if email sent successfully
    """
    try:
        # Get recipients
        if recipients is None:
            recipients = [admin[1] for admin in getattr(settings, 'ADMINS', [])]
        
        if not recipients:
            logger.warning("No recipients configured for error notifications")
            return False
        
        # Build context
        email_context = {
            'error_title': error_title,
            'error_message': error_message,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'environment': getattr(settings, 'ENVIRONMENT', 'development'),
            'context': context or {},
        }
        
        if include_traceback:
            email_context['traceback'] = traceback.format_exc()
        
        # Create subject
        subject = f"[Django Replication] ERROR: {error_title}"
        
        # Create plain text message
        message = f"""
ERROR: {error_title}

Time: {email_context['timestamp']}
Environment: {email_context['environment']}

Message:
{error_message}

Context:
{context or 'No additional context'}

{"Traceback:" if include_traceback else ""}
{email_context.get('traceback', '')}
        """.strip()
        
        # Send email
        send_mail(
            subject=subject,
            message=message,
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@example.com'),
            recipient_list=recipients,
            fail_silently=False,
        )
        
        logger.info(f"Error notification sent: {error_title}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send error notification: {str(e)}")
        return False


def send_replication_status_email(
    client_name: str,
    status: str,
    details: Dict[str, Any],
    recipients: List[str],
    success: bool = True
) -> bool:
    """
    Send replication status notification email
    
    Args:
        client_name: Name of the client
        status: Status message
        details: Detailed information about the replication
        recipients: List of email addresses
        success: Whether the replication was successful
        
    Returns:
        bool: True if email sent successfully
    """
    try:
        if not recipients:
            logger.warning(f"No recipients for replication status: {client_name}")
            return False
        
        # Create subject
        status_emoji = "✅" if success else "❌"
        subject = f"{status_emoji} Replication Status: {client_name} - {status}"
        
        # Create context
        context = {
            'client_name': client_name,
            'status': status,
            'success': success,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'details': details,
        }
        
        # Create plain text message
        message = f"""
Replication Status for {client_name}

Status: {status}
Time: {context['timestamp']}
Result: {"Success" if success else "Failed"}

Details:
"""
        
        for key, value in details.items():
            message += f"  {key}: {value}\n"
        
        # Send email
        send_mail(
            subject=subject,
            message=message,
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@example.com'),
            recipient_list=recipients,
            fail_silently=False,
        )
        
        logger.info(f"Replication status email sent for: {client_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send replication status email: {str(e)}")
        return False


def log_and_notify_error(
    logger_instance: logging.Logger,
    error_title: str,
    exception: Exception,
    context: Optional[Dict[str, Any]] = None,
    notify: bool = True
) -> None:
    """
    Log error and optionally send notification
    
    Args:
        logger_instance: Logger instance to use
        error_title: Brief title of the error
        exception: Exception that occurred
        context: Additional context information
        notify: Whether to send email notification
        
    Example:
        try:
            risky_operation()
        except Exception as e:
            log_and_notify_error(
                logger,
                "Risky Operation Failed",
                e,
                context={'client_id': 123},
                notify=True
            )
    """
    error_message = str(exception)
    
    # Log the error
    logger_instance.error(
        f"{error_title}: {error_message}",
        extra={'context': context},
        exc_info=True
    )
    
    # Send notification if requested
    if notify:
        send_error_notification(
            error_title=error_title,
            error_message=error_message,
            context=context,
            include_traceback=True
        )


def send_connector_status_email(
    connector_name: str,
    status: str,
    client_name: str,
    database_name: str,
    error_message: Optional[str] = None,
    recipients: Optional[List[str]] = None
) -> bool:
    """
    Send Debezium connector status notification
    
    Args:
        connector_name: Name of the connector
        status: Connector status (RUNNING, FAILED, etc.)
        client_name: Client name
        database_name: Database name
        error_message: Error message if status is FAILED
        recipients: List of email addresses
        
    Returns:
        bool: True if email sent successfully
    """
    try:
        # Get recipients
        if recipients is None:
            recipients = [admin[1] for admin in getattr(settings, 'ADMINS', [])]
        
        if not recipients:
            logger.warning("No recipients configured for connector notifications")
            return False
        
        # Determine if it's a success or failure
        success = status.upper() == 'RUNNING'
        status_emoji = "✅" if success else "❌"
        
        # Create subject
        subject = f"{status_emoji} Debezium Connector: {connector_name} - {status}"
        
        # Create message
        message = f"""
Debezium Connector Status Update

Connector: {connector_name}
Client: {client_name}
Database: {database_name}
Status: {status}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{f"Error: {error_message}" if error_message else "Connector is operating normally"}
        """.strip()
        
        # Send email
        send_mail(
            subject=subject,
            message=message,
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@example.com'),
            recipient_list=recipients,
            fail_silently=False,
        )
        
        logger.info(f"Connector status email sent: {connector_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send connector status email: {str(e)}")
        return False


def send_batch_notification(
    notifications: List[Dict[str, Any]],
    recipients: List[str],
    subject_prefix: str = "Batch Notification"
) -> bool:
    """
    Send multiple notifications in a single email
    
    Args:
        notifications: List of notification dictionaries with keys:
                       - title: str
                       - message: str
                       - status: str (success/error/warning)
        recipients: List of email addresses
        subject_prefix: Prefix for email subject
        
    Returns:
        bool: True if email sent successfully
    """
    try:
        if not recipients or not notifications:
            return False
        
        # Count statuses
        error_count = sum(1 for n in notifications if n.get('status') == 'error')
        warning_count = sum(1 for n in notifications if n.get('status') == 'warning')
        success_count = sum(1 for n in notifications if n.get('status') == 'success')
        
        # Create subject
        subject = f"{subject_prefix} - {error_count} errors, {warning_count} warnings, {success_count} success"
        
        # Create message
        message = f"""
Batch Notification Summary

Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Notifications: {len(notifications)}
Errors: {error_count}
Warnings: {warning_count}
Success: {success_count}

Details:
{'=' * 60}

"""
        
        for i, notification in enumerate(notifications, 1):
            status_emoji = {
                'success': '✅',
                'warning': '⚠️',
                'error': '❌',
            }.get(notification.get('status', 'info'), 'ℹ️')
            
            message += f"{i}. {status_emoji} {notification.get('title', 'Untitled')}\n"
            message += f"   {notification.get('message', 'No message')}\n"
            message += f"{'-' * 60}\n\n"
        
        # Send email
        send_mail(
            subject=subject,
            message=message,
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@example.com'),
            recipient_list=recipients,
            fail_silently=False,
        )
        
        logger.info(f"Batch notification sent with {len(notifications)} items")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send batch notification: {str(e)}")
        return False