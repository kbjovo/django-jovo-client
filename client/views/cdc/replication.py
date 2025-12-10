"""
CDC Replication Control Views.

Handles replication lifecycle management:
- Start replication
- Stop replication
- Restart replication
- Replication status
"""

import logging
import time
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages

from client.models.replication import ReplicationConfig
from client.utils.debezium_manager import DebeziumConnectorManager
from client.tasks import restart_replication
from client.replication import ReplicationOrchestrator

logger = logging.getLogger(__name__)
@require_http_methods(["POST"])
def start_replication(request, config_id):
    """Start CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Check if already running
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator for simplified flow (Option A)
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        # Get force_resync parameter from request (default: False)
        force_resync = request.POST.get('force_resync', 'false').lower() == 'true'

        logger.info(f"Starting replication for config {config_id} (force_resync={force_resync})")

        # Start replication with new simplified flow
        # This will:
        # 1. Validate prerequisites
        # 2. Perform initial SQL copy
        # 3. Start Debezium connector (CDC-only mode)
        # 4. Start consumer with fresh group ID
        success, message = orchestrator.start_replication(force_resync=force_resync)

        if success:
            messages.success(
                request,
                f"🚀 Replication started successfully! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to start replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to start replication: {e}", exc_info=True)
        messages.error(request, f"Failed to start replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator to stop replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Stopping replication for config {config_id}")

        success, message = orchestrator.stop_replication()

        if success:
            messages.success(
                request,
                f"⏸️ Replication stopped! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to stop replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to stop replication: {e}", exc_info=True)
        messages.error(request, f"Failed to stop replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator to restart replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Restarting replication for config {config_id}")

        success, message = orchestrator.restart_replication()

        if success:
            messages.success(
                request,
                f"🔄 Replication restarted! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to restart replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to restart replication: {e}", exc_info=True)
        messages.error(request, f"Failed to restart replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["GET"])
def replication_status(request, config_id):
    """Get replication status (AJAX endpoint) - NEW: Uses orchestrator"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator for unified status
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)
        unified_status = orchestrator.get_unified_status()

        # Return comprehensive status
        return JsonResponse({
            'success': True,
            'status': unified_status
        })

    except Exception as e:
        logger.error(f"Failed to get replication status: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)



def wait_for_connector_running(manager, connector_name, timeout=30, interval=2):
    end_time = time.time() + timeout
    while time.time() < end_time:
        status, details = manager.get_connector_status(connector_name)
        if status:
            if all(task['state'] == 'RUNNING' for task in details.get('tasks', [])):
                logger.info("✅ All connector tasks are RUNNING.")
                return True
        time.sleep(interval)
        logger.info(f"⏳ Waiting for connector tasks to stabilize. Current status: {status}")
    logger.error(f"❌ Connector tasks did not stabilize within {timeout} seconds.")
    return False


