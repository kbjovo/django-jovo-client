"""
Logging utility functions for structured logging
Create this file: client/logging_utils.py
"""
import logging
import time
from functools import wraps
from contextlib import contextmanager

# Get loggers for different parts of the application
cdc_logger = logging.getLogger('client.cdc')
kafka_logger = logging.getLogger('client.kafka')
db_logger = logging.getLogger('client.database')
app_logger = logging.getLogger('client')


def log_with_context(logger, level, message, **context):
    """
    Log a message with additional context fields
    
    Args:
        logger: The logger instance to use
        level: Log level (INFO, ERROR, WARNING, etc.)
        message: The log message
        **context: Additional context fields (client_id, table_name, etc.)
    
    Example:
        log_with_context(
            cdc_logger, 
            'INFO', 
            'CDC connector created successfully',
            client_id=123,
            connector_name='mysql-connector-1',
            duration=2.5
        )
    """
    # Create a LogRecord with extra fields
    extra = {k: v for k, v in context.items() if v is not None}
    logger.log(getattr(logging, level.upper()), message, extra=extra)


@contextmanager
def log_operation(logger, operation_name, **context):
    """
    Context manager to log the start, end, and duration of an operation
    
    Example:
        with log_operation(cdc_logger, 'table_discovery', client_id=123, database='mydb'):
            tables = discover_tables()
    """
    start_time = time.time()
    
    # Log operation start
    log_with_context(
        logger,
        'INFO',
        f'{operation_name} started',
        operation=operation_name,
        **context
    )
    
    try:
        yield
        
        # Log operation success
        duration = time.time() - start_time
        log_with_context(
            logger,
            'INFO',
            f'{operation_name} completed successfully',
            operation=operation_name,
            duration=duration,
            status='success',
            **context
        )
        
    except Exception as e:
        # Log operation failure
        duration = time.time() - start_time
        log_with_context(
            logger,
            'ERROR',
            f'{operation_name} failed: {str(e)}',
            operation=operation_name,
            duration=duration,
            status='failed',
            error_type=type(e).__name__,
            error_message=str(e),
            **context
        )
        raise


def log_decorator(logger, operation_name=None):
    """
    Decorator to automatically log function execution
    
    Example:
        @log_decorator(cdc_logger, 'create_connector')
        def create_connector(client_id, config):
            # Your logic here
            pass
    """
    def decorator(func):
        op_name = operation_name or func.__name__
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Try to extract context from function arguments
            context = {}
            if 'client_id' in kwargs:
                context['client_id'] = kwargs['client_id']
            if 'connector_name' in kwargs:
                context['connector_name'] = kwargs['connector_name']
            if 'table_name' in kwargs:
                context['table_name'] = kwargs['table_name']
            
            log_with_context(
                logger,
                'INFO',
                f'{op_name} started',
                operation=op_name,
                **context
            )
            
            try:
                result = func(*args, **kwargs)
                
                duration = time.time() - start_time
                log_with_context(
                    logger,
                    'INFO',
                    f'{op_name} completed successfully',
                    operation=op_name,
                    duration=duration,
                    status='success',
                    **context
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                log_with_context(
                    logger,
                    'ERROR',
                    f'{op_name} failed: {str(e)}',
                    operation=op_name,
                    duration=duration,
                    status='failed',
                    error_type=type(e).__name__,
                    error_message=str(e),
                    **context
                )
                raise
        
        return wrapper
    return decorator


# ====================================
# CDC-SPECIFIC LOGGING FUNCTIONS
# ====================================

def log_connector_created(client_id, connector_name, config, duration=None):
    """Log Debezium connector creation"""
    log_with_context(
        cdc_logger,
        'INFO',
        'Debezium connector created',
        client_id=client_id,
        connector_name=connector_name,
        operation='connector_create',
        duration=duration,
        tables_count=len(config.get('table.include.list', '').split(',')) if config.get('table.include.list') else 0
    )


def log_connector_deleted(client_id, connector_name, duration=None):
    """Log Debezium connector deletion"""
    log_with_context(
        cdc_logger,
        'INFO',
        'Debezium connector deleted',
        client_id=client_id,
        connector_name=connector_name,
        operation='connector_delete',
        duration=duration
    )


def log_connector_error(client_id, connector_name, error, operation='unknown'):
    """Log Debezium connector errors"""
    log_with_context(
        cdc_logger,
        'ERROR',
        f'Connector operation failed: {str(error)}',
        client_id=client_id,
        connector_name=connector_name,
        operation=operation,
        error_type=type(error).__name__,
        error_message=str(error)
    )


def log_table_discovery(client_id, database_name, tables_count, duration=None):
    """Log table discovery operation"""
    log_with_context(
        db_logger,
        'INFO',
        f'Discovered {tables_count} tables',
        client_id=client_id,
        database_name=database_name,
        operation='table_discovery',
        tables_count=tables_count,
        duration=duration
    )


def log_replication_event(client_id, table_name, operation, event_count=1):
    """Log CDC replication events"""
    log_with_context(
        cdc_logger,
        'INFO',
        f'Processed {operation} event',
        client_id=client_id,
        table_name=table_name,
        operation=operation,
        event_count=event_count
    )


def log_kafka_message(topic, partition, offset, consumer_group, processing_time=None):
    """Log Kafka message consumption"""
    log_with_context(
        kafka_logger,
        'INFO',
        'Kafka message processed',
        topic=topic,
        partition=partition,
        offset=offset,
        consumer_group=consumer_group,
        operation='message_consume',
        duration=processing_time
    )


def log_replication_lag(client_id, connector_name, lag_seconds):
    """Log replication lag"""
    level = 'WARNING' if lag_seconds > 60 else 'INFO'
    log_with_context(
        cdc_logger,
        level,
        f'Replication lag: {lag_seconds}s',
        client_id=client_id,
        connector_name=connector_name,
        operation='lag_check',
        lag_seconds=lag_seconds
    )


def log_database_connection(database_type, host, status, duration=None, error=None):
    """Log database connection attempts"""
    level = 'INFO' if status == 'success' else 'ERROR'
    message = f'Database connection {status}'
    
    context = {
        'database_type': database_type,
        'host': host,
        'operation': 'db_connection_test',
        'status': status,
        'duration': duration
    }
    
    if error:
        context['error_type'] = type(error).__name__
        context['error_message'] = str(error)
    
    log_with_context(db_logger, level, message, **context)


# ====================================
# CELERY TASK LOGGING
# ====================================

def log_celery_task_start(task_name, task_id, queue, **kwargs):
    """Log Celery task start"""
    log_with_context(
        logging.getLogger('celery'),
        'INFO',
        f'Celery task started: {task_name}',
        task_name=task_name,
        task_id=task_id,
        queue=queue,
        operation='task_start',
        **kwargs
    )


def log_celery_task_complete(task_name, task_id, queue, duration, **kwargs):
    """Log Celery task completion"""
    log_with_context(
        logging.getLogger('celery'),
        'INFO',
        f'Celery task completed: {task_name}',
        task_name=task_name,
        task_id=task_id,
        queue=queue,
        operation='task_complete',
        duration=duration,
        status='success',
        **kwargs
    )


def log_celery_task_error(task_name, task_id, queue, error, duration, **kwargs):
    """Log Celery task error"""
    log_with_context(
        logging.getLogger('celery'),
        'ERROR',
        f'Celery task failed: {task_name}',
        task_name=task_name,
        task_id=task_id,
        queue=queue,
        operation='task_error',
        duration=duration,
        status='failed',
        error_type=type(error).__name__,
        error_message=str(error),
        **kwargs
    )