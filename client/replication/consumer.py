"""
Resilient Kafka Consumer with smart auto-restart.

Wraps the existing DebeziumCDCConsumer with:
- Automatic retry with exponential backoff
- Heartbeat tracking for health monitoring
- Structured logging for UI display
- Error classification (transient vs persistent)
"""

import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from django.utils import timezone

from client.utils.kafka_consumer import DebeziumCDCConsumer, KafkaConsumerException

logger = logging.getLogger(__name__)


class ConsumerError(Exception):
    """Base exception for consumer errors."""
    pass


class TransientError(ConsumerError):
    """Temporary error that should be retried."""
    pass


class PersistentError(ConsumerError):
    """Permanent error that requires manual intervention."""
    pass


class ResilientKafkaConsumer:
    """
    Wraps DebeziumCDCConsumer with auto-restart and health monitoring.

    Features:
    - Smart retry with exponential backoff (max 5 retries)
    - Heartbeat updates every 30 seconds
    - Error classification (transient vs persistent)
    - Structured logging for SSE/UI display
    """

    # Retry configuration
    MAX_RETRIES = 5
    BASE_BACKOFF_SECONDS = 2  # 2, 4, 8, 16, 32 seconds
    HEARTBEAT_INTERVAL_SECONDS = 30

    COMMIT_INTERVAL_MESSAGES = 100  # Commit every N messages
    COMMIT_INTERVAL_SECONDS = 5     # Or every N seconds
    LOG_PROGRESS_INTERVAL = 1000 

    def __init__(
        self,
        replication_config,
        consumer_group_id: str,
        topics: list,
        target_engine,
        bootstrap_servers: str = "localhost:9092",
    ):
        """
        Initialize resilient consumer.

        Args:
            replication_config: ReplicationConfig model instance
            consumer_group_id: Kafka consumer group ID
            topics: List of Kafka topics to consume
            target_engine: SQLAlchemy engine for target database
            bootstrap_servers: Kafka bootstrap servers
        """
        self.config = replication_config
        self.consumer_group_id = consumer_group_id
        self.topics = topics
        self.target_engine = target_engine
        self.bootstrap_servers = bootstrap_servers

        # Internal state
        self.consumer = None
        self.retry_count = 0
        self.last_heartbeat = None
        self.last_error = None
        self.is_running = False
        self.should_stop = False

        # Statistics
        self.stats = {
            'started_at': timezone.now(),
            'messages_processed': 0,
            'errors_encountered': 0,
            'retries_attempted': 0,
            'last_message_at': None,
        }

    def start(self):
        """
        Start consuming with auto-restart on failure.

        This is the main entry point. It will:
        1. Create consumer instance
        2. Start consumption loop
        3. Auto-retry on transient errors
        4. Stop and alert on persistent errors
        """
        self._log_info("Starting resilient Kafka consumer...")
        self.is_running = True
        self.should_stop = False

        while not self.should_stop:
            try:
                self._log_info(f"STEP 1/3: Initializing consumer (attempt {self.retry_count + 1}/{self.MAX_RETRIES})")
                self._initialize_consumer()

                self._log_info(f"STEP 2/3: Starting consumption from {len(self.topics)} topics")
                self._consume_with_heartbeat()

                # If we exit consumption normally, stop
                self._log_info("Consumption completed normally")
                break

            except TransientError as e:
                self._handle_transient_error(e)

            except PersistentError as e:
                self._handle_persistent_error(e)
                break

            except Exception as e:
                # Unknown error - classify it
                error_class = self._classify_error(e)
                if error_class == TransientError:
                    self._handle_transient_error(TransientError(str(e)))
                else:
                    self._handle_persistent_error(PersistentError(str(e)))
                    break

        self.is_running = False
        self._log_info("Consumer stopped")

    def stop(self):
        """Gracefully stop the consumer."""
        self._log_info("Stop signal received")
        self.should_stop = True
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer: {e}")


    def _initialize_consumer(self):
        """Create DebeziumCDCConsumer instance."""
        try:

            # Create the Debezium consumer using the normalized topics
            self.consumer = DebeziumCDCConsumer(
                consumer_group_id=self.consumer_group_id,
                topics=self.topics,
                target_engine=self.target_engine,
                replication_config=self.config,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
            )
        
            # Force a short poll to trigger group join / partition assignment immediately
            try:
                # poll a tiny amount to ensure group join happens and assignment is created in Kafka
                self.consumer.consumer.poll(0.1)
            except Exception:
                # ignore poll errors here but continue — poll will be used in the main loop
                pass

            # Log the actual topics used for consumption (use normalized_topics)
            self._log_info(f"✓ Consumer initialized successfully with topics: {self.topics}")

        except Exception as e:
            error_msg = f"Failed to initialize consumer: {str(e)}"
            self._log_error(error_msg)
            raise self._classify_error(e)(error_msg)

    def _consume_with_heartbeat(self):
        """
        OPTIMIZED consumption loop with batching and async commits.
        
        Key optimizations:
        1. Remove blocking debug sleeps
        2. Async offset commits with periodic sync
        3. Reduced logging frequency
        4. Batch progress tracking
        """
        last_heartbeat_time = time.time()
        last_commit_time = time.time()
        last_log_time = time.time()
        
        message_count = 0
        messages_since_commit = 0
        no_message_count = 0
        consecutive_errors = 0
        
        try:
            self._log_info(f"✓ Starting OPTIMIZED consumption loop")
            self._log_info(f"   Topics: {', '.join(self.topics)}")
            self._log_info(f"   Consumer group: {self.consumer.consumer_group_id}")
            self._log_info(f"   Commit interval: {self.COMMIT_INTERVAL_MESSAGES} msgs or {self.COMMIT_INTERVAL_SECONDS}s")
            
            # Wait for initial partition assignment (one-time check)
            max_wait = 10
            waited = 0
            while not self.consumer.consumer.assignment() and waited < max_wait:
                time.sleep(0.5)
                waited += 0.5
            
            if self.consumer.consumer.assignment():
                self._log_info(f"✓ Assigned partitions: {self.consumer.consumer.assignment()}")
            else:
                self._log_warning("⚠️ No partitions assigned after 10s - continuing anyway")

            while not self.should_stop:
                current_time = time.time()
                
                # ============================================
                # HEARTBEAT UPDATE (every 30s)
                # ============================================
                if current_time - last_heartbeat_time >= self.HEARTBEAT_INTERVAL_SECONDS:
                    self._update_heartbeat()
                    last_heartbeat_time = current_time

                # ============================================
                # POLL FOR MESSAGE (non-blocking with 1s timeout)
                # ============================================
                try:
                    msg = self.consumer.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        no_message_count += 1
                        
                        # Log "waiting" every 30 seconds only
                        if current_time - last_log_time >= 30:
                            self._log_info(f"⏳ Waiting for messages... (no messages in last 30s)")
                            last_log_time = current_time
                            no_message_count = 0
                        
                        # PERIODIC COMMIT: Commit even when no messages (every N seconds)
                        if current_time - last_commit_time >= self.COMMIT_INTERVAL_SECONDS:
                            if messages_since_commit > 0:
                                self._commit_offsets(asynchronous=False)
                                messages_since_commit = 0
                                last_commit_time = current_time
                        
                        continue

                    # Reset counters when we receive messages
                    no_message_count = 0

                    # Handle Kafka errors
                    if msg.error():
                        self._handle_kafka_error(msg.error())
                        continue

                    # ============================================
                    # PROCESS MESSAGE
                    # ============================================
                    self.consumer.process_message(msg)
                    message_count += 1
                    messages_since_commit += 1
                    self.stats['messages_processed'] += 1
                    self.stats['last_message_at'] = timezone.now()
                    consecutive_errors = 0  # Reset error counter on success

                    # ============================================
                    # ASYNC COMMIT (every N messages OR N seconds)
                    # ============================================
                    should_commit = (
                        messages_since_commit >= self.COMMIT_INTERVAL_MESSAGES or
                        (current_time - last_commit_time) >= self.COMMIT_INTERVAL_SECONDS
                    )
                    
                    if should_commit:
                        # Use async commit for better throughput
                        self._commit_offsets(asynchronous=True)
                        messages_since_commit = 0
                        last_commit_time = current_time

                    # ============================================
                    # PROGRESS LOGGING (reduced frequency)
                    # ============================================
                    if message_count % self.LOG_PROGRESS_INTERVAL == 0:
                        elapsed = (current_time - self.stats['started_at'].timestamp())
                        rate = message_count / elapsed if elapsed > 0 else 0
                        self._log_info(
                            f"📊 Progress: {message_count:,} msgs "
                            f"({rate:.1f} msgs/sec, total: {self.stats['messages_processed']:,})"
                        )

                except Exception as e:
                    # Error processing message
                    consecutive_errors += 1
                    self.stats['errors_encountered'] += 1
                    
                    # Log error but don't spam logs
                    if consecutive_errors == 1 or consecutive_errors % 10 == 0:
                        self._log_error(f"❌ Error processing message: {str(e)}")
                        logger.exception(f"[{self.config.connector_name}] Exception details:")
                    
                    # If too many consecutive errors, raise
                    if consecutive_errors >= 10:
                        self._log_error(f"⚠️ 10 consecutive errors - may indicate persistent issue")
                        raise TransientError(f"Multiple consecutive message processing errors: {str(e)}")

            # ============================================
            # FINAL COMMIT ON EXIT
            # ============================================
            if messages_since_commit > 0:
                self._log_info(f"Final commit: {messages_since_commit} pending messages")
                self._commit_offsets(asynchronous=False)

        except KeyboardInterrupt:
            self._log_info("ℹ️ Consumption interrupted by user")
        except Exception as e:
            self._log_error(f"❌ Consumption loop error: {str(e)}")
            raise

    def _commit_offsets(self, asynchronous: bool = True):
        """
        Commit consumer offsets.
        
        Args:
            asynchronous: If True, commit async (fire-and-forget, faster).
                         If False, commit sync (blocks until acknowledged, safer).
        """
        try:
            self.consumer.consumer.commit(asynchronous=asynchronous)
            if not asynchronous:
                logger.debug(f"[{self.config.connector_name}] Offset committed (sync)")
        except Exception as e:
            logger.warning(f"Failed to commit offset: {e}")

    def _handle_kafka_error(self, error):
        """Handle Kafka-specific errors."""
        from confluent_kafka import KafkaError

        if error.code() == KafkaError._PARTITION_EOF:
            # End of partition - not an error
            return
        elif error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            # Topic doesn't exist yet - wait for it
            logger.debug(f"Topic not available yet: {error}")
            return
        else:
            # Other Kafka error
            self._log_error(f"Kafka error: {error}")
            raise TransientError(f"Kafka error: {error}")

    def _update_heartbeat(self):
        """Update heartbeat timestamp in database."""
        try:
            self.last_heartbeat = timezone.now()

            from client.models import ReplicationConfig
            ReplicationConfig.objects.filter(id=self.config.id).update(
                consumer_last_heartbeat=self.last_heartbeat
            )

            # Reduced logging frequency
            logger.debug(
                f"💚 Heartbeat (msgs: {self.stats['messages_processed']}, "
                f"errors: {self.stats['errors_encountered']})"
            )

        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {e}")

    def _handle_transient_error(self, error: TransientError):
        """
        Handle transient error with exponential backoff retry.

        Args:
            error: TransientError instance
        """
        self.retry_count += 1
        self.stats['retries_attempted'] += 1
        self.last_error = str(error)

        self._log_warning("=" * 60)
        self._log_warning(f"⚠️  TRANSIENT ERROR DETECTED")
        self._log_warning(f"   Error: {error}")
        self._log_warning(f"   Retry attempt: {self.retry_count}/{self.MAX_RETRIES}")
        self._log_warning("=" * 60)

        if self.retry_count >= self.MAX_RETRIES:
            # Max retries exceeded - convert to persistent error
            error_msg = f"Max retries ({self.MAX_RETRIES}) exceeded. Last error: {error}"
            self._log_error(f"❌ {error_msg}")
            self._update_config_status('error', error_msg)
            raise PersistentError(error_msg)

        # Calculate backoff delay: 2, 4, 8, 16, 32 seconds
        backoff_delay = self.BASE_BACKOFF_SECONDS * (2 ** (self.retry_count - 1))

        self._log_warning(f"🔄 Retrying in {backoff_delay} seconds...")

        time.sleep(backoff_delay)
        self._log_info(f"🔄 Retry #{self.retry_count} starting now...")

    def _handle_persistent_error(self, error: PersistentError):
        """
        Handle persistent error that requires manual intervention.

        Args:
            error: PersistentError instance
        """
        error_msg = f"Persistent error - manual intervention required: {error}"
        self._log_error(error_msg)
        self.last_error = error_msg

        # Update config status to error
        self._update_config_status('error', error_msg)

    def _classify_error(self, error: Exception):
        """
        Classify error as transient or persistent.

        Transient errors (should retry):
        - Network timeouts
        - Kafka broker unavailable
        - Database deadlocks
        - Temporary connection issues

        Persistent errors (manual intervention):
        - Authentication failures
        - Missing tables/columns
        - Invalid configuration
        - Permission errors

        Args:
            error: Exception to classify

        Returns:
            TransientError or PersistentError class
        """
        error_str = str(error).lower()

        # Persistent error patterns
        persistent_patterns = [
            'authentication',
            'permission denied',
            'access denied',
            'invalid credentials',
            'table does not exist',
            'column does not exist',
            'no such table',
            'invalid configuration',
        ]

        for pattern in persistent_patterns:
            if pattern in error_str:
                return PersistentError

        # Transient error patterns
        transient_patterns = [
            'timeout',
            'connection refused',
            'connection reset',
            'temporarily unavailable',
            'deadlock',
            'try again',
            'broker not available',
        ]

        for pattern in transient_patterns:
            if pattern in error_str:
                return TransientError

        # Default to transient for unknown errors
        return TransientError

    def _update_config_status(self, status: str, error_message: Optional[str] = None):
        """Update ReplicationConfig status in database."""
        try:
            from client.models import ReplicationConfig

            update_data = {
                'status': status,
                'consumer_state': status.upper(),
            }

            if error_message:
                update_data['last_error_message'] = error_message

            if status == 'error':
                update_data['is_active'] = False

            ReplicationConfig.objects.filter(id=self.config.id).update(**update_data)

        except Exception as e:
            logger.error(f"Failed to update config status: {e}")

    def _log_info(self, message: str):
        """Log info message with structured format for SSE."""
        logger.info(f"[{self.config.connector_name}] {message}")

    def _log_warning(self, message: str):
        """Log warning message with structured format for SSE."""
        logger.warning(f"[{self.config.connector_name}] {message}")

    def _log_error(self, message: str):
        """Log error message with structured format for SSE."""
        logger.error(f"[{self.config.connector_name}] {message}")

    def get_status(self) -> Dict[str, Any]:
        """
        Get current consumer status for monitoring.

        Returns:
            Dictionary with consumer state and statistics
        """
        return {
            'is_running': self.is_running,
            'retry_count': self.retry_count,
            'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            'last_error': self.last_error,
            'stats': {
                'started_at': self.stats['started_at'].isoformat(),
                'messages_processed': self.stats['messages_processed'],
                'errors_encountered': self.stats['errors_encountered'],
                'retries_attempted': self.stats['retries_attempted'],
                'last_message_at': self.stats['last_message_at'].isoformat() if self.stats['last_message_at'] else None,
            }
        }