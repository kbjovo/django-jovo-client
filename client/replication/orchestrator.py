"""
Replication Orchestrator - Efficient sink connector management.

Key improvements:
- Reuses existing sink connector when possible
- Updates sink connector config if table list changes
- Only creates new sink connector if it doesn't exist
- Always creates fresh source connector with incremented version
"""

import logging
from typing import Dict, Any, Tuple, Optional, Set
from django.utils import timezone
from django.conf import settings

from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from .validators import ReplicationValidator
from sqlalchemy import text

logger = logging.getLogger(__name__)


class ReplicationOrchestrator:
    """
    Orchestrates all replication operations.

    This is the single source of truth for managing CDC replication.
    It ensures connector and consumer are always in sync.
    """

    def __init__(self, replication_config):
        """
        Initialize orchestrator for a specific replication config.

        Args:
            replication_config: ReplicationConfig model instance
        """
        self.config = replication_config
        self.validator = ReplicationValidator(replication_config)
        self.connector_manager = DebeziumConnectorManager()
        self.topic_manager = KafkaTopicManager()

    # ==========================================
    # Utility Methods
    # ==========================================

    def _update_status(self, status: str, message: Optional[str] = None):
        """Update ReplicationConfig status."""
        self.config.status = status
        if message:
            self.config.last_error_message = message if status == 'error' else ''
        self.config.save()

    def _log_info(self, message: str):
        """Log info message."""
        logger.info(f"[{self.config.connector_name}] {message}")

    def _log_warning(self, message: str):
        """Log warning message."""
        logger.warning(f"[{self.config.connector_name}] {message}")

    def _log_error(self, message: str):
        """Log error message."""
        logger.error(f"[{self.config.connector_name}] {message}") 
    # ==========================================

    def start_replication(self) -> Tuple[bool, str]:
        """
        Start complete replication (connector + consumer).

        EFFICIENT FLOW:
        1. Validate prerequisites
        2. Create Kafka topics
        3. Ensure sink connector is ready (create/update/reuse)
        4. Create fresh source connector with incremented version
        5. Mark as active

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STARTING CDC REPLICATION")
        self._log_info("=" * 60)

        try:
            # ========================================
            # STEP 1: Validate Prerequisites
            # ========================================
            self._log_info("STEP 1/4: Validating prerequisites...")
            self._log_info("  → Checking database connectivity")
            self._log_info("  → Verifying binlog configuration")
            self._log_info("  → Validating user permissions")

            is_valid, errors = self.validator.validate_all()
            if not is_valid:
                error_msg = f"Validation failed: {'; '.join(errors)}"
                self._log_error(error_msg)
                self._update_status('error', error_msg)
                return False, error_msg

            self._log_info("✓ All prerequisites validated successfully")
            self._log_info("")

            # ========================================
            # STEP 2: Create Kafka Topics Explicitly
            # ========================================
            self._log_info("STEP 2/4: Creating Kafka topics explicitly...")
            
            success, message = self.create_topics()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ {message}")
            self._log_info("")

            # ========================================
            # STEP 3: Ensure Sink Connector Ready (Efficient)
            # ========================================
            self._log_info("STEP 3/4: Ensuring sink connector is ready...")
            self._log_info("  → Checking if sink connector exists")
            self._log_info("  → Will reuse if compatible, update if needed, or create if missing")

            success, message = self._ensure_sink_connector_ready()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ {message}")
            self._log_info("")

            # ========================================
            # STEP 4: Create Fresh Source Connector
            # ========================================
            self._log_info("STEP 4/4: Creating fresh source connector...")
            self._log_info("  → Snapshot mode: 'initial' (full snapshot + CDC streaming)")
            self._log_info("  → Source: {}.{}".format(
                self.config.client_database.host,
                self.config.client_database.database_name
            ))

            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )
            self._log_info(f"  → Tables: {', '.join(enabled_tables)}")

            # Increment version and create connector
            success, message = self._ensure_connector_running(snapshot_mode='initial')
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ Source connector created: {self.config.connector_name}")
            self._log_info("")

            # ========================================
            # Mark as Active
            # ========================================
            self._update_status('active', 'Replication active')
            self.config.is_active = True
            self.config.save()

            # ========================================
            # Success Summary
            # ========================================
            self._log_info("=" * 60)
            self._log_info("✓ CDC REPLICATION STARTED SUCCESSFULLY")
            self._log_info("=" * 60)
            self._log_info("Status: ACTIVE")
            self._log_info(f"Source Connector: {self.config.connector_name}")
            self._log_info(f"Sink Connector: {self.config.sink_connector_name}")
            self._log_info(f"Topics: {self.config.kafka_topic_prefix}.*")
            self._log_info("")
            self._log_info("Data flow:")
            self._log_info(f"  Source DB → Debezium → Kafka → JDBC Sink → Target DB")
            self._log_info("=" * 60)

            return True, "Replication started successfully"

        except Exception as e:
            error_msg = f"Failed to start replication: {str(e)}"
            self._log_error(error_msg)
            self._update_status('error', error_msg)
            return False, error_msg

    def stop_replication(self) -> Tuple[bool, str]:
        """
        Stop replication (pause both connectors).

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STOPPING REPLICATION")
        self._log_info("=" * 60)

        try:
            # Update status
            self._update_status('paused', 'Stopping replication...')
            self.config.is_active = False
            self.config.save()

            # Pause both connectors
            errors = []
            
            # Pause source connector
            if self.config.connector_name:
                try:
                    success, error = self.connector_manager.pause_connector(self.config.connector_name)
                    if success:
                        self._log_info(f"✓ Paused source: {self.config.connector_name}")
                    else:
                        errors.append(f"Source connector: {error}")
                except Exception as e:
                    errors.append(f"Source connector: {str(e)}")

            # Pause sink connector
            if self.config.sink_connector_name:
                try:
                    success, error = self.connector_manager.pause_connector(self.config.sink_connector_name)
                    if success:
                        self._log_info(f"✓ Paused sink: {self.config.sink_connector_name}")
                    else:
                        errors.append(f"Sink connector: {error}")
                except Exception as e:
                    errors.append(f"Sink connector: {str(e)}")

            if errors:
                error_msg = "; ".join(errors)
                self._log_warning(f"⚠️ Some connectors could not be paused: {error_msg}")
                return False, error_msg

            self._log_info("=" * 60)
            self._log_info("✓ REPLICATION STOPPED SUCCESSFULLY")
            self._log_info("=" * 60)

            return True, "Replication stopped successfully"

        except Exception as e:
            error_msg = f"Failed to stop replication: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def restart_replication(self) -> Tuple[bool, str]:
        """
        Restart replication (stop + start).

        Returns:
            (success, message)
        """
        self._log_info("Restarting replication...")

        # Stop first
        success, message = self.stop_replication()
        if not success:
            return False, f"Failed to stop: {message}"

        # Wait a moment
        import time
        time.sleep(2)

        # Start again
        success, message = self.start_replication()
        if not success:
            return False, f"Failed to start: {message}"

        return True, "Replication restarted successfully"

    def delete_replication(self, delete_topics: bool = False) -> Tuple[bool, str]:
        """
        Delete replication completely.

        Args:
            delete_topics: If True, permanently delete Kafka topics

        Returns:
            (success, message)
        """
        self._log_info("=" * 80)
        self._log_info("DELETING REPLICATION")
        self._log_info("=" * 80)

        config_id = self.config.id
        source_connector_name = self.config.connector_name
        sink_connector_name = self.config.sink_connector_name
        
        try:
            # Mark as inactive
            self.config.is_active = False
            self.config.status = 'stopping'
            self.config.save()

            # Delete source connector
            if source_connector_name:
                try:
                    success, error = self.connector_manager.delete_connector(
                        source_connector_name,
                        delete_topics=False
                    )
                    if success:
                        # Mark as deleted in history
                        from client.models import ConnectorHistory
                        ConnectorHistory.mark_connector_deleted(
                            connector_name=source_connector_name,
                            notes="Deleted via orchestrator"
                        )
                        self._log_info(f"✓ Deleted source: {source_connector_name}")
                    else:
                        self._log_warning(f"⚠️ Source deletion failed: {error}")
                except Exception as e:
                    self._log_warning(f"⚠️ Source deletion error: {e}")

            # Delete sink connector
            if sink_connector_name:
                try:
                    success, error = self.connector_manager.delete_connector(
                        sink_connector_name,
                        delete_topics=False
                    )
                    if success:
                        # Mark as deleted in history
                        from client.models import ConnectorHistory
                        ConnectorHistory.mark_connector_deleted(
                            connector_name=sink_connector_name,
                            notes="Deleted via orchestrator"
                        )
                        self._log_info(f"✓ Deleted sink: {sink_connector_name}")
                    else:
                        self._log_warning(f"⚠️ Sink deletion failed: {error}")
                except Exception as e:
                    self._log_warning(f"⚠️ Sink deletion error: {e}")

            # Optionally delete topics and target tables
            if delete_topics:
                self._log_info("Deleting Kafka topics...")
                success, message = self.delete_topics()
                if success:
                    self._log_info(f"✓ {message}")
                else:
                    self._log_warning(f"⚠️ {message}")

                # Also drop target database tables for these topics
                self._log_info("Dropping target database tables...")
                success, message = self._drop_target_tables()
                if success:
                    self._log_info(f"✓ {message}")
                else:
                    self._log_warning(f"⚠️ {message}")

            # Delete config
            from client.models import ReplicationConfig
            ReplicationConfig.objects.filter(pk=config_id).delete()
            self._log_info(f"✓ Deleted config (id={config_id})")

            self._log_info("=" * 80)
            self._log_info("✓ REPLICATION DELETED")
            self._log_info("=" * 80)

            return True, "Replication deleted successfully"

        except Exception as e:
            error_msg = f"Failed to delete: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Efficient Sink Connector Management
    # ==========================================

    def _ensure_sink_connector_ready(self) -> Tuple[bool, str]:
        """
        Efficiently ensure sink connector is ready.

        Strategy:
        1. Check if sink connector exists
        2. If exists:
           - Compare current tables with configured tables
           - If same: reuse (do nothing)
           - If different: update config
        3. If doesn't exist: create new

        Returns:
            (success, message)
        """
        try:
            # Get client's target database
            from client.models.database import ClientDatabase
            client = self.config.client_database.client
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if not target_db:
                return False, "No target database configured"

            # Generate sink connector name (NOT versioned - same across source connector versions)
            sink_connector_name = f"client_{client.id}_db_{target_db.id}_sink_connector"

            # Get current enabled tables
            current_topics = self._get_kafka_topics_for_config()
            
            if not current_topics:
                return False, "No tables enabled for replication"

            # Check if sink connector exists
            exists, status_data = self.connector_manager.get_connector_status(sink_connector_name)

            if exists:
                # Get existing config
                existing_config = self.connector_manager.get_connector_config(sink_connector_name)

                if existing_config:
                    # Compare topics
                    existing_topics = self._parse_topics_from_config(existing_config)

                    if existing_topics == current_topics:
                        # Perfect match - reuse as is
                        self._log_info("✓ Reusing existing sink connector")
                        self.config.sink_connector_name = sink_connector_name
                        self.config.sink_connector_state = 'RUNNING'
                        self.config.save()
                        return True, "Sink connector reused (config unchanged)"
                    else:
                        # Tables changed - update config
                        self._log_info(f"Updating sink connector topics ({len(current_topics)} tables)")
                        return self._update_sink_connector(sink_connector_name, current_topics, target_db)
                else:
                    # Couldn't get config - recreate
                    self._log_warning("Unable to retrieve sink config - recreating")
                    self.connector_manager.delete_connector(sink_connector_name, delete_topics=False)
                    import time
                    time.sleep(2)
                    return self._create_sink_connector(sink_connector_name, current_topics, target_db)
            else:
                # Doesn't exist - create new
                return self._create_sink_connector(sink_connector_name, current_topics, target_db)

        except Exception as e:
            error_msg = f"Failed to ensure sink connector ready: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_kafka_topics_for_config(self) -> Set[str]:
        """
        Get set of Kafka topics for currently enabled tables.

        Returns:
            Set of topic names
        """
        topics = set()
        enabled_tables = self.config.table_mappings.filter(is_enabled=True)
        
        for table_mapping in enabled_tables:
            db_config = self.config.client_database
            topic_prefix = self.config.kafka_topic_prefix

            if db_config.db_type == 'mysql':
                topic = f"{topic_prefix}.{db_config.database_name}.{table_mapping.source_table}"
            elif db_config.db_type == 'postgresql':
                schema = table_mapping.source_schema or 'public'
                topic = f"{topic_prefix}.{schema}.{table_mapping.source_table}"
            elif db_config.db_type == 'mssql':
                schema = table_mapping.source_schema or 'dbo'
                topic = f"{topic_prefix}.{db_config.database_name}.{schema}.{table_mapping.source_table}"
            elif db_config.db_type == 'oracle':
                schema = table_mapping.source_schema
                topic = f"{topic_prefix}.{schema}.{table_mapping.source_table}"
            else:
                continue

            topics.add(topic)

        return topics

    def _parse_topics_from_config(self, config: Dict[str, Any]) -> Set[str]:
        """
        Parse topics from existing sink connector config.

        Args:
            config: Sink connector configuration dict

        Returns:
            Set of topic names
        """
        topics_str = config.get('topics', '')
        if topics_str:
            return set(t.strip() for t in topics_str.split(',') if t.strip())
        return set()

    def _update_sink_connector(
        self,
        sink_connector_name: str,
        topics: Set[str],
        target_db
    ) -> Tuple[bool, str]:
        """
        Update existing sink connector with new topic list.

        Args:
            sink_connector_name: Name of sink connector
            topics: Set of Kafka topics to subscribe to
            target_db: Target database instance

        Returns:
            (success, message)
        """
        try:
            # Get primary key fields
            primary_key_fields = self._get_primary_key_fields()

            # Generate new config
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )

            custom_config = {'name': sink_connector_name}

            new_config = get_sink_connector_config_for_database(
                db_config=target_db,
                topics=list(topics),
                kafka_bootstrap_servers=kafka_bootstrap,
                primary_key_fields=primary_key_fields,
                custom_config=custom_config,
            )

            # Update connector config
            success, error = self.connector_manager.update_connector_config(
                sink_connector_name,
                new_config
            )

            if success:
                self._log_info(f"✓ Updated sink connector with {len(topics)} topics")
                self.config.sink_connector_name = sink_connector_name
                self.config.sink_connector_state = 'RUNNING'
                self.config.save()
                return True, f"Sink connector updated ({len(topics)} topics)"
            else:
                return False, f"Failed to update sink connector: {error}"

        except Exception as e:
            error_msg = f"Failed to update sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _create_sink_connector(
        self,
        sink_connector_name: str,
        topics: Set[str],
        target_db
    ) -> Tuple[bool, str]:
        """
        Create new sink connector.

        Args:
            sink_connector_name: Name for sink connector
            topics: Set of Kafka topics to subscribe to
            target_db: Target database instance

        Returns:
            (success, message)
        """
        try:
            # Get primary key fields
            primary_key_fields = self._get_primary_key_fields()

            # Get Kafka bootstrap servers
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )

            # Generate config
            custom_config = {'name': sink_connector_name}

            sink_config = get_sink_connector_config_for_database(
                db_config=target_db,
                topics=list(topics),
                kafka_bootstrap_servers=kafka_bootstrap,
                primary_key_fields=primary_key_fields,
                custom_config=custom_config,
            )

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=sink_connector_name,
                config=sink_config
            )

            if success:
                # Record sink connector creation in history
                from client.models import ConnectorHistory
                ConnectorHistory.record_connector_creation(
                    replication_config=self.config,
                    connector_name=sink_connector_name,
                    connector_version=1,  # Sink connectors don't use versions
                    connector_type='sink'
                )

                # Log success with configuration details
                self._log_info(f"✓ Sink connector created successfully")
                self._log_info(f"  Connector: {sink_connector_name}")
                self._log_info(f"  Target: {target_db.db_type} ({target_db.host}:{target_db.port})")
                self._log_info(f"  Database: {target_db.database_name}")
                self._log_info(f"  Topics subscribed: {len(topics)}")
                # Log topic list (excluding internal topics)
                user_topics = [t for t in sorted(topics) if not t.startswith('_') and not t.startswith('connect-') and not t.startswith('schema-')]
                for topic in user_topics:
                    self._log_info(f"    - {topic}")
                self._log_info(f"  Primary key fields: {primary_key_fields or 'auto-detected'}")
                self._log_info(f"  Kafka bootstrap: {kafka_bootstrap}")

                self.config.sink_connector_name = sink_connector_name
                self.config.sink_connector_state = 'RUNNING'
                self.config.save()

                return True, f"Sink connector created ({len(topics)} topics)"
            else:
                return False, f"Failed to create sink connector: {error}"

        except Exception as e:
            error_msg = f"Failed to create sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_primary_key_fields(self) -> Optional[str]:
        """
        Extract primary key fields from first enabled table.

        Returns:
            Comma-separated list of primary key column names, or None
        """
        enabled_tables = list(self.config.table_mappings.filter(is_enabled=True))
        
        if enabled_tables:
            first_table = enabled_tables[0]
            pk_columns = first_table.column_mappings.filter(
                is_primary_key=True,
                is_enabled=True
            )
            
            if pk_columns.exists():
                pk_fields = ",".join([col.target_column for col in pk_columns])
                return pk_fields
        
        return None

    # ==========================================
    # Source Connector Management (Existing)
    # ==========================================

    def _ensure_connector_running(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Ensure Debezium source connector exists and is running.

        Creates new connector with incremented version.

        Args:
            snapshot_mode: Debezium snapshot mode

        Returns:
            (success, message)
        """
        # Generate versioned connector name
        from jovoclient.utils.debezium.connector_templates import generate_connector_name
        db_config = self.config.client_database
        client = db_config.client
        
        versioned_connector_name = generate_connector_name(
            client,
            db_config,
            version=self.config.connector_version
        )

        # Delete old connector if exists
        try:
            self.connector_manager.delete_connector(versioned_connector_name, delete_topics=False)
        except Exception:
            pass  # Connector didn't exist

        # Create fresh connector
        return self._create_connector(snapshot_mode=snapshot_mode)

    def _create_connector(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Create new Debezium source connector with incremented version.
        Uses ConnectorHistory to track version numbers across deletions.

        Returns:
            (success, message)
        """
        try:
            # Get next version from history (handles deleted connectors)
            from client.models import ConnectorHistory
            db_config = self.config.client_database
            client = db_config.client

            next_version = ConnectorHistory.get_next_version(
                client_id=client.id,
                database_id=db_config.id,
                connector_type='source'
            )

            self.config.connector_version = next_version
            self.config.save()

            # Generate configuration
            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )

            if not enabled_tables:
                return False, "No tables enabled for replication"

            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            schema_registry = settings.DEBEZIUM_CONFIG.get(
                'SCHEMA_REGISTRY_URL',
                'http://schema-registry:8081'
            )

            config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=enabled_tables,
                kafka_bootstrap_servers=kafka_bootstrap,
                schema_registry_url=schema_registry,
                snapshot_mode=snapshot_mode,
            )

            # Generate versioned connector name
            from jovoclient.utils.debezium.connector_templates import generate_connector_name
            versioned_connector_name = generate_connector_name(
                client,
                db_config,
                version=self.config.connector_version
            )

            # Update config
            self.config.connector_name = versioned_connector_name
            self.config.save()

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=versioned_connector_name,
                config=config
            )

            if success:
                # Record connector creation in history
                ConnectorHistory.record_connector_creation(
                    replication_config=self.config,
                    connector_name=versioned_connector_name,
                    connector_version=self.config.connector_version,
                    connector_type='source'
                )

                # Log success with configuration details
                self._log_info(f"✓ Source connector created successfully")
                self._log_info(f"  Connector: {versioned_connector_name}")
                self._log_info(f"  Version: {self.config.connector_version}")
                self._log_info(f"  Snapshot mode: {snapshot_mode}")
                self._log_info(f"  Tables: {', '.join(enabled_tables)}")
                self._log_info(f"  Kafka bootstrap: {kafka_bootstrap}")
                self._log_info(f"  Topic prefix: {self.config.kafka_topic_prefix}")

                return True, "Connector created successfully"
            else:
                return False, f"Failed to create connector: {error}"

        except Exception as e:
            error_msg = f"Failed to create connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Topic Management (Delegated)
    # ==========================================

    def create_topics(self) -> Tuple[bool, str]:
        """Create Kafka topics for replication."""
        try:
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            topic_manager = KafkaTopicManager(bootstrap_servers=kafka_bootstrap)
            return topic_manager.create_topics_for_config(self.config)
        except Exception as e:
            error_msg = f"Failed to create topics: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def delete_topics(self) -> Tuple[bool, str]:
        """Delete Kafka topics for replication."""
        try:
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            topic_manager = KafkaTopicManager(bootstrap_servers=kafka_bootstrap)
            topic_prefix = self.config.kafka_topic_prefix
            return topic_manager.delete_topics_by_prefix(topic_prefix)
        except Exception as e:
            error_msg = f"Failed to delete topics: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg
        

    def _drop_target_tables(self) -> Tuple[bool, str]:
        """
        Drop target database tables corresponding to the replicated tables.

        This is called when deleting replication with delete_topics=True
        to clean up the target database tables that were receiving data.

        Returns:
            (success, message)
        """
        try:
            # Get target database
            from client.models.database import ClientDatabase
            from client.utils.database_utils import get_database_engine
            
            client = self.config.client_database.client
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if not target_db:
                return False, "No target database configured"

            # Get enabled table mappings
            enabled_tables = self.config.table_mappings.filter(is_enabled=True)

            if not enabled_tables.exists():
                return True, "No tables to drop"

            # Get target database engine using the utility function
            engine = get_database_engine(target_db)

            dropped_tables = []
            failed_tables = []

            try:
                with engine.connect() as conn:
                    for table_mapping in enabled_tables:
                        target_table = table_mapping.target_table
                        target_schema = table_mapping.target_schema or None

                        try:
                            # Build fully qualified table name
                            if target_schema:
                                full_table_name = f"{target_schema}.{target_table}"
                            else:
                                full_table_name = target_table

                            # Drop table with appropriate syntax for each database type
                            if target_db.db_type == 'mysql':
                                drop_sql = f"DROP TABLE IF EXISTS `{target_table}`"
                            elif target_db.db_type == 'postgresql':
                                if target_schema:
                                    drop_sql = f'DROP TABLE IF EXISTS "{target_schema}"."{target_table}" CASCADE'
                                else:
                                    drop_sql = f'DROP TABLE IF EXISTS "{target_table}" CASCADE'
                            elif target_db.db_type == 'mssql':
                                if target_schema:
                                    drop_sql = f"DROP TABLE IF EXISTS [{target_schema}].[{target_table}]"
                                else:
                                    drop_sql = f"DROP TABLE IF EXISTS [{target_table}]"
                            elif target_db.db_type == 'oracle':
                                # Oracle doesn't support IF EXISTS, need to handle exception
                                if target_schema:
                                    drop_sql = f'DROP TABLE "{target_schema}"."{target_table}" CASCADE CONSTRAINTS'
                                else:
                                    drop_sql = f'DROP TABLE "{target_table}" CASCADE CONSTRAINTS'
                            else:
                                drop_sql = f"DROP TABLE IF EXISTS {target_table}"

                            # Execute drop
                            conn.execute(text(drop_sql))
                            conn.commit()

                            dropped_tables.append(full_table_name)
                            self._log_info(f"  ✓ Dropped table: {full_table_name}")

                        except Exception as e:
                            # For Oracle, if table doesn't exist, it's okay
                            if target_db.db_type == 'oracle' and ('ORA-00942' in str(e) or 'does not exist' in str(e)):
                                self._log_info(f"  ○ Table already dropped: {full_table_name}")
                            else:
                                failed_tables.append(f"{full_table_name}: {str(e)}")
                                self._log_warning(f"  ⚠️ Failed to drop {full_table_name}: {e}")

            finally:
                # Always dispose of the engine
                engine.dispose()

            # Build result message
            if dropped_tables:
                message = f"Dropped {len(dropped_tables)} table(s): {', '.join(dropped_tables)}"
            else:
                message = "No tables were dropped"

            if failed_tables:
                message += f" | {len(failed_tables)} failed"
                return False, message

            return True, message

        except Exception as e:
            error_msg = f"Failed to drop target tables: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Status and Health
    # ==========================================

    def get_unified_status(self) -> Dict[str, Any]:
        """Get comprehensive status of replication."""
        connector_status = self._get_connector_status()
        sink_status = self._get_sink_connector_status()
        overall_health = self._calculate_overall_health(connector_status, sink_status)

        return {
            'overall': overall_health,
            'source_connector': connector_status,
            'sink_connector': sink_status,
            'config': {
                'status': self.config.status,
                'is_active': self.config.is_active,
                'connector_name': self.config.connector_name,
                'sink_connector_name': self.config.sink_connector_name,
                'kafka_topic_prefix': self.config.kafka_topic_prefix,
            },
            'statistics': {
                'total_rows_synced': sum(
                    tm.total_rows_synced or 0
                    for tm in self.config.table_mappings.all()
                ),
                'tables_enabled': self.config.table_mappings.filter(is_enabled=True).count(),
                'last_sync_at': self.config.last_sync_at.isoformat() if self.config.last_sync_at else None,
            },
            'timestamp': timezone.now().isoformat(),
        }

    def _get_connector_status(self) -> Dict[str, Any]:
        """Get Debezium source connector status."""
        try:
            result = self.connector_manager.get_connector_status(self.config.connector_name)
            
            if isinstance(result, tuple):
                success, status_data = result
                if not success or not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}
            else:
                status_data = result
                if not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}

            state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            return {
                'state': state,
                'healthy': state == 'RUNNING',
                'tasks': status_data.get('tasks', []),
            }
        except Exception as e:
            return {'state': 'ERROR', 'healthy': False, 'message': str(e)}

    def _get_sink_connector_status(self) -> Dict[str, Any]:
        """Get JDBC Sink connector status."""
        try:
            if not self.config.sink_connector_name:
                return {'state': 'NOT_CONFIGURED', 'healthy': False}

            result = self.connector_manager.get_connector_status(self.config.sink_connector_name)
            
            if isinstance(result, tuple):
                success, status_data = result
                if not success or not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}
            else:
                status_data = result
                if not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}

            state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            return {
                'state': state,
                'healthy': state == 'RUNNING',
                'tasks': status_data.get('tasks', []),
            }
        except Exception as e:
            return {'state': 'ERROR', 'healthy': False, 'message': str(e)}

    def _calculate_overall_health(self, connector_status: Dict, sink_status: Dict) -> str:
        """Calculate overall health: 'healthy', 'degraded', or 'failed'."""
        connector_healthy = connector_status.get('healthy', False)
        sink_healthy = sink_status.get('healthy', False)

        if connector_healthy and sink_healthy:
            return 'healthy'
        elif connector_healthy or sink_healthy:
            return 'degraded'
        else:
            return 'failed'

    # ==========================================
    #


