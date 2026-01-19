"""
Debezium Connector Manager - Manages CDC connectors via Kafka Connect REST API
"""

import logging
import requests
import json
import time
from typing import Dict, List, Optional, Any, Tuple
from django.conf import settings
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig
from client.utils.notification_utils import send_connector_status_email, log_and_notify_error
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager

logger = logging.getLogger(__name__)


class DebeziumException(Exception):
    """Base exception for Debezium operations"""
    pass


class ConnectorNotFoundException(DebeziumException):
    """Raised when connector is not found"""
    pass


class ConnectorCreationException(DebeziumException):
    """Raised when connector creation fails"""
    pass


class DebeziumConnectorManager:
    """
    Manager class for Debezium Kafka Connect operations
    
    Handles:
    - Creating connectors
    - Deleting connectors
    - Checking connector status
    - Updating connector configuration
    - Pausing/Resuming connectors
    """
    
    def __init__(self):
        """Initialize Debezium manager with configuration from settings"""
        debezium_config = getattr(settings, 'DEBEZIUM_CONFIG', {})
        
        self.kafka_connect_url = debezium_config.get(
            'KAFKA_CONNECT_URL', 
            'http://localhost:8083'
        )
        self.kafka_bootstrap_servers = debezium_config.get(
            'KAFKA_BOOTSTRAP_SERVERS', 
            'localhost:9092'
        )
        self.schema_registry_url = debezium_config.get(
            'SCHEMA_REGISTRY_URL', 
            'http://localhost:8081'
        )
        
        # API endpoints
        self.connectors_url = f"{self.kafka_connect_url}/connectors"
        
        logger.info(f"DebeziumConnectorManager initialized with URL: {self.kafka_connect_url}")
    
    def _make_request(
        self, 
        method: str, 
        url: str, 
        data: Optional[Dict] = None,
        timeout: int = 30
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """
        Make HTTP request to Kafka Connect API
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: Full URL
            data: Request body data
            timeout: Request timeout in seconds
            
        Returns:
            Tuple[bool, Optional[Dict], Optional[str]]: (success, response_data, error_message)
        """
        try:
            headers = {'Content-Type': 'application/json'}
            
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=timeout)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=timeout)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data, timeout=timeout)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=timeout)
            else:
                return False, None, f"Unsupported HTTP method: {method}"
            logger.debug(f"method: {method}, url: {url}")
            # Check if request was successful
            # 200: OK, 201: Created, 202: Accepted (async operations), 204: No Content
            if response.status_code in [200, 201, 202, 204]:
                # Some DELETE requests return 204 with no content
                if response.status_code == 204:
                    return True, {}, None

                # 202 Accepted may have no response body
                if response.status_code == 202:
                    try:
                        return True, response.json() if response.text else {}, None
                    except json.JSONDecodeError:
                        return True, {}, None

                try:
                    return True, response.json(), None
                except json.JSONDecodeError:
                    return True, {}, None
            else:
                error_data = response.text
                try:
                    error_json = response.json()
                    error_message = error_json.get('message', error_data)
                except:
                    error_message = error_data

                # 404 is expected when checking if connector exists - log as debug
                if response.status_code == 404:
                    logger.debug(f"Resource not found: {method} {url}")
                else:
                    logger.error(f"Request failed: {method} {url} - Status: {response.status_code} - {error_message}")

                return False, None, f"HTTP {response.status_code}: {error_message}"
                
        except requests.exceptions.Timeout:
            error_msg = f"Request timeout after {timeout} seconds"
            logger.error(error_msg)
            return False, None, error_msg
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
    
    def check_kafka_connect_health(self) -> Tuple[bool, Optional[str]]:
        """
        Check if Kafka Connect is running and healthy
        
        Returns:
            Tuple[bool, Optional[str]]: (is_healthy, error_message)
        """
        try:
            success, data, error = self._make_request('GET', self.kafka_connect_url)
            
            if success:
                logger.info("Kafka Connect is healthy")
                return True, None
            else:
                return False, error
                
        except Exception as e:
            error_msg = f"Health check failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def list_connectors(self) -> List[str]:
        """
        List all existing connectors
        
        Returns:
            List[str]: List of connector names
        """
        try:
            success, data, error = self._make_request('GET', self.connectors_url)
            
            if success and data:
                connectors = data if isinstance(data, list) else []
                logger.info(f"Found {len(connectors)} connectors")
                return connectors
            else:
                logger.error(f"Failed to list connectors: {error}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing connectors: {str(e)}")
            return []
    
    def get_connector_status(self, connector_name: str) -> Tuple[bool, Optional[Dict]]:
        """
        Get connector status
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Tuple[bool, Optional[Dict]]: (exists, status_data)
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/status"
            success, data, error = self._make_request('GET', url)
            
            if success and data:
                logger.debug(f"Connector {connector_name} status: {data.get('connector', {}).get('state', 'UNKNOWN')}")
                return True, data
            else:
                if '404' in str(error):
                    logger.debug(f"Connector {connector_name} not found")
                    return False, None
                logger.error(f"Failed to get connector status: {error}")
                return False, None
                
        except Exception as e:
            logger.error(f"Error getting connector status: {str(e)}")
            return False, None
    
    def get_connector_config(self, connector_name: str) -> Optional[Dict]:
        """
        Get connector configuration
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Optional[Dict]: Connector configuration or None
        """
        try:
            url = f"{self.connectors_url}/{connector_name}"
            success, data, error = self._make_request('GET', url)
            
            if success and data:
                logger.info(f"Retrieved config for connector: {connector_name}")
                return data.get('config', {})
            else:
                logger.error(f"Failed to get connector config: {error}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting connector config: {str(e)}")
            return None

    def get_connector_topics(self, connector_name: str) -> Optional[Dict]:
        """
        Get topics that a connector is producing to or consuming from.

        Args:
            connector_name: Name of the connector

        Returns:
            Optional[Dict]: Topics info with format {connector_name: {topics: [...]}} or None
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/topics"
            success, data, error = self._make_request('GET', url)

            if success and data:
                logger.debug(f"Retrieved topics for connector {connector_name}: {data}")
                return data
            else:
                logger.error(f"Failed to get connector topics: {error}")
                return None

        except Exception as e:
            logger.error(f"Error getting connector topics: {str(e)}")
            return None

    def create_connector(
        self, 
        connector_name: str, 
        config: Dict[str, Any],
        notify_on_error: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """
        Create a new Debezium connector
        
        Args:
            connector_name: Name for the connector
            config: Connector configuration dictionary
            notify_on_error: Send email notification on error
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            # Check if connector already exists
            exists, _ = self.get_connector_status(connector_name)
            if exists:
                error_msg = f"Connector {connector_name} already exists"
                logger.warning(error_msg)
                return False, error_msg
            
            # Prepare request body
            request_body = {
                "name": connector_name,
                "config": config
            }
            
            # Create connector
            success, data, error = self._make_request('POST', self.connectors_url, request_body)
            
            if success:
                logger.info(f"Successfully created connector: {connector_name}")
                
                # Wait a moment for connector to initialize
                time.sleep(2)
                
                # Check status
                _, status_data = self.get_connector_status(connector_name)
                if status_data:
                    state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                    
                    if state == 'RUNNING':
                        logger.info(f"Connector {connector_name} is RUNNING")
                    else:
                        logger.warning(f"Connector {connector_name} state: {state}")
                
                return True, None
            else:
                logger.error(f"Failed to create connector {connector_name}: {error}")
                
                if notify_on_error:
                    send_connector_status_email(
                        connector_name=connector_name,
                        status='FAILED',
                        client_name=config.get('database.user', 'Unknown'),
                        database_name=config.get('database.dbname', 'Unknown'),
                        error_message=error
                    )
                
                return False, error
                
        except Exception as e:
            error_msg = f"Error creating connector: {str(e)}"
            logger.error(error_msg)
            
            if notify_on_error:
                log_and_notify_error(
                    logger,
                    f"Failed to create connector {connector_name}",
                    e,
                    context={'connector_name': connector_name, 'config': config}
                )
            
            return False, error_msg
    
    def delete_connector(
        self,
        connector_name: str,
        notify: bool = False,
        delete_topics: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """
        Delete a connector and optionally its associated topics

        Args:
            connector_name: Name of the connector
            notify: Send notification after deletion
            delete_topics: Delete associated Kafka topics (default: True)

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            # Check if connector exists
            exists, _ = self.get_connector_status(connector_name)
            if not exists:
                # Connector doesn't exist - this is fine, nothing to delete
                logger.debug(f"Connector {connector_name} does not exist (already deleted or never created)")
                return True, None  # Return success since the desired state (deleted) is achieved

            # Get connector config to find topic prefix (needed for topic deletion)
            topic_prefix = None
            if delete_topics:
                try:
                    config = self.get_connector_config(connector_name)
                    logger.info(f"DELETE CONNECTOR confg: {config}")
                    if config and 'topic.prefix' in config:
                        topic_prefix = config['topic.prefix']
                        logger.info(f"Found topic prefix: {topic_prefix}")
                except Exception as e:
                    logger.warning(f"Could not get topic prefix: {e}")

            # Delete connector
            url = f"{self.connectors_url}/{connector_name}"
            success, _, error = self._make_request('DELETE', url)

            if success:
                logger.info(f"✅ Successfully deleted connector: {connector_name}")

                # Delete associated topics
                if delete_topics and topic_prefix:
                    try:
                        logger.info(f"🗑️  Deleting topics with prefix: {topic_prefix}")
                        topic_manager = KafkaTopicManager()

                        # Delete data topics (e.g., client_2_db_5.database.table)
                        results = topic_manager.delete_topics_by_prefix(topic_prefix, exclude_internal=True)
                        deleted_count = sum(1 for v in results.values() if v)
                        logger.info(f"Deleted {deleted_count} data topics")

                        # Delete schema history topic
                        schema_topic = f"schema-changes.{connector_name}"
                        if topic_manager.topic_exists(schema_topic):
                            topic_manager.delete_topic(schema_topic)
                            logger.info(f"Deleted schema history topic: {schema_topic}")

                    except Exception as e:
                        logger.warning(f"Failed to delete topics: {e}")
                        # Don't fail the whole operation if topic deletion fails

                if notify:
                    send_connector_status_email(
                        connector_name=connector_name,
                        status='DELETED',
                        client_name='N/A',
                        database_name='N/A',
                        error_message=None
                    )

                return True, None
            else:
                logger.error(f"Failed to delete connector {connector_name}: {error}")
                return False, error

        except Exception as e:
            error_msg = f"Error deleting connector: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def pause_connector(self, connector_name: str) -> Tuple[bool, Optional[str]]:
        """
        Pause a connector
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/pause"
            success, _, error = self._make_request('PUT', url)
            
            if success:
                logger.info(f"Successfully paused connector: {connector_name}")
                return True, None
            else:
                logger.error(f"Failed to pause connector {connector_name}: {error}")
                return False, error
                
        except Exception as e:
            error_msg = f"Error pausing connector: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def resume_connector(self, connector_name: str) -> Tuple[bool, Optional[str]]:
        """
        Resume a paused connector
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/resume"
            success, _, error = self._make_request('PUT', url)
            
            if success:
                logger.info(f"Successfully resumed connector: {connector_name}")
                return True, None
            else:
                logger.error(f"Failed to resume connector {connector_name}: {error}")
                return False, error

        except Exception as e:
            error_msg = f"Error resuming connector: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def restart_connector(self, connector_name: str) -> Tuple[bool, Optional[str]]:
        """
        Restart a connector
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/restart"
            success, _, error = self._make_request('POST', url)
            
            if success:
                logger.info(f"Successfully restarted connector: {connector_name}")
                return True, None
            else:
                logger.error(f"Failed to restart connector {connector_name}: {error}")
                return False, error
                
        except Exception as e:
            error_msg = f"Error restarting connector: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def update_connector_config(
        self, 
        connector_name: str, 
        new_config: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Update connector configuration
        
        Args:
            connector_name: Name of the connector
            new_config: New configuration dictionary
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/config"
            success, data, error = self._make_request('PUT', url, new_config)
            
            if success:
                logger.info(f"Successfully updated connector config: {connector_name}")
                return True, None
            else:
                logger.error(f"Failed to update connector config {connector_name}: {error}")
                return False, error
                
        except Exception as e:
            error_msg = f"Error updating connector config: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def get_connector_tasks(self, connector_name: str) -> Optional[List[Dict]]:
        """
        Get connector tasks information
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Optional[List[Dict]]: List of task information or None
        """
        try:
            url = f"{self.connectors_url}/{connector_name}/tasks"
            success, data, error = self._make_request('GET', url)
            
            if success and data:
                logger.info(f"Retrieved {len(data)} tasks for connector: {connector_name}")
                return data
            else:
                logger.error(f"Failed to get connector tasks: {error}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting connector tasks: {str(e)}")
            return None
    
    def validate_connector_config(self, connector_class: str, config: Dict[str, Any]) -> Tuple[bool, Optional[Dict]]:
        """
        Validate connector configuration without creating it
        
        Args:
            connector_class: Connector class (e.g., 'io.debezium.connector.mysql.MySqlConnector')
            config: Configuration to validate
            
        Returns:
            Tuple[bool, Optional[Dict]]: (is_valid, validation_results)
        """
        try:
            url = f"{self.kafka_connect_url}/connector-plugins/{connector_class}/config/validate"
            
            request_body = {
                "connector.class": connector_class,
                **config
            }
            
            success, data, error = self._make_request('PUT', url, request_body)
            
            if success and data:
                # Check for errors in validation
                configs = data.get('configs', [])
                has_errors = any(c.get('value', {}).get('errors', []) for c in configs)
                
                if has_errors:
                    logger.warning(f"Configuration validation has errors")
                    return False, data
                else:
                    logger.info(f"Configuration validation passed")
                    return True, data
            else:
                logger.error(f"Failed to validate config: {error}")
                return False, None
                
        except Exception as e:
            logger.error(f"Error validating config: {str(e)}")
            return False, None