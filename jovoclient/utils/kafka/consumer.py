import logging
import time
import signal
from typing import List, Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)

class ResilientDatabaseConsumer:
    """
    A production-grade Kafka Consumer designed for high-availability 
    database replication tasks.
    """
    def __init__(self, bootstrap_servers: str, group_id: str, topics: List[str]):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            # 1. Disable auto-commit for "At Least Once" delivery
            'enable.auto.commit': False,
            # 2. Frequent heartbeats to keep the session alive
            'heartbeat.interval.ms': 3000,
            'session.timeout.ms': 10000,
            # 3. Handle rebalances gracefully
            'partition.assignment.strategy': 'cooperative-sticky',
        }
        self.topics = topics
        self.consumer = Consumer(self.config)
        self.running = True
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def subscribe(self):
        """Subscribe to the specified list of database topics."""
        self.consumer.subscribe(self.topics)
        logger.info(f"Subscribed to topics: {self.topics}")

    def poll_and_process(self, timeout: float = 1.0):
        """
        Main loop: Polls for messages, processes them, and commits offsets.
        """
        try:
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                # --- Business Logic Starts Here ---
                try:
                    self.handle_message(msg)
                    # 4. Manual Commit: Only commit AFTER successful processing
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Failed to process/commit message: {e}")
                    # In production, send to a Dead Letter Queue (DLQ) here
                # --- Business Logic Ends Here ---

        except Exception as e:
            logger.exception("Critical error in consumer loop")
        finally:
            self.close()

    def handle_message(self, msg):
        """Placeholder for actual DB sync logic (e.g., writing to SQL)."""
        value = msg.value().decode('utf-8')
        logger.debug(f"Received message from {msg.topic()}: {value}")

    def shutdown(self, signum=None, frame=None):
        """Triggered by SIGINT/SIGTERM to stop the loop safely."""
        logger.info("Shutdown signal received. Stopping consumer...")
        self.running = False

    def close(self):
        """Properly close the consumer and commit final offsets."""
        try:
            self.consumer.close()
            logger.info("Consumer closed successfully.")
        except Exception as e:
            logger.error(f"Error while closing consumer: {e}")