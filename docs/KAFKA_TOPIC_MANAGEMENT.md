# Kafka Topic Management Guide

Complete guide for creating and managing Kafka topics for CDC (Change Data Capture) replication.

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [Usage Examples](#usage-examples)
4. [Production Setup](#production-setup)
5. [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. View Current Configuration

```bash
python manage.py create_kafka_topics --summary
```

This shows your current Kafka topic settings from `settings.py`.

### 2. List Existing Topics

```bash
# List all topics
python manage.py create_kafka_topics --list

# List topics with specific prefix
python manage.py create_kafka_topics --list --prefix client_2
```

### 3. Create Topics for All Replication Configs

```bash
python manage.py create_kafka_topics --all
```

This automatically creates topics for all enabled tables in all replication configurations.

### 4. Create Topics for Specific Config

```bash
python manage.py create_kafka_topics --config-id 5
```

Replace `5` with your ReplicationConfig ID.

---

## Configuration

### Default Settings (Development)

Current defaults in `settings.py`:

```python
KAFKA_TOPIC_CONFIG = {
    'PARTITIONS': 1,                    # Single partition for message ordering
    'REPLICATION_FACTOR': 1,            # 1 broker = replication factor 1
    'RETENTION_MS': 604800000,          # 7 days
    'RETENTION_BYTES': -1,              # Unlimited size
    'CLEANUP_POLICY': 'delete',         # Delete old messages
    'COMPRESSION_TYPE': 'snappy',       # Balanced compression
    'MIN_INSYNC_REPLICAS': 1,           # 1 broker = min ISR 1
    'SEGMENT_BYTES': 1073741824,        # 1GB segments
    'SEGMENT_MS': 604800000,            # Roll segment every 7 days
}
```

### Override via Environment Variables

Add to your `.env` file:

```bash
# Example: Change retention to 30 days
KAFKA_TOPIC_RETENTION_MS=2592000000

# Example: Use 3 partitions for high throughput
KAFKA_TOPIC_PARTITIONS=3

# Example: Use LZ4 compression
KAFKA_TOPIC_COMPRESSION_TYPE=lz4
```

### Configuration Reference

| Setting | Description | Values | Default |
|---------|-------------|--------|---------|
| `PARTITIONS` | Number of partitions | 1-N | 1 |
| `REPLICATION_FACTOR` | Number of replicas | 1-N (≤ brokers) | 1 |
| `RETENTION_MS` | How long to keep messages | milliseconds or -1 | 604800000 (7 days) |
| `RETENTION_BYTES` | Max size per partition | bytes or -1 | -1 (unlimited) |
| `CLEANUP_POLICY` | How to delete old data | delete, compact | delete |
| `COMPRESSION_TYPE` | Compression algorithm | none, gzip, snappy, lz4, zstd | snappy |
| `MIN_INSYNC_REPLICAS` | Min replicas for write | 1-N (≤ RF) | 1 |

---

## Usage Examples

### Example 1: Create Topics for Specific Tables

```bash
python manage.py create_kafka_topics --topics \
    client_2.kbe.auth_user \
    client_2.kbe.busy_acc_greenera \
    client_2.kbe.django_content_type
```

### Example 2: Programmatic Usage in Python

```python
from client.utils.kafka_topic_manager import KafkaTopicManager

# Initialize manager
manager = KafkaTopicManager()

# Create single topic
manager.create_topic('client_2.kbe.my_table')

# Create multiple topics
manager.create_topics_bulk([
    'client_2.kbe.users',
    'client_2.kbe.orders',
    'client_2.kbe.products'
])

# Create topics for database tables
manager.create_cdc_topics_for_tables(
    server_name='client_2',
    database='kbe',
    table_names=['users', 'orders', 'products']
)

# Check if topic exists
if manager.topic_exists('client_2.kbe.users'):
    print("Topic exists!")

# Get topic configuration
config = manager.get_topic_config('client_2.kbe.users')
print(f"Retention: {config['retention.ms']} ms")
```

### Example 3: Custom Configuration

```python
from client.utils.kafka_topic_manager import KafkaTopicManager

manager = KafkaTopicManager()

# Create topic with custom partitions and replication
manager.create_topic(
    topic_name='client_2.kbe.high_volume_table',
    partitions=6,
    replication_factor=1,
    config_overrides={
        'retention.ms': '2592000000',  # 30 days
        'compression.type': 'lz4'
    }
)
```

---

## Production Setup

### Scaling from Development to Production

**Development (1 Kafka Broker):**
```bash
# .env
KAFKA_TOPIC_PARTITIONS=1
KAFKA_TOPIC_REPLICATION_FACTOR=1
KAFKA_TOPIC_RETENTION_MS=604800000  # 7 days
KAFKA_TOPIC_MIN_ISR=1
```

**Production (3 Kafka Brokers):**
```bash
# .env
KAFKA_TOPIC_PARTITIONS=3              # Higher throughput
KAFKA_TOPIC_REPLICATION_FACTOR=3      # Fault tolerance
KAFKA_TOPIC_RETENTION_MS=2592000000   # 30 days
KAFKA_TOPIC_MIN_ISR=2                 # Strong consistency
KAFKA_TOPIC_COMPRESSION_TYPE=zstd     # Best compression
```

### Retention Planning

Calculate your storage needs:

```
Daily CDC Events = Rows Changed Per Day
Event Size = ~1KB average
Daily Storage = Events × Size

Example with 100,000 daily changes:
- Daily: 100,000 × 1KB = 100MB
- 7 days: 700MB
- 30 days: 3GB
```

**Recommendations:**
- **Low volume** (< 10K changes/day): 7 days retention
- **Medium volume** (10K-100K/day): 7-14 days retention
- **High volume** (> 100K/day): 3-7 days retention or use size-based retention

### High Volume Tables

For tables with frequent changes:

```python
# Create high-volume table with more partitions
manager.create_topic(
    topic_name='client_2.kbe.transactions',
    partitions=6,  # Distribute load across partitions
    config_overrides={
        'retention.ms': '259200000',  # 3 days
        'retention.bytes': '5368709120',  # 5GB per partition
        'segment.bytes': '536870912',  # 512MB segments
    }
)
```

---

## Troubleshooting

### Issue: "Topic already exists"

**Solution:** Topic was already created (either manually or by Debezium auto-creation). This is not an error.

```bash
# List topics to verify
python manage.py create_kafka_topics --list --prefix client_2
```

### Issue: "Replication factor larger than available brokers"

**Error:**
```
org.apache.kafka.common.errors.InvalidReplicationFactorException
```

**Solution:** Reduce replication factor to match number of brokers:

```bash
# Check number of brokers
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | grep localhost | wc -l

# If you have 1 broker, set:
KAFKA_TOPIC_REPLICATION_FACTOR=1
```

### Issue: Topics not being consumed

**Check:**
1. Topic exists: `python manage.py create_kafka_topics --list`
2. Consumer is running: Check Celery logs
3. Consumer group ID is correct
4. No offset issues: `docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group your_group_id`

### Issue: High disk usage

**Solutions:**

1. **Reduce retention time:**
   ```bash
   KAFKA_TOPIC_RETENTION_MS=86400000  # 1 day
   ```

2. **Add size-based retention:**
   ```bash
   KAFKA_TOPIC_RETENTION_BYTES=1073741824  # 1GB per partition
   ```

3. **Enable compression:**
   ```bash
   KAFKA_TOPIC_COMPRESSION_TYPE=zstd  # Best compression ratio
   ```

### Issue: Slow performance

**Solutions:**

1. **Increase partitions** (sacrifices ordering):
   ```bash
   KAFKA_TOPIC_PARTITIONS=6
   ```

2. **Use faster compression**:
   ```bash
   KAFKA_TOPIC_COMPRESSION_TYPE=lz4  # Or 'none'
   ```

3. **Reduce retention**:
   ```bash
   KAFKA_TOPIC_RETENTION_MS=259200000  # 3 days
   ```

---

## Best Practices

### 1. Message Ordering
- Use **1 partition** if you need strict ordering of CDC events per table
- Multiple partitions break ordering guarantees

### 2. Partition Strategy
```
Low throughput (< 10 msg/sec):  1 partition
Medium (10-100 msg/sec):        3 partitions
High (> 100 msg/sec):           6+ partitions
```

### 3. Retention Strategy
```
Development:       7 days
Staging:          14 days
Production:       30 days (or based on compliance requirements)
```

### 4. Monitoring
```bash
# Check topic sizes
docker exec kafka kafka-log-dirs --describe --bootstrap-server localhost:9092 \
  --topic-list client_2.kbe.users

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group cdc_consumer_2_1
```

### 5. Pre-create Topics
Always pre-create topics instead of relying on auto-creation:
- ✅ Consistent configuration
- ✅ Controlled partitioning
- ✅ Proper retention settings
- ❌ Auto-creation uses broker defaults

---

## Environment-Specific Configurations

### Local Development
```bash
# .env.development
KAFKA_TOPIC_PARTITIONS=1
KAFKA_TOPIC_REPLICATION_FACTOR=1
KAFKA_TOPIC_RETENTION_MS=86400000  # 1 day
KAFKA_TOPIC_COMPRESSION_TYPE=none
```

### Staging
```bash
# .env.staging
KAFKA_TOPIC_PARTITIONS=1
KAFKA_TOPIC_REPLICATION_FACTOR=2
KAFKA_TOPIC_RETENTION_MS=604800000  # 7 days
KAFKA_TOPIC_COMPRESSION_TYPE=snappy
```

### Production
```bash
# .env.production
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_REPLICATION_FACTOR=3
KAFKA_TOPIC_RETENTION_MS=2592000000  # 30 days
KAFKA_TOPIC_COMPRESSION_TYPE=zstd
KAFKA_TOPIC_MIN_ISR=2
```

---

## Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Debezium MySQL Connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Confluent Kafka Admin API](https://docs.confluent.io/kafka-clients/python/current/overview.html#adminapi)