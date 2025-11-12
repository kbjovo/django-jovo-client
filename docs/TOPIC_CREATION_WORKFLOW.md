# Kafka Topic Creation Workflow

## 🎯 Overview

Your CDC system now **automatically creates Kafka topics** with your custom configuration whenever you create or update connectors through the UI.

---

## 📊 How It Works

### When You Create a Connector

```
┌─────────────────────────────────────────────────────────────┐
│  User Action: Create Connector via UI                       │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Create Debezium Connector in Kafka Connect         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Read Settings from KAFKA_TOPIC_CONFIG             │
│  - Partitions: 1                                            │
│  - Replication Factor: 1                                    │
│  - Retention: 7 days                                        │
│  - Compression: snappy                                      │
│  - etc.                                                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Create Kafka Topics for Each Table                │
│  - client_2.kbe.auth_user                                   │
│  - client_2.kbe.orders                                      │
│  - client_2.kbe.products                                    │
│  (with your custom configuration)                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Save ReplicationConfig & Show Success Message      │
└─────────────────────────────────────────────────────────────┘
```

### When You Update a Connector (Add Tables)

```
┌─────────────────────────────────────────────────────────────┐
│  User Action: Edit Connector → Add New Tables              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Update TableMappings in Database                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Detect Newly Added Tables                         │
│  - customers (NEW)                                          │
│  - invoices (NEW)                                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Create Kafka Topics for New Tables Only           │
│  - client_2.kbe.customers                                   │
│  - client_2.kbe.invoices                                    │
│  (existing topics are skipped)                              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  System: Restart Connector (if requested)                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔧 Configuration Flow

### 1. **Default Settings** (settings.py)

```python
KAFKA_TOPIC_CONFIG = {
    'PARTITIONS': 1,
    'REPLICATION_FACTOR': 1,
    'RETENTION_MS': 604800000,  # 7 days
    # ... other settings
}
```

### 2. **Environment Overrides** (.env)

```bash
# Override defaults for your environment
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_RETENTION_MS=2592000000  # 30 days
```

### 3. **Runtime Application**

When connector is created:
```python
topic_manager = KafkaTopicManager()
# Reads from settings + .env overrides
topic_manager.create_cdc_topics_for_tables(...)
```

---

## 📝 Code Integration Points

### File: `client/views.py`

#### 1. Connector Creation (`cdc_create_connector` - Line ~978)

```python
# After connector is created successfully...

# Pre-create Kafka topics with proper configuration
from client.utils.kafka_topic_manager import KafkaTopicManager
topic_manager = KafkaTopicManager()

topic_results = topic_manager.create_cdc_topics_for_tables(
    server_name=replication_config.debezium_server_name,
    database=db_config.database_name,
    table_names=tables_list  # All enabled tables
)
```

**What happens:**
- Reads `KAFKA_TOPIC_CONFIG` from settings
- Creates topic for each table in `tables_list`
- Uses configured partitions, retention, compression, etc.
- Logs success/failure for each topic
- **Does NOT fail connector creation** if topic creation fails

#### 2. Connector Update (`cdc_config_update` - Line ~1724)

```python
# After tables are added/updated...

# Track newly added tables
newly_added_tables = []  # Populated during table updates

# Create topics for newly added tables only
if newly_added_tables:
    topic_manager = KafkaTopicManager()
    topic_results = topic_manager.create_cdc_topics_for_tables(
        server_name=replication_config.debezium_server_name,
        database=replication_config.source_db.database_name,
        table_names=newly_added_tables  # Only new tables
    )
```

**What happens:**
- Detects which tables are newly added
- Creates topics only for new tables
- Skips existing tables to avoid errors
- Logs results

---

## ⚙️ What Gets Created

For a connector named `gaurav_kbe_con_connector` with tables `users`, `orders`:

### Topics Created:
```
client_2.kbe.users
client_2.kbe.orders
```

### With Configuration:
```
Topic: client_2.kbe.users
  Partitions: 1
  Replication Factor: 1
  Configs:
    retention.ms=604800000 (7 days)
    retention.bytes=-1 (unlimited)
    cleanup.policy=delete
    compression.type=snappy
    min.insync.replicas=1
    segment.bytes=1073741824 (1GB)
    segment.ms=604800000 (7 days)
```

---

## 🎨 Failure Behavior

**If topic creation fails:**
- ❌ Connector creation **FAILS** (operation aborted)
- 🔄 Connector is **automatically deleted** (rollback)
- ⚠️ Error message displayed to user
- 📝 Error logged to Django logs

**Why this is critical:**
- Connector without topics is **useless** (can't publish CDC events)
- Fail fast to prevent broken configurations
- Forces user to fix Kafka issues before proceeding
- Ensures data integrity from the start

**Common causes:**
1. **Kafka broker is down** → Start Kafka first
2. **Network issues** → Check connectivity
3. **Permission issues** → Verify Kafka ACLs
4. **Disk full** → Free up disk space

---

## 🚀 Manual Override (Optional)

You can still manually create/manage topics if needed:

### Command Line:
```bash
# Create for all configs
python manage.py create_kafka_topics --all

# Create for specific config
python manage.py create_kafka_topics --config-id 5

# Create specific topics
python manage.py create_kafka_topics --topics client_2.kbe.custom_table
```

### Python Code:
```python
from client.utils.kafka_topic_manager import KafkaTopicManager

manager = KafkaTopicManager()

# Custom configuration for high-volume table
manager.create_topic(
    topic_name='client_2.kbe.high_volume_table',
    partitions=6,  # Override default
    config_overrides={
        'retention.ms': '259200000',  # 3 days
        'compression.type': 'zstd'
    }
)
```

---

## 🔍 Verification

### Check Topics Were Created:
```bash
# List all CDC topics
python manage.py create_kafka_topics --list --prefix client_2

# Describe specific topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic client_2.kbe.users
```

### Check Django Logs:
```
INFO kafka_topic_manager Created 3 Kafka topics for connector gaurav_kbe_con_connector
```

Or if topic already exists:
```
WARNING kafka_topic_manager Topic 'client_2.kbe.users' already exists
```

---

## 📚 Configuration Reference

See full configuration options in:
- [KAFKA_TOPIC_MANAGEMENT.md](./KAFKA_TOPIC_MANAGEMENT.md) - Complete guide
- [QUICK_START_TOPICS.md](./QUICK_START_TOPICS.md) - Quick commands

---

## 🎯 Best Practices

1. **Set retention before creating connectors**
   ```bash
   # Add to .env BEFORE creating connectors
   KAFKA_TOPIC_RETENTION_MS=2592000000  # 30 days
   ```

2. **Use 1 partition for CDC** (default)
   - Maintains strict ordering of database changes
   - Essential for CDC correctness

3. **Scale in production**
   ```bash
   # When moving to 3-broker production cluster
   KAFKA_TOPIC_REPLICATION_FACTOR=3
   KAFKA_TOPIC_MIN_ISR=2
   ```

4. **Monitor disk usage**
   - Retention × Daily Changes = Disk Usage
   - Adjust retention if disk fills up

5. **Pre-create for important tables**
   - UI auto-creation handles 99% of cases
   - For critical tables, manually create with custom settings

---

## 🐛 Troubleshooting

### Topics Not Created?

**Check Django Logs:**
```bash
tail -f logs/django.log | grep kafka_topic_manager
```

**Possible causes:**
1. Kafka broker down → Check `docker ps | grep kafka`
2. Configuration error → Check `python manage.py create_kafka_topics --summary`
3. Permission issues → Check Kafka Connect logs

### Topics Created with Wrong Config?

**If auto-created by Kafka (not by your code):**
- Delete and recreate:
  ```bash
  docker exec kafka kafka-topics --delete --topic client_2.kbe.wrong_config
  python manage.py create_kafka_topics --topics client_2.kbe.wrong_config
  ```

### High Disk Usage?

**Reduce retention:**
```bash
# Add to .env
KAFKA_TOPIC_RETENTION_MS=259200000  # 3 days

# Recreate topics or alter existing ones
```

---

## ✅ Summary

**Automatic Integration:**
- ✅ Topics created when connector created
- ✅ Topics created when tables added to connector
- ✅ Uses your custom configuration from settings/env
- ❌ **Fails fast** if topics can't be created (ensures data integrity)
- 🔄 Automatic rollback on failure
- ✅ Manual override available when needed

**Critical behavior:** Connector creation/update **fails** if Kafka topics can't be created, ensuring you never have a broken configuration. Fix Kafka issues first, then retry. 🎯