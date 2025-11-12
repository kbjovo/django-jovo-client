# Quick Start: Creating Kafka Topics

## ✨ Automatic Topic Creation (NEW!)

**Topics are now automatically created with your configured settings when you:**
- ✅ Create a new Debezium connector via UI
- ✅ Edit a connector and add new tables via UI

**No manual steps needed!** Topics use your custom configuration from `settings.py` and `.env`.

---

## 🎯 Current Configuration (Development)

Your topics will be created with these settings:

```
✓ Partitions: 1 (strict message ordering)
✓ Replication Factor: 1 (single broker)
✓ Retention: 7 days (604800000 ms)
✓ Compression: snappy
✓ Cleanup: delete (remove old messages)
✓ Storage: unlimited per partition
```

**These settings are automatically applied when creating topics through:**
- UI connector creation
- UI connector updates
- Manual management commands

---

## 📋 Quick Commands

### Show Current Settings
```bash
python manage.py create_kafka_topics --summary
```

### List All Topics
```bash
python manage.py create_kafka_topics --list
```

### List CDC Topics Only
```bash
python manage.py create_kafka_topics --list --prefix client_2
```

### Create Topics for All Your Replications
```bash
python manage.py create_kafka_topics --all
```

### Create Topics for Specific Replication
```bash
# First, find your config ID
python manage.py shell -c "from client.models import ReplicationConfig; [print(f'{c.id}: {c.connector_name}') for c in ReplicationConfig.objects.all()]"

# Then create topics
python manage.py create_kafka_topics --config-id YOUR_ID
```

### Create Specific Topics Manually
```bash
python manage.py create_kafka_topics --topics \
    client_2.kbe.table1 \
    client_2.kbe.table2 \
    client_2.kbe.table3
```

---

## 🚀 Production Scaling

### Step 1: Update .env for Production

When you move to production with 3 Kafka brokers, add to `.env`:

```bash
# Production with 3 Kafka brokers
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_REPLICATION_FACTOR=3
KAFKA_TOPIC_RETENTION_MS=2592000000  # 30 days
KAFKA_TOPIC_MIN_ISR=2
KAFKA_TOPIC_COMPRESSION_TYPE=zstd
```

### Step 2: Recreate Topics (one-time migration)

```bash
# Delete old topics (CAUTION: Data loss!)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic 'client_2.*'

# Create new topics with production config
python manage.py create_kafka_topics --all
```

---

## 💡 Common Scenarios

### Scenario 1: New Database Added
```bash
# Topics will be created automatically when you:
# 1. Add new replication config in UI
# 2. Select tables
# 3. Save connector

# Or manually:
python manage.py create_kafka_topics --config-id NEW_CONFIG_ID
```

### Scenario 2: Need More Retention (30 days)
```bash
# Add to .env:
echo "KAFKA_TOPIC_RETENTION_MS=2592000000" >> .env

# Restart Django
# Create new topics with this setting
python manage.py create_kafka_topics --topics client_2.kbe.important_table
```

### Scenario 3: High Volume Table Needs More Partitions
```python
# In Python shell or script
from client.utils.kafka_topic_manager import KafkaTopicManager

manager = KafkaTopicManager()
manager.create_topic(
    topic_name='client_2.kbe.high_volume_transactions',
    partitions=6,  # Override default
    config_overrides={
        'retention.ms': '259200000',  # 3 days
    }
)
```

---

## 🔍 Verification

### Check Topic Was Created
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic client_2.kbe.YOUR_TABLE
```

### Check Topic Size
```bash
docker exec kafka kafka-log-dirs --describe --bootstrap-server localhost:9092 \
  --topic-list client_2.kbe.YOUR_TABLE
```

### Check Messages in Topic
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic client_2.kbe.YOUR_TABLE \
  --from-beginning \
  --max-messages 5
```

---

## ⚠️ Important Notes

1. **Topic Names Follow Pattern**: `{server_name}.{database}.{table}`
   - Example: `client_2.kbe.auth_user`

2. **Can't Change Partitions**: Once created, you can only increase partitions, not decrease

3. **Retention is Important**:
   - Too short → Can't replay old data if consumer fails
   - Too long → High disk usage

4. **For CDC, Use 1 Partition**: Maintains strict ordering of database changes per table

5. **Auto-Creation Still Works**: If you don't pre-create, Debezium will auto-create with broker defaults (but you lose control over settings)

---

## 📚 Full Documentation

See [KAFKA_TOPIC_MANAGEMENT.md](./KAFKA_TOPIC_MANAGEMENT.md) for complete guide.