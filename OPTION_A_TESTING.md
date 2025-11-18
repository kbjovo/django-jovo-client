# Option A Testing Guide - Simplified Snapshot-First Replication

## ✅ What's Been Implemented

### NEW Simplified Flow (4 Steps)
1. **Validate prerequisites** - Check Kafka, database connections, tables
2. **Perform initial SQL copy** - Direct database-to-database copy using pandas
3. **Start Debezium connector** - CDC-only mode (`snapshot.mode: never`)
4. **Start consumer** - Fresh consumer group ID with timestamp

### Key Features
- ✅ **Force re-sync option** - Truncates target tables and reloads all data
- ✅ **Smart sync strategy** - Small tables (<100K rows) sync inline, large tables in background
- ✅ **Database agnostic** - Works for MySQL, PostgreSQL, Oracle
- ✅ **Fresh consumer groups** - Timestamp-based to avoid offset issues
- ✅ **Comprehensive logging** - Every step logged with visual separators

---

## 🚀 Quick Test (5 Minutes)

### Prerequisites
1. Ensure Celery workers are running:
```bash
# Terminal 1: Main Celery worker
celery -A jovoclient worker --loglevel=info

# Terminal 2: Celery beat (for scheduled tasks)
celery -A jovoclient beat --loglevel=info
```

2. Ensure Kafka services are running:
```bash
docker ps | grep -E 'kafka|zookeeper|schema-registry|connect'
```

### Test 1: Fresh Start (No Existing Data)

```python
from client.models import ReplicationConfig
from client.replication import ReplicationOrchestrator

# Get your replication config
config = ReplicationConfig.objects.get(id=6)  # Use your config ID

# Create orchestrator
orchestrator = ReplicationOrchestrator(config)

# Start replication with new simplified flow
success, message = orchestrator.start_replication(force_resync=False)

print(f"Success: {success}")
print(f"Message: {message}")
```

**Expected Console Output:**
```
============================================================
STARTING REPLICATION (SIMPLIFIED)
============================================================
[client_2_db_5_connector] STEP 1/4: Validating prerequisites...
[client_2_db_5_connector] ✓ All prerequisites validated
[client_2_db_5_connector] STEP 2/4: Performing initial data sync...
[client_2_db_5_connector] Syncing table users...
[client_2_db_5_connector] Source table has 150 rows
[client_2_db_5_connector] Target table already has 0 rows
[client_2_db_5_connector] ✓ Synced 150 rows to testdb_users
[client_2_db_5_connector] Syncing table orders...
[client_2_db_5_connector] Source table has 2500 rows
[client_2_db_5_connector] ✓ Synced 2500 rows to testdb_orders
[client_2_db_5_connector] ✓ Initial sync completed: 2/2 tables, 2650 rows
[client_2_db_5_connector] ✓ Initial data sync completed
[client_2_db_5_connector] STEP 3/4: Starting Debezium connector (CDC-only)...
[client_2_db_5_connector] Deleted old connector for fresh start
[client_2_db_5_connector] Creating connector with snapshot_mode=never...
[client_2_db_5_connector] Creating Debezium connector...
[client_2_db_5_connector] ✓ Connector created: client_2_db_5_connector
[client_2_db_5_connector] ✓ Connector is running: client_2_db_5_connector
[client_2_db_5_connector] STEP 4/4: Starting consumer with fresh group ID...
[client_2_db_5_connector] Starting consumer with group: cdc_consumer_2_6_1732080456
[client_2_db_5_connector] ✓ Consumer task queued: abc-123-def-456
[client_2_db_5_connector] ============================================================
[client_2_db_5_connector] ✓ REPLICATION STARTED SUCCESSFULLY
[client_2_db_5_connector] ============================================================
```

**In Celery Worker Terminal:**
```
================================================================================
🎧 STARTING KAFKA CONSUMER
   Config ID: 6
   Task ID: abc-123-def-456
================================================================================
✓ Loaded config: client_2_db_5_connector
✓ Target database: 127.0.0.1:3306/target_db
✓ Target database engine created
✓ Subscribing to 2 topics:
   - client_2_db_5.source_db.users
   - client_2_db_5.source_db.orders
✓ Using custom consumer group: cdc_consumer_2_6_1732080456
✓ Updated config state to STARTING
🔄 Creating ResilientKafkaConsumer...
================================================================================
✓ CONSUMER READY - Starting message consumption loop
================================================================================
```

---

### Test 2: Force Re-sync (Truncate and Reload)

```python
# Force re-sync (truncate target tables and reload all data)
success, message = orchestrator.start_replication(force_resync=True)

print(f"Success: {success}")
print(f"Message: {message}")
```

**Expected Console Output:**
```
[client_2_db_5_connector] STEP 2/4: Performing initial data sync...
[client_2_db_5_connector] Syncing table users...
[client_2_db_5_connector] Source table has 150 rows
[client_2_db_5_connector] Truncating target table testdb_users...
[client_2_db_5_connector] ✓ Synced 150 rows to testdb_users
...
```

---

### Test 3: Verify CDC (Change Data Capture)

After replication is running, insert/update/delete in source database:

```sql
-- In SOURCE database
INSERT INTO users (name, email, created_at)
VALUES ('Test CDC', 'cdc@test.com', NOW());

UPDATE users SET email = 'updated@test.com' WHERE name = 'Test CDC';

DELETE FROM users WHERE name = 'Test CDC';
```

**Check Target Database (within 2-3 seconds):**
```sql
-- In TARGET database
SELECT * FROM testdb_users ORDER BY id DESC LIMIT 10;
```

**Expected:**
- New row appears immediately (INSERT)
- Row email updated immediately (UPDATE)
- Row deleted immediately (DELETE)

**In Celery Worker Logs:**
```
[Consumer abc-123] Processed INSERT on testdb_users (1 rows)
[Consumer abc-123] Processed UPDATE on testdb_users (1 rows)
[Consumer abc-123] Processed DELETE on testdb_users (1 rows)
```

---

## 🔍 Monitoring & Debugging

### Check Unified Status

```python
status = orchestrator.get_unified_status()
print(status)
```

**Expected Output:**
```python
{
    'overall': 'healthy',
    'connector': {
        'state': 'RUNNING',
        'healthy': True,
        'tasks': [{'state': 'RUNNING', 'id': 0}],
        'message': 'Connector is RUNNING'
    },
    'consumer': {
        'state': 'RUNNING',
        'healthy': True,
        'last_heartbeat': '2025-11-18T10:30:45Z',
        'heartbeat_recent': True,
        'message': 'Consumer is healthy and processing messages'
    },
    'config': {
        'status': 'active',
        'is_active': True,
        'connector_name': 'client_2_db_5_connector',
        'kafka_topic_prefix': 'client_2_db_5'
    },
    'statistics': {
        'total_rows_synced': 2650,
        'tables_enabled': 2,
        'last_sync_at': '2025-11-18T10:30:00Z'
    },
    'timestamp': '2025-11-18T10:30:45Z'
}
```

### Check Database State

```sql
SELECT
    id,
    connector_name,
    status,
    is_active,
    connector_state,
    consumer_state,
    consumer_last_heartbeat,
    last_error_message
FROM replication_config
WHERE id = 6;
```

**Expected:**
```
id                      | 6
connector_name          | client_2_db_5_connector
status                  | active
is_active               | true
connector_state         | RUNNING
consumer_state          | RUNNING
consumer_last_heartbeat | 2025-11-18 10:30:45 (updates every 30s)
last_error_message      | (empty)
```

### Watch Logs in Real-Time

```bash
# Watch all replication logs
tail -f logs/celery.log | grep "\[client_"

# Watch only initial sync
tail -f logs/celery.log | grep "Syncing table"

# Watch only errors
tail -f logs/celery.log | grep "ERROR"

# Watch consumer heartbeats
tail -f logs/celery.log | grep "Heartbeat"
```

---

## ✅ Success Criteria

After running these tests, you should see:

1. ✅ Initial data copied successfully via direct SQL
2. ✅ Debezium connector created in CDC-only mode (`snapshot.mode: never`)
3. ✅ Consumer started with fresh timestamp-based group ID
4. ✅ No offset errors or schema history errors
5. ✅ CDC works in real-time (INSERT/UPDATE/DELETE within 2-3 seconds)
6. ✅ Comprehensive logs visible in console/log files
7. ✅ `force_resync=True` truncates and reloads all data
8. ✅ Replication state tracked in database

---

## 🐛 Troubleshooting

### Issue: "No target database configured"

**Solution:** Check that client has target database set:
```python
client = config.client_database.client
target_db = client.get_target_database()
print(target_db)  # Should not be None
```

### Issue: "Failed to copy table"

**Check Logs:**
```python
print(config.last_error_message)
```

**Common Causes:**
- Database credentials incorrect
- Table doesn't exist in target
- Schema mismatch between source and target

### Issue: Consumer not receiving messages

**Check Topics:**
```bash
# List Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092 | grep client_

# Check topic has messages
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic client_2_db_5.source_db.users \
    --from-beginning --max-messages 10
```

**Check Consumer Group:**
```python
# Check what consumer group is being used
print(config.consumer_task_id)

# Check Celery logs for consumer group ID
tail -f logs/celery.log | grep "consumer group"
```

---

## 🎯 Next Steps

After successful testing:

1. **Integrate into UI** - Add "Force Re-sync" checkbox in monitoring page
2. **Add progress tracking** - Show initial sync progress in UI
3. **Implement large table async** - Currently falls back to sync for >100K rows
4. **Add alerting** - Email/Slack notifications on failures
5. **Build metrics dashboard** - Real-time replication statistics

---

## 📝 Code Changes Summary

### Modified Files:
1. **[client/replication/orchestrator.py](client/replication/orchestrator.py:47-686)** - Added simplified 4-step flow with initial SQL sync
2. **[client/utils/connector_templates.py](client/utils/connector_templates.py:346-393)** - Added `snapshot_mode` parameter to all connector generators
3. **[client/tasks.py](client/tasks.py:225-355)** - Added `consumer_group_override` and comprehensive logging

### No Database Migration Needed
The health monitoring migration was already applied. No new migration required for Option A.

---

**Ready to test!** 🚀

Run the tests above and check the logs. The new simplified flow should eliminate all offset and schema history issues.