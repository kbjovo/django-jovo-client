# Quick Testing Guide - Replication System

## 🚀 Quick Start (5 Minutes)

### 1. Apply Database Migration

```bash
cd /root/projects/django/django-jovo-client
python manage.py migrate client
```

**Expected Output**:
```
Running migrations:
  Applying client.0002_add_health_monitoring_fields... OK
```

### 2. Restart Services

```bash
# Restart Celery workers (load new consumer code)
# If using systemd:
sudo systemctl restart celery

# If running manually:
# Ctrl+C to stop existing worker, then:
celery -A jovoclient worker --loglevel=info

# In another terminal, restart Celery beat (load new schedule):
celery -A jovoclient beat --loglevel=info
```

### 3. Check Celery Beat Schedule

```bash
# Should see new tasks in the schedule
celery -A jovoclient inspect scheduled
```

**Expected to see**:
- `monitor-replication-health` (every 1 minute)
- `check-consumer-heartbeat` (every 2 minutes)

---

## 🧪 Test Scenario 1: Fresh Replication Setup

### Step 1: Create Connector (via UI)

1. Go to CDC workflow
2. Discover tables
3. Select tables and configure mappings
4. Click "Create Connector"

**Expected Result**:
- Connector created successfully
- Status: `configured` (NOT `active`)
- `is_active`: `False`
- Consumer NOT started yet

**Verify in Database**:
```sql
SELECT
    connector_name,
    status,
    is_active,
    connector_state,
    consumer_state
FROM replication_config
ORDER BY id DESC LIMIT 1;
```

**Expected**:
```
connector_name    | client_X_db_Y_connector
status            | configured
is_active         | false
connector_state   | NULL or RUNNING
consumer_state    | UNKNOWN
```

### Step 2: Start Replication Manually

1. Go to monitoring page
2. Click "Start Replication" button

**Expected Result**:
- Status changes to `active`
- `is_active` becomes `True`
- Consumer task starts
- `consumer_state` becomes `RUNNING`
- `consumer_task_id` populated
- `consumer_last_heartbeat` starts updating

**Verify**:
```sql
SELECT
    connector_name,
    status,
    is_active,
    consumer_state,
    consumer_task_id,
    consumer_last_heartbeat
FROM replication_config
ORDER BY id DESC LIMIT 1;
```

**Expected**:
```
status                  | active
is_active               | true
consumer_state          | RUNNING
consumer_task_id        | abc-123-def-456
consumer_last_heartbeat | 2025-11-17 15:30:45 (updates every 30s)
```

### Step 3: Insert Test Data

```sql
-- In SOURCE database
INSERT INTO users (name, email, created_at)
VALUES ('Test User', 'test@example.com', NOW());
```

### Step 4: Check Target Database (Within 5 seconds)

```sql
-- In TARGET database
SELECT * FROM testdb_users ORDER BY id DESC LIMIT 5;
```

**Expected**:
- New row appears with 'Test User'
- Data replicated successfully ✅

---

## 🧪 Test Scenario 2: Auto-Restart on Kafka Failure

### Step 1: Stop Kafka Temporarily

```bash
# If using Docker
docker stop kafka

# Wait 10 seconds
```

### Step 2: Watch Logs

```bash
tail -f logs/celery.log | grep "client_"
```

**Expected to see**:
```
[client_2_db_5] Transient error (retry 1/5): Kafka unavailable. Retrying in 2 seconds...
[client_2_db_5] Transient error (retry 2/5): Kafka unavailable. Retrying in 4 seconds...
[client_2_db_5] Transient error (retry 3/5): Kafka unavailable. Retrying in 8 seconds...
```

### Step 3: Restart Kafka

```bash
docker start kafka
```

**Expected**:
```
[client_2_db_5] ✓ Consumer initialized successfully
[client_2_db_5] ✓ Starting consumption loop
[client_2_db_5] Heartbeat updated
```

✅ **Consumer auto-recovered!**

### Step 4: Verify Replication Still Works

```sql
-- Insert another row
INSERT INTO users (name, email) VALUES ('After Recovery', 'recovery@example.com');

-- Check target DB
SELECT * FROM testdb_users ORDER BY id DESC LIMIT 1;
```

---

## 🧪 Test Scenario 3: Health Monitor Auto-Fix

### Step 1: Pause Connector Manually

```bash
# Pause via Kafka Connect API
curl -X PUT http://localhost:8083/connectors/client_2_db_5_connector/pause
```

### Step 2: Wait 1 Minute (Health Monitor Runs)

```bash
# Watch logs
tail -f logs/celery.log | grep "health"
```

**Expected to see**:
```
[2025-11-17 15:30:00] HEALTH MONITORING - Starting health check
[2025-11-17 15:30:01] [client_2_db_5] Health: degraded (Connector: ✗, Consumer: ✓)
[2025-11-17 15:30:02] [client_2_db_5] Connector is PAUSED, resuming...
[2025-11-17 15:30:03] [client_2_db_5] ✓ Connector resumed
[2025-11-17 15:30:04] HEALTH MONITORING - Summary:
  Total: 1
  Healthy: 1
  Degraded: 0
  Failed: 0
  Auto-fixed: 1
```

### Step 3: Verify Connector is Running

```bash
curl http://localhost:8083/connectors/client_2_db_5_connector/status
```

**Expected**:
```json
{
  "connector": {
    "state": "RUNNING"
  }
}
```

✅ **Health monitor auto-fixed the issue!**

---

## 🧪 Test Scenario 4: Heartbeat Detection

### Step 1: Kill Consumer Task

```bash
# Find consumer task ID
SELECT consumer_task_id FROM replication_config WHERE is_active = true;

# Revoke it
celery -A jovoclient control revoke <task-id> --terminate --signal=SIGKILL
```

### Step 2: Wait 2 Minutes (Heartbeat Check Runs)

**Expected**:
```
[2025-11-17 15:32:00] Checking consumer heartbeats...
[2025-11-17 15:32:01] [client_2_db_5] Heartbeat stale (150s ago), restarting consumer...
[2025-11-17 15:32:02] 🎧 Starting resilient Kafka consumer for ReplicationConfig 1
[2025-11-17 15:32:03] ✓ Consumer started successfully
```

### Step 3: Verify Consumer is Running Again

```sql
SELECT
    consumer_state,
    consumer_task_id,
    consumer_last_heartbeat
FROM replication_config
WHERE id = 1;
```

**Expected**:
```
consumer_state          | RUNNING
consumer_task_id        | new-task-id (different from before)
consumer_last_heartbeat | 2025-11-17 15:32:30 (updating every 30s)
```

✅ **Consumer auto-restarted!**

---

## 🧪 Test Scenario 5: Unified Status API

### Test API Endpoint

```bash
# Call unified status API (you may need to add URL route first)
curl http://localhost:8000/cdc/config/1/unified-status/ \
  -H "Authorization: Bearer <token>" \
  | python -m json.tool
```

**Expected Response**:
```json
{
  "success": true,
  "status": {
    "overall": "healthy",
    "connector": {
      "state": "RUNNING",
      "healthy": true,
      "tasks": [
        {
          "id": 0,
          "state": "RUNNING"
        }
      ],
      "message": "Connector is RUNNING"
    },
    "consumer": {
      "state": "RUNNING",
      "healthy": true,
      "last_heartbeat": "2025-11-17T15:30:45Z",
      "heartbeat_recent": true,
      "message": "Consumer is healthy and processing messages"
    },
    "config": {
      "status": "active",
      "is_active": true,
      "connector_name": "client_2_db_5_connector",
      "kafka_topic_prefix": "client_2"
    },
    "statistics": {
      "total_rows_synced": 10000,
      "tables_enabled": 5,
      "last_sync_at": "2025-11-17T15:30:00Z"
    },
    "timestamp": "2025-11-17T15:30:45Z"
  }
}
```

---

## 📊 Monitoring Commands

### Check Active Consumers

```bash
# Celery inspect active tasks
celery -A jovoclient inspect active | grep start_kafka_consumer
```

### Check Database State

```sql
-- See all active replications
SELECT
    id,
    connector_name,
    status,
    is_active,
    connector_state,
    consumer_state,
    TIMESTAMPDIFF(SECOND, consumer_last_heartbeat, NOW()) as seconds_since_heartbeat
FROM replication_config
WHERE is_active = true;
```

### Watch Logs in Real-Time

```bash
# All replication logs
tail -f logs/celery.log | grep "\[client_"

# Only errors
tail -f logs/celery.log | grep "ERROR"

# Only heartbeats
tail -f logs/celery.log | grep "Heartbeat"

# Health monitor
tail -f logs/celery.log | grep "HEALTH"
```

---

## 🐛 Troubleshooting

### Issue: Consumer not starting

**Check**:
```sql
SELECT status, is_active, consumer_state, last_error_message
FROM replication_config WHERE id = 1;
```

**Solutions**:
- If `status='configured'` → Click "Start Replication" manually
- If `consumer_state='ERROR'` → Check `last_error_message`
- If `last_error_message` shows auth error → Fix database credentials

### Issue: Heartbeat not updating

**Check**:
```sql
SELECT
    consumer_task_id,
    consumer_last_heartbeat,
    TIMESTAMPDIFF(SECOND, consumer_last_heartbeat, NOW()) as age_seconds
FROM replication_config WHERE id = 1;
```

**Solutions**:
- If `age_seconds > 120` → Health monitor should auto-restart (wait 2 minutes)
- If `consumer_task_id` is NULL → Consumer not running, start manually
- Check Celery worker is running: `celery -A jovoclient inspect active`

### Issue: Data not appearing in target DB

**Check**:
1. Connector state: `SELECT connector_state FROM replication_config WHERE id = 1;`
2. Consumer state: `SELECT consumer_state FROM replication_config WHERE id = 1;`
3. Kafka topics: `kafka-topics --list --bootstrap-server localhost:9092 | grep client_`

**Solutions**:
- If connector != RUNNING → Check connector logs
- If consumer != RUNNING → Start consumer manually
- If topics missing → Topics should auto-create on first CDC event

---

## ✅ Success Criteria

After testing, you should see:

1. ✅ Connector creates and starts successfully
2. ✅ Consumer starts when you click "Start Replication"
3. ✅ Data flows from source to target within seconds
4. ✅ Heartbeat updates every 30 seconds
5. ✅ Consumer auto-restarts on Kafka failures
6. ✅ Health monitor auto-fixes connector issues
7. ✅ Unified status API returns complete health info

---

## 📞 Need Help?

If something doesn't work:

1. **Check logs first**: `tail -f logs/celery.log`
2. **Check database state**: Run monitoring SQL queries above
3. **Check Celery is running**: `celery -A jovoclient inspect active`
4. **Check Kafka Connect**: `curl http://localhost:8083/connectors`

**Common Issues**:
- Celery worker not running → Start it
- Celery beat not running → Start it
- Kafka Connect down → Start it
- Database credentials wrong → Fix in config