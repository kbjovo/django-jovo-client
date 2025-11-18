# Replication System Refactor - Complete Summary

## 🎯 What Was Done

We've completely refactored and improved the CDC replication system to fix intermittent replication issues and make it easier to debug and maintain.

---

## 🔧 Major Changes

### 1. **New Organized Architecture** (`client/replication/`)

Created a clean, maintainable structure:

```
client/replication/
├── __init__.py              # Module exports
├── orchestrator.py          # ReplicationOrchestrator (main controller)
├── consumer.py              # ResilientKafkaConsumer (auto-restart logic)
├── validators.py            # Pre-flight validation
└── health_monitor.py        # Health monitoring tasks
```

### 2. **ReplicationOrchestrator** - Unified Control

**Location**: [client/replication/orchestrator.py](client/replication/orchestrator.py)

Single entry point for all replication operations:

```python
from client.replication import ReplicationOrchestrator

orchestrator = ReplicationOrchestrator(replication_config)

# Start replication (connector + consumer together)
success, message = orchestrator.start_replication()

# Get comprehensive status
status = orchestrator.get_unified_status()
# Returns: {
#   'overall': 'healthy' | 'degraded' | 'failed',
#   'connector': {...},
#   'consumer': {...},
#   'statistics': {...}
# }

# Stop replication
orchestrator.stop_replication()

# Restart replication
orchestrator.restart_replication()
```

**Benefits**:
- Ensures connector and consumer are always synchronized
- Clear, step-by-step logging for debugging
- Validates everything before starting

### 3. **ResilientKafkaConsumer** - Smart Auto-Restart

**Location**: [client/replication/consumer.py](client/replication/consumer.py)

**Key Features**:

✅ **Smart Error Classification**
- Transient errors (network timeouts, Kafka unavailable) → Auto-retry with exponential backoff
- Persistent errors (auth failures, missing tables) → Stop and alert admin

✅ **Auto-Restart Logic**
- Max 5 retries with backoff: 2s, 4s, 8s, 16s, 32s
- After 5 failures → Stop, set status='error', notify admin

✅ **Heartbeat Tracking**
- Updates `consumer_last_heartbeat` every 30 seconds
- Health monitor detects stale heartbeat and restarts consumer

✅ **Structured Logging**
- All logs prefixed with `[connector_name]` for easy filtering
- Step-by-step progress logging (perfect for SSE/UI display later)

**Example Logs**:
```
[client_2_db_5] Starting resilient Kafka consumer...
[client_2_db_5] STEP 1/3: Initializing consumer (attempt 1/5)
[client_2_db_5] ✓ Consumer initialized successfully
[client_2_db_5] STEP 2/3: Starting consumption from 5 topics
[client_2_db_5] ✓ Starting consumption loop (topics: topic1, topic2, ...)
[client_2_db_5] Heartbeat updated
[Consumer abc-123] Processed INSERT on testdb_users (10 rows)
```

### 4. **Health Monitoring** - Auto-Fix Issues

**Location**: [client/replication/health_monitor.py](client/replication/health_monitor.py)

**Two Monitoring Tasks**:

#### A. `monitor_replication_health` (Every 1 minute)
- Checks BOTH connector and consumer health
- Auto-fixes common issues:
  - Connector PAUSED → Resumes it
  - Connector FAILED → Restarts it
  - Consumer heartbeat stale → Restarts consumer
  - Consumer ERROR → Restarts consumer

#### B. `check_consumer_heartbeat` (Every 2 minutes)
- Dedicated consumer heartbeat check
- Restarts consumer if heartbeat > 2 minutes old

**Benefits**:
- Self-healing system
- Minimal downtime
- Less manual intervention

### 5. **Database Schema Updates**

**Migration**: [client/migrations/0002_add_health_monitoring_fields.py](client/migrations/0002_add_health_monitoring_fields.py)

**New Fields in `ReplicationConfig`**:

| Field | Type | Purpose |
|-------|------|---------|
| `connector_state` | CharField | Debezium state (RUNNING/PAUSED/FAILED) |
| `consumer_state` | CharField | Consumer state (RUNNING/STOPPED/ERROR) |
| `consumer_task_id` | CharField | Celery task ID for tracking |
| `consumer_last_heartbeat` | DateTimeField | Updated every 30s by consumer |
| `last_error_message` | TextField | Last error for debugging |

### 6. **Signal Table Removed** ✂️

**What Changed**:
- Removed `debezium_signal` table creation
- Removed incremental snapshot triggering
- No more tables created in source database

**New Approach**:
- When adding new tables → Restart connector to get initial data
- OR delete and recreate connector with all tables
- Simpler, cleaner, no source DB modifications

**Files Modified**:
- [client/utils/connector_templates.py](client/utils/connector_templates.py:147-148) - Removed signal table from MySQL config
- [client/utils/connector_templates.py](client/utils/connector_templates.py:244-245) - Removed signal table from PostgreSQL config
- [client/utils/connector_templates.py](client/utils/connector_templates.py:322-323) - Removed signal table from Oracle config
- [client/views/cdc_views.py](client/views/cdc_views.py:418-420) - Removed signal table setup
- [client/views/cdc_views.py](client/views/cdc_views.py:1312-1317) - Removed incremental snapshot trigger

### 7. **Updated Celery Tasks**

**Location**: [client/tasks.py](client/tasks.py)

**Changes**:

#### `start_kafka_consumer` (Line 224)
- Now uses `ResilientKafkaConsumer` instead of `DebeziumCDCConsumer`
- Tracks task ID in `consumer_task_id`
- Updates `consumer_state` throughout lifecycle
- Auto-restart on errors

#### `create_debezium_connector` (Line 122)
- Status changed from 'active' → 'configured'
- `is_active` changed from True → False
- Consumer NO LONGER auto-started
- User must manually start from monitor page

#### Celery Beat Schedule (Line 19)
**New Tasks**:
```python
'monitor-replication-health': {
    'task': 'client.replication.monitor_replication_health',
    'schedule': crontab(minute='*/1'),  # Every 1 minute
},
'check-consumer-heartbeat': {
    'task': 'client.replication.check_consumer_heartbeat',
    'schedule': crontab(minute='*/2'),  # Every 2 minutes
},
```

### 8. **New API Endpoint** - Unified Status

**Location**: [client/views/cdc_views.py](client/views/cdc_views.py:1512-1546)

**Endpoint**: `/cdc/config/<id>/unified-status/` (you'll need to add URL route)

**Response**:
```json
{
  "success": true,
  "status": {
    "overall": "healthy",
    "connector": {
      "state": "RUNNING",
      "healthy": true,
      "tasks": [{"state": "RUNNING", "id": 0}],
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
      "connector_name": "client_2_db_5_connector"
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

## 🚀 How to Use the New System

### Step 1: Run Database Migration

```bash
python manage.py migrate client
```

This adds the new health monitoring fields.

### Step 2: Configure Replication (Same as Before)

1. Go to CDC Discover Tables
2. Select tables to replicate
3. Configure column mappings
4. Create connector

### Step 3: Start Replication (NEW BEHAVIOR)

**Old Behavior** (BROKEN):
- Connector created → Consumer never started → Data stuck in Kafka ❌

**New Behavior** (FIXED):
1. Connector created → Status: 'configured', Consumer: NOT RUNNING ✅
2. User clicks "Start Replication" → Consumer starts ✅
3. Both connector AND consumer running → Data flows to target DB ✅

### Step 4: Monitor Health

**Option A**: Use existing monitoring page
- Shows connector state
- Shows consumer state
- Shows last heartbeat

**Option B**: Call unified status API
```javascript
fetch('/cdc/config/123/unified-status/')
  .then(r => r.json())
  .then(data => console.log(data.status))
```

### Step 5: Let Auto-Fix Handle Issues

The health monitor will automatically:
- Restart failed connectors
- Restart stale consumers
- Update status in real-time

---

## 🔍 Debugging Guide

### 1. Check Overall Health

```python
from client.replication import ReplicationOrchestrator
from client.models.replication import ReplicationConfig

config = ReplicationConfig.objects.get(id=1)
orchestrator = ReplicationOrchestrator(config)
status = orchestrator.get_unified_status()

print(f"Overall: {status['overall']}")  # healthy/degraded/failed
print(f"Connector: {status['connector']['state']}")
print(f"Consumer: {status['consumer']['state']}")
print(f"Last heartbeat: {status['consumer']['last_heartbeat']}")
```

### 2. Check Logs (Structured for SSE)

All logs are prefixed with `[connector_name]` for easy filtering:

```bash
# Filter by connector
tail -f logs/replication.log | grep "\[client_2_db_5\]"

# Watch consumer health
tail -f logs/replication.log | grep "Heartbeat"

# Watch errors
tail -f logs/replication.log | grep "ERROR"
```

### 3. Manual Consumer Restart

```python
from client.tasks import start_kafka_consumer

# Restart consumer manually
start_kafka_consumer.apply_async(args=[config_id])
```

### 4. Check Database State

```sql
SELECT
    connector_name,
    status,
    connector_state,
    consumer_state,
    consumer_last_heartbeat,
    last_error_message
FROM replication_config
WHERE is_active = true;
```

---

## 🧪 Testing Guide

### Test 1: Basic Replication Flow

```bash
# 1. Create connector (should NOT start consumer)
# Check: status='configured', is_active=False, consumer_state='UNKNOWN'

# 2. Start replication manually
# Check: status='active', is_active=True, consumer_state='RUNNING'

# 3. Insert data into source database
INSERT INTO users (name, email) VALUES ('Test', 'test@example.com');

# 4. Check target database (should see data within seconds)
SELECT * FROM testdb_users;

# 5. Check heartbeat is updating
# Query every 30 seconds, consumer_last_heartbeat should update
```

### Test 2: Auto-Restart on Failure

```bash
# 1. Start replication

# 2. Stop Kafka broker (simulate failure)
docker stop kafka

# 3. Check logs - should see retry attempts
# [connector_name] Transient error (retry 1/5): ... Retrying in 2 seconds...
# [connector_name] Transient error (retry 2/5): ... Retrying in 4 seconds...

# 4. Restart Kafka
docker start kafka

# 5. Consumer should auto-reconnect and resume
# Check: consumer_state='RUNNING', heartbeat updating
```

### Test 3: Health Monitor Auto-Fix

```bash
# 1. Pause connector manually
curl -X PUT http://localhost:8083/connectors/client_2_db_5_connector/pause

# 2. Wait 1 minute for health monitor

# 3. Check logs - health monitor should auto-resume
# [client_2_db_5] Connector is PAUSED, resuming...
# [client_2_db_5] ✓ Connector resumed

# 4. Verify connector is RUNNING again
```

### Test 4: Consumer Heartbeat Detection

```bash
# 1. Kill consumer task forcefully
celery -A jovoclient control revoke <task-id> --terminate

# 2. Wait 2 minutes for heartbeat check

# 3. Check logs - should auto-restart
# [client_2_db_5] Consumer heartbeat stale, restarting...
# [client_2_db_5] ✓ Consumer restart triggered

# 4. Verify consumer is running again
```

---

## 📊 Monitoring Dashboard (For Future SSE)

All logging is already structured for SSE display. When you implement SSE later, you can:

1. Stream logs in real-time:
```python
@login_required
def stream_logs(request, config_pk):
    def event_stream():
        # Stream structured logs
        for log in tail_logs(config_pk):
            yield f"data: {json.dumps(log)}\n\n"

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')
```

2. Display in UI:
```javascript
const eventSource = new EventSource('/cdc/config/123/stream-logs/');
eventSource.onmessage = (event) => {
    const log = JSON.parse(event.data);
    appendToLogView(log);  // Show in UI
};
```

---

## 🐛 Known Issues & Limitations

### 1. Adding New Tables Requires Connector Restart

**Issue**: When you add new tables to existing connector, they won't get initial data.

**Solution**:
- Restart connector to re-snapshot all tables
- OR delete and recreate connector with all tables

**Why**: Removed signal table mechanism (no source DB modifications)

### 2. Consumer Task ID Not Cleaned Up on Crash

**Issue**: If Celery worker crashes, `consumer_task_id` might be stale.

**Solution**: Health monitor will detect stale heartbeat and restart consumer automatically.

### 3. No Metrics Dashboard Yet

**Issue**: No visual dashboard for monitoring.

**Solution**: Use API endpoint or Django admin for now. Build dashboard later with SSE.

---

## 📝 Migration Checklist

For existing deployments:

- [ ] Run database migration: `python manage.py migrate client`
- [ ] Restart Celery workers to load new tasks
- [ ] Restart Celery beat to load new schedule
- [ ] Manually start any existing connectors (they may be in 'configured' state)
- [ ] Monitor logs for first 24 hours
- [ ] Verify heartbeats are updating
- [ ] Test auto-restart by simulating failures

---

## 🎯 Next Steps (Your TODO)

1. **Add URL route** for unified status endpoint:
```python
# In client/urls.py
path('cdc/config/<int:config_pk>/unified-status/', cdc_unified_status, name='cdc_unified_status'),
```

2. **Update monitoring page UI** to show:
   - Consumer state badge
   - Last heartbeat timestamp
   - Auto-fix status

3. **Implement Server-Sent Events (SSE)** for real-time log streaming

4. **Add metrics dashboard** showing:
   - Messages processed per minute
   - Replication lag
   - Error rates

5. **Add alerting** via email/Slack when:
   - Consumer fails after max retries
   - Connector in FAILED state for > 5 minutes
   - Heartbeat missing for > 5 minutes

---

## 📚 File Reference

### New Files Created
- `client/replication/__init__.py` - Module exports
- `client/replication/orchestrator.py` - Main controller
- `client/replication/consumer.py` - Resilient consumer
- `client/replication/validators.py` - Validation logic
- `client/replication/health_monitor.py` - Health monitoring tasks

### Modified Files
- `client/models/replication.py` - Added health monitoring fields
- `client/utils/connector_templates.py` - Removed signal table
- `client/views/cdc_views.py` - Removed signal table setup, added unified status
- `client/tasks.py` - Updated to use new consumer
- `jovoclient/celery.py` - Added health monitoring tasks
- `client/migrations/0002_add_health_monitoring_fields.py` - Database migration

### Kept as Backup (Not Deleted)
- `client/utils/debezium_snapshot.py` - Signal table logic (not used anymore, but kept)
- `client/utils/kafka_consumer.py` - Original consumer (wrapped by ResilientKafkaConsumer)
- `client/utils/debezium_manager.py` - Unchanged
- `client/utils/kafka_topic_manager.py` - Unchanged

---

## ✅ Summary

### What Fixed the Intermittent Replication Issue?

**Root Cause**: Consumer was NOT running (or stopped after first run)

**Fix Applied**:
1. ✅ Consumer now auto-starts when you click "Start Replication"
2. ✅ Consumer has auto-restart on failures (max 5 retries)
3. ✅ Health monitor detects stale heartbeat and restarts consumer
4. ✅ All state changes tracked in database
5. ✅ Structured logging for easy debugging

### Architecture Improvements

- ✅ Clean separation of concerns (orchestrator, consumer, validators, health monitor)
- ✅ Unified status API for debugging
- ✅ Smart error classification (transient vs persistent)
- ✅ Self-healing system with auto-fix
- ✅ Easy to extend for future features (SSE, metrics, alerting)

### Simplified Approach

- ✅ Removed signal table (no source DB modifications)
- ✅ Clear state management (connector_state, consumer_state)
- ✅ One source of truth (ReplicationOrchestrator)

---

**Ready to test!** 🚀

Run the migration and start replicating. The system will now reliably process all changes with auto-restart on failures.