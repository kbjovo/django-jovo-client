# UI Orchestrator Integration - Complete

## ✅ What Was Updated

All UI views now use the **ReplicationOrchestrator** instead of calling tasks directly.

---

## 🔧 Updated Views

### 1. **cdc_create_connector** ([cdc_views.py:384-434](client/views/cdc_views.py#L384-L434))

**OLD Behavior:**
- Created Debezium connector directly
- Created Kafka topics manually
- Complex error handling for topic creation

**NEW Behavior:**
- Just saves the configuration
- Sets `status='configured'` and `is_active=False`
- **Does NOT create the actual connector**
- Orchestrator will create connector when user clicks "Start Replication"

**Why This is Better:**
- Simpler UI code
- No schema history errors during connector creation
- Connector created with correct `snapshot.mode: never` by orchestrator

---

### 2. **start_replication** ([cdc_views.py:706-750](client/views/cdc_views.py#L706-L750))

**OLD Behavior:**
- Called `create_debezium_connector.apply_async()` task
- Returned task ID

**NEW Behavior:**
- Uses `ReplicationOrchestrator.start_replication()`
- Performs complete 4-step flow:
  1. Validate prerequisites
  2. Perform initial SQL copy
  3. Start Debezium connector (CDC-only mode)
  4. Start consumer with fresh group ID
- Supports `force_resync` parameter from UI

**Example:**
```python
orchestrator = ReplicationOrchestrator(config)
success, message = orchestrator.start_replication(force_resync=False)
```

---

### 3. **stop_replication** ([cdc_views.py:754-788](client/views/cdc_views.py#L754-L788))

**OLD Behavior:**
- Called `stop_kafka_consumer.apply_async()`

**NEW Behavior:**
- Uses `ReplicationOrchestrator.stop_replication()`
- Stops both consumer and pauses connector

---

### 4. **restart_replication_view** ([cdc_views.py:792-822](client/views/cdc_views.py#L792-L822))

**OLD Behavior:**
- Called `restart_replication.apply_async()`

**NEW Behavior:**
- Uses `ReplicationOrchestrator.restart_replication()`
- Performs stop + start with 2 second delay

---

### 5. **replication_status** ([cdc_views.py:758-780](client/views/cdc_views.py#L758-L780))

**OLD Behavior:**
- Manually queried connector status from DebeziumConnectorManager
- Limited status information

**NEW Behavior:**
- Uses `ReplicationOrchestrator.get_unified_status()`
- Returns comprehensive status:
  - Overall health ('healthy', 'degraded', 'failed')
  - Connector state and tasks
  - Consumer state and heartbeat
  - Statistics (rows synced, tables enabled)
  - Timestamps

**Example Response:**
```json
{
  "success": true,
  "status": {
    "overall": "healthy",
    "connector": {
      "state": "RUNNING",
      "healthy": true,
      "tasks": [{"id": 0, "state": "RUNNING"}],
      "message": "Connector is RUNNING"
    },
    "consumer": {
      "state": "RUNNING",
      "healthy": true,
      "last_heartbeat": "2025-11-18T10:30:45Z",
      "heartbeat_recent": true,
      "message": "Consumer is healthy and processing messages"
    },
    "config": {
      "status": "active",
      "is_active": true,
      "connector_name": "testient_test_kbe_connector",
      "kafka_topic_prefix": "client_2_db_5"
    },
    "statistics": {
      "total_rows_synced": 0,
      "tables_enabled": 2,
      "last_sync_at": null
    },
    "timestamp": "2025-11-18T10:30:45Z"
  }
}
```

---

## 🧪 How to Test

### Step 1: Delete Old Failed Connector

```bash
curl -X DELETE http://localhost:8083/connectors/testient_test_kbe_connector
```

### Step 2: Go Through UI Flow

1. **Navigate to CDC Setup**
   - Go to client detail page
   - Click "Set up CDC replication"

2. **Discover Tables**
   - Select tables to replicate
   - Click "Next"

3. **Configure Tables**
   - Map columns
   - Click "Create Connector"
   - ✅ **Status should be: `configured`, is_active: `False`**
   - ✅ **No actual Debezium connector created yet**

4. **Start Replication**
   - Click "Start Replication" button
   - ✅ **Orchestrator performs 4-step flow**
   - ✅ **Initial SQL copy runs**
   - ✅ **Connector created with `snapshot.mode: never`**
   - ✅ **Consumer started with fresh group ID**

5. **Check Logs**
   ```bash
   tail -f logs/celery.log | grep "STEP\|Syncing\|rows"
   ```

   **Expected:**
   ```
   ============================================================
   STARTING REPLICATION (SIMPLIFIED)
   ============================================================
   [testient_test_kbe_connector] STEP 1/4: Validating prerequisites...
   [testient_test_kbe_connector] ✓ All prerequisites validated
   [testient_test_kbe_connector] STEP 2/4: Performing initial data sync...
   [testient_test_kbe_connector] Syncing table users...
   [testient_test_kbe_connector] Source table has 150 rows
   [testient_test_kbe_connector] ✓ Synced 150 rows to testdb_users
   [testient_test_kbe_connector] STEP 3/4: Starting Debezium connector (CDC-only)...
   [testient_test_kbe_connector] ✓ Connector created: testient_test_kbe_connector
   [testient_test_kbe_connector] STEP 4/4: Starting consumer with fresh group ID...
   [testient_test_kbe_connector] Starting consumer with group: cdc_consumer_2_6_1732080456
   ============================================================
   ✓ REPLICATION STARTED SUCCESSFULLY
   ============================================================
   ```

6. **Verify Data in Target Database**
   ```sql
   -- Check target database
   SELECT COUNT(*) FROM testdb_users;
   ```
   ✅ **Should see all rows from source**

7. **Test CDC (Real-time Changes)**
   ```sql
   -- In SOURCE database
   INSERT INTO users (name, email) VALUES ('Test CDC', 'test@cdc.com');

   -- In TARGET database (check within 2-3 seconds)
   SELECT * FROM testdb_users ORDER BY id DESC LIMIT 1;
   ```
   ✅ **Should see the new row**

---

## 🎯 What This Fixes

### ❌ Old Problem: Schema History Error
```
io.debezium.DebeziumException: The db history topic is missing.
```

### ✅ New Solution:
- Orchestrator uses `snapshot.mode: never` (CDC-only)
- No schema history needed for snapshots
- File-based schema history for ongoing CDC
- **No more schema history errors!**

---

## 🔍 Monitoring the New Flow

### Check Unified Status via AJAX

JavaScript in your monitoring page can poll this endpoint:

```javascript
// Every 5 seconds, get unified status
setInterval(() => {
  fetch('/cdc/config/6/status/')  // Adjust config_id
    .then(r => r.json())
    .then(data => {
      if (data.success) {
        const status = data.status;
        console.log('Overall:', status.overall);
        console.log('Connector:', status.connector.state);
        console.log('Consumer:', status.consumer.state);
        console.log('Heartbeat:', status.consumer.last_heartbeat);

        // Update UI badges
        updateStatusBadge(status.overall);
        updateConnectorBadge(status.connector.state);
        updateConsumerBadge(status.consumer.state);
      }
    });
}, 5000);
```

---

## 📊 Expected Flow Diagram

```
USER FLOW (NEW):

1. [UI] Configure Tables
   ↓
2. [UI] Click "Create Connector"
   ↓
   [Backend] Just saves config (status='configured')
   ↓
3. [UI] Click "Start Replication"
   ↓
   [Orchestrator] STEP 1: Validate prerequisites
   ↓
   [Orchestrator] STEP 2: Initial SQL copy (direct DB-to-DB)
   ↓
   [Orchestrator] STEP 3: Create Debezium connector (snapshot.mode: never)
   ↓
   [Orchestrator] STEP 4: Start consumer (fresh group ID)
   ↓
4. [UI] Replication Running ✅
   ↓
   [Background] CDC captures INSERT/UPDATE/DELETE in real-time
```

---

## 🐛 Troubleshooting

### Issue: "Connector already exists"

**Solution:**
```bash
# Delete old connector
curl -X DELETE http://localhost:8083/connectors/testient_test_kbe_connector

# Try again from UI
```

### Issue: "No rows in target table after starting"

**Check:**
1. **Logs:** `tail -f logs/celery.log | grep "Syncing"`
2. **Source has data:** `SELECT COUNT(*) FROM users;` (source DB)
3. **Orchestrator ran:** Look for "STEP 2/4: Performing initial data sync..."

### Issue: "Consumer not receiving messages"

**Check:**
1. **Consumer state:** `/cdc/config/6/status/` → `consumer.state` should be `RUNNING`
2. **Heartbeat:** `consumer.last_heartbeat` should update every 30 seconds
3. **Topics exist:** `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092 | grep client_`

---

## ✅ Success Criteria

After testing, you should see:

1. ✅ **Configuration saves successfully** without creating connector
2. ✅ **Start Replication performs 4-step flow** with detailed logs
3. ✅ **Initial data copied** to target database immediately
4. ✅ **No schema history errors** in connector status
5. ✅ **CDC works in real-time** (INSERT/UPDATE/DELETE within 2-3 seconds)
6. ✅ **Unified status API** returns comprehensive health information
7. ✅ **Consumer heartbeat updates** every 30 seconds

---

## 📝 Files Modified

- **[client/views/cdc_views.py](client/views/cdc_views.py)** - All views updated to use orchestrator
- **[client/replication/orchestrator.py](client/replication/orchestrator.py)** - Already had simplified flow
- **[client/tasks.py](client/tasks.py)** - Already had enhanced logging

**No database migrations needed** - Health monitoring fields already added.

---

**Ready to test the new UI flow!** 🚀

Delete the old failed connector and go through the UI flow from scratch. The orchestrator will handle everything correctly.