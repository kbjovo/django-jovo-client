# CDC Fix Summary - Schema History Issue

## Problem Identified

CDC was not working because the Debezium connector task was **FAILED** with error:
```
Could not find existing binlog information while attempting schema only recovery snapshot
```

### Root Cause
When using Option A (snapshot-first approach):
1. We copy data via SQL ✓
2. We create Debezium connector with `snapshot.mode: schema_only_recovery` ✗
3. **ERROR**: `schema_only_recovery` expects existing binlog offsets (from a previous connector run)
4. Since we deleted the old connector, all offsets were deleted
5. Debezium couldn't "recover" because there was nothing to recover from

## Solution Applied

Changed snapshot mode from `schema_only_recovery` → `schema_only`

### What `schema_only` Does:
✅ Takes a snapshot of the **schema structure ONLY** (not data)
✅ Initializes the schema history file (`/tmp/schema-history-{connector}.dat`)
✅ Establishes initial binlog position
✅ Starts CDC from current binlog position
✅ Does NOT copy data (we already did that via SQL)

### Files Modified

**1. client/replication/orchestrator.py (line 91)**
```python
# Before:
success, message = self._ensure_connector_running(snapshot_mode='schema_only_recovery')

# After:
success, message = self._ensure_connector_running(snapshot_mode='schema_only')
```

### Cleanup Performed
✅ Deleted failed connector: `gaurav_test_kbe_connector`
✅ Reset replication config status to `configured`
✅ Removed old schema history file

## Next Steps

### 1. Restart Replication
Go to: http://127.0.0.1:8000/cdc/config/14/monitor/

Click **"Start Replication"** button

### 2. Expected Flow
```
STEP 1/4: Validating prerequisites... ✓
STEP 2/4: Performing initial data sync... ✓ (will skip if already done)
STEP 3/4: Starting Debezium connector (CDC-only)... ✓
  - Creates connector with snapshot.mode: schema_only
  - Snapshots schema structure (NOT data)
  - Initializes schema history file
  - Establishes binlog position
STEP 4/4: Starting consumer with fresh group ID... ✓
```

### 3. Test CDC Replication

Once replication is running, test with a real database change:

```sql
-- In SOURCE database (kbe)
UPDATE tally_items SET some_column = 'new_value' WHERE id = 1;

-- Wait 2-3 seconds

-- Check TARGET database
SELECT * FROM target_tally_items WHERE id = 1;
```

The change should appear in target database within seconds! 🎉

## Diagnostic Tool

Run diagnostics anytime to check CDC health:
```bash
source .venv/bin/activate
python diagnose_cdc.py
```

This will show:
- Connector status
- Task status (should be RUNNING, not FAILED)
- Kafka topics (will be created after first CDC event)
- Consumer health
- Binlog configuration

## Debezium Snapshot Modes Reference

For future reference:

| Mode | Use Case |
|------|----------|
| `initial` | First-time setup: snapshot data + schema, then CDC |
| `schema_only` | **Option A**: Snapshot schema only (data already copied), then CDC |
| `never` | Pure CDC, no snapshot (requires existing binlog position) |
| `schema_only_recovery` | Recover from missing schema history (requires existing binlog position) |
| `when_needed` | Auto-decide: snapshot if no offsets exist |

## Why This Fix Works

**Option A (Snapshot-First) Flow:**
1. ✅ Copy data via SQL (fast, direct)
2. ✅ Use `schema_only` to establish binlog position WITHOUT copying data
3. ✅ Consumer starts reading CDC events from current binlog position
4. ✅ All future changes are replicated in real-time

**Benefits:**
- No duplicate data (we don't snapshot data twice)
- No offset issues (fresh binlog position established)
- Schema history properly initialized
- CDC starts from current point in time