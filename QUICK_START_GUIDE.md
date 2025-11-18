# Quick Start Guide - Fixed UI Flow

## ✅ What's Fixed

The UI now uses the **ReplicationOrchestrator** and will:
- Show the correct buttons based on state
- Not try to restart connectors that don't exist
- Use the simplified 4-step flow (Option A)

---

## 🚀 What to Do Now

You have **two options**:

### Option 1: Start with Existing Config (Fastest)

If your config is already saved (`status='configured'`), just click **"Start Replication"** on the monitor page.

1. Go to the monitor page (you're probably already there)
2. Refresh the page
3. You should see a **"Start Replication"** button
4. Click it
5. ✅ Watch the logs for the 4-step flow!

```bash
# In another terminal, watch the logs
tail -f logs/celery.log | grep -E "STEP|Syncing|rows"
```

---

### Option 2: Start from Scratch (Clean Slate)

If you want to reconfigure everything:

1. **Delete the existing config from Django admin** (optional)
   - Go to Django admin → Replication Configs → Delete config ID 11

2. **Go through UI flow**:
   - Client detail page → "Set up CDC"
   - Discover tables
   - Select tables
   - Configure mappings
   - Click "Create Connector" (just saves config now)
   - Click "Start Replication" (orchestrator does the work)

3. **Watch the magic happen!**

---

## 📊 What You Should See

### In UI:

After clicking "Start Replication":
- ✅ Success message: "Replication started successfully!"
- ✅ Status badge changes to "Active"
- ✅ Connector state: "RUNNING"
- ✅ Consumer state: "RUNNING"

### In Logs:

```bash
tail -f logs/celery.log | grep -E "STEP|Syncing"
```

**Expected output:**
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
[testient_test_kbe_connector] ✓ Connector created
[testient_test_kbe_connector] STEP 4/4: Starting consumer with fresh group ID...
============================================================
✓ REPLICATION STARTED SUCCESSFULLY
============================================================
```

### In Target Database:

```sql
-- Check that data was copied
SELECT COUNT(*) FROM testdb_users;
-- Should see all rows from source!
```

---

## 🔍 Verify It's Working

### 1. Check Target Database Has Data

```sql
-- In TARGET database
SELECT COUNT(*) FROM testdb_users;
SELECT * FROM testdb_users ORDER BY id DESC LIMIT 5;
```

✅ **Should see all rows from source database**

### 2. Test CDC (Real-time)

```sql
-- In SOURCE database
INSERT INTO users (name, email, created_at)
VALUES ('Test CDC', 'test@cdc.com', NOW());

-- Wait 2-3 seconds, then check TARGET database
SELECT * FROM testdb_users WHERE email = 'test@cdc.com';
```

✅ **Should see the new row appear within seconds!**

### 3. Check Connector Status

```bash
curl http://localhost:8083/connectors/testient_test_kbe_connector/status | python -m json.tool
```

**Expected:**
```json
{
  "name": "testient_test_kbe_connector",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING"
    }
  ]
}
```

✅ **No errors, both connector and task state: RUNNING**

---

## �� Quick Checklist

- [ ] UI monitor page shows "Start Replication" button
- [ ] Clicked "Start Replication"
- [ ] Logs show 4-step flow (STEP 1/4, 2/4, 3/4, 4/4)
- [ ] Initial data appears in target database
- [ ] Connector status is RUNNING (no schema history error)
- [ ] Consumer is running (check heartbeat updating)
- [ ] CDC works in real-time (INSERT/UPDATE/DELETE)

---

## 🐛 Still Have Issues?

### Issue: "Connector not found" error

**This is normal!** After you deleted the old connector, it doesn't exist anymore. You need to create it fresh via "Start Replication" button.

### Issue: Don't see "Start Replication" button

**Refresh the page!** The view has been updated to show the correct buttons.

### Issue: Button is grayed out or doesn't work

**Check the config status:**
```python
python manage.py shell

from client.models import ReplicationConfig
config = ReplicationConfig.objects.get(id=11)
print(f"Status: {config.status}")
print(f"Is Active: {config.is_active}")

# If needed, reset it:
config.status = 'configured'
config.is_active = False
config.save()
```

Then refresh the UI and try again.

---

## ✅ Expected Result

After following these steps:

1. ✅ No schema history errors
2. ✅ Initial data copied to target database
3. ✅ CDC working in real-time
4. ✅ Connector state: RUNNING
5. ✅ Consumer state: RUNNING
6. ✅ Heartbeat updating every 30 seconds

**You're all set!** 🎉

---

## 📞 Next Steps

Once replication is working:

1. **Test with real data changes** - INSERT/UPDATE/DELETE in source
2. **Monitor the dashboard** - Watch the status badges
3. **Check the logs** - Look for any errors or warnings
4. **Verify data consistency** - Compare row counts between source and target

If everything looks good, your simplified replication flow (Option A) is working perfectly! 🚀