# Phase B2: Step 2 - Debezium Integration Complete ✅

## Files Created

### 1. `client/utils/debezium_manager.py`
Complete Debezium Connector Manager with REST API integration.

#### Key Features:
- ✅ Create/Delete/Update connectors
- ✅ Pause/Resume/Restart connectors
- ✅ Get connector status and tasks
- ✅ Validate configurations
- ✅ Health checks
- ✅ Automatic error notifications
- ✅ Comprehensive error handling

#### Main Class: `DebeziumConnectorManager`

**Methods:**
```python
# Health & Status
manager.check_kafka_connect_health()              # Check if Kafka Connect is running
manager.list_connectors()                         # Get all connectors
manager.get_connector_status(connector_name)      # Get detailed status
manager.get_connector_config(connector_name)      # Get configuration
manager.get_connector_tasks(connector_name)       # Get task information

# Connector Operations
manager.create_connector(name, config)            # Create new connector
manager.delete_connector(name)                    # Delete connector
manager.update_connector_config(name, config)     # Update configuration
manager.pause_connector(name)                     # Pause connector
manager.resume_connector(name)                    # Resume paused connector
manager.restart_connector(name)                   # Restart connector

# Validation
manager.validate_connector_config(class, config)  # Validate before creating
```

---

### 2. `client/utils/connector_templates.py`
Configuration templates for different database types.

#### Key Functions:

**Connector Name Generation:**
```python
generate_connector_name(client, db_config)
# Returns: "acme_corp_production_db_connector"
```

**Configuration Generators:**
```python
# MySQL
get_mysql_connector_config(
    client, 
    db_config, 
    tables_whitelist=['users', 'orders']
)

# PostgreSQL
get_postgresql_connector_config(
    client, 
    db_config, 
    schema_name='public'
)

# Oracle
get_oracle_connector_config(client, db_config)

# Auto-detect and generate
get_connector_config_for_database(db_config)
```

**Utility Functions:**
```python
# Get available snapshot modes
get_snapshot_modes()
# Returns: {
#   'initial': 'Performs initial snapshot...',
#   'when_needed': '...',
#   'never': '...',
# }

# Validate configuration
validate_connector_config(config)
# Returns: (is_valid, [list_of_errors])
```

#### MySQL Connector Configuration Details:

The template includes:
- ✅ **Connection settings** (host, port, user, password, database)
- ✅ **Server identification** (unique server ID, server name)
- ✅ **Topic configuration** (prefix: `client_{id}`)
- ✅ **Schema history tracking** (using Kafka topics)
- ✅ **Snapshot mode** (initial, when_needed, never, schema_only)
- ✅ **Data type handling** (decimal: precise, binary: bytes)
- ✅ **Time precision** (adaptive_time_microseconds)
- ✅ **Table whitelist** (optional table filtering)
- ✅ **Connection pooling** (max queue size, batch size)
- ✅ **Retry logic** (connection timeouts, backoff strategy)

---

### 3. `client/management/commands/test_debezium.py`
Interactive testing command for Debezium operations.

#### Usage Examples:

```bash
# Health check and list connectors
python manage.py test_debezium

# List all connectors
python manage.py test_debezium --list

# Get connector status
python manage.py test_debezium --status acme_corp_db_connector

# Create connector for database ID 1
python manage.py test_debezium --create --db-id 1

# Create connector for specific tables only
python manage.py test_debezium --create --db-id 1 --tables "users,orders,products"

# Delete a connector
python manage.py test_debezium --delete acme_corp_db_connector
```

---

## How It Works: Complete Flow

### 1. Creating a Connector

```python
from client.models import Client, ClientDatabase
from client.utils import (
    DebeziumConnectorManager,
    get_connector_config_for_database,
    generate_connector_name,
)

# Get database configuration
db = ClientDatabase.objects.get(id=1)
client = db.client

# Initialize manager
manager = DebeziumConnectorManager()

# Generate connector name
connector_name = generate_connector_name(client, db)
# Result: "acme_corp_production_db_connector"

# Generate configuration
config = get_connector_config_for_database(
    db_config=db,
    tables_whitelist=['users', 'orders', 'products']  # Optional
)

# Create connector
success, error = manager.create_connector(connector_name, config)

if success:
    print(f"✅ Connector created: {connector_name}")
    
    # Check status
    exists, status = manager.get_connector_status(connector_name)
    if exists:
        state = status['connector']['state']
        print(f"Status: {state}")
else:
    print(f"❌ Failed: {error}")
```

### 2. Monitoring Connectors

```python
# List all connectors
connectors = manager.list_connectors()
print(f"Found {len(connectors)} connectors")

# Get detailed status
for connector_name in connectors:
    exists, status = manager.get_connector_status(connector_name)
    
    if exists:
        connector_state = status['connector']['state']
        tasks = status['tasks']
        
        print(f"{connector_name}: {connector_state}")
        print(f"  Tasks: {len(tasks)}")
        
        for task in tasks:
            print(f"  - Task {task['id']}: {task['state']}")
```

### 3. Handling Failures

```python
# Get connector status
exists, status = manager.get_connector_status(connector_name)

if exists:
    state = status['connector']['state']
    
    if state == 'FAILED':
        print("Connector failed, restarting...")
        
        # Restart connector
        success, error = manager.restart_connector(connector_name)
        
        if success:
            print("✅ Connector restarted")
        else:
            # Send notification (automatic with notify_on_error=True)
            manager.create_connector(connector_name, config, notify_on_error=True)
```

---

## Testing Your Setup

### Prerequisites:

1. **Docker services running:**
```bash
cd ~/django-jovo-client/debezium-setup
docker-compose up -d
./scripts/check-services.sh
```

2. **ClientDatabase configured:**
```bash
python manage.py shell
```

```python
from client.models import Client, ClientDatabase

# Create or get client
client = Client.objects.first()

# Create database connection
db = ClientDatabase.objects.create(
    client=client,
    connection_name="Test MySQL DB",
    db_type="MySQL",
    host="localhost",  # or your MySQL host
    port=3306,
    database_name="your_test_database",
    username="your_user",
    password="your_password",  # Will be encrypted
)

print(f"Created database with ID: {db.id}")
```

### Test Sequence:

```bash
# 1. Health check
python manage.py test_debezium

# 2. Create connector (interactive)
python manage.py test_debezium --create --db-id 1

# 3. Check status
python manage.py test_debezium --status your_connector_name

# 4. Monitor in Kafka UI
# Open: http://localhost:8080
```

---

## Configuration Reference

### Snapshot Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `initial` | Full snapshot on first start | New replication setup |
| `when_needed` | Snapshot if needed | After crashes/restarts |
| `never` | No snapshots, only CDC | Existing data already synced |
| `schema_only` | Schema only, no data | Testing schema changes |

### Connector States

| State | Meaning | Action |
|-------|---------|--------|
| `RUNNING` | Working normally | ✅ Monitor |
| `PAUSED` | Manually paused | Resume when ready |
| `FAILED` | Error occurred | Check logs, restart |
| `UNASSIGNED` | No worker assigned | Check cluster |

### Common Configurations

**Real-time CDC (recommended):**
```python
config = {
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "include.schema.changes": "true",
}
```

**Schema-only (testing):**
```python
config = {
    "snapshot.mode": "schema_only",
    "include.schema.changes": "true",
}
```

**Specific tables only:**
```python
config = {
    "table.include.list": "mydb.users,mydb.orders,mydb.products",
}
```

---

## Kafka Topics Created

When you create a connector, Debezium automatically creates topics:

### Data Topics
- `client_{id}.{database}.{table}` - One topic per table
- Example: `client_1.production.users`

### Schema History
- `schema-changes.{connector_name}`
- Example: `schema-changes.acme_corp_db_connector`

### Monitoring in Kafka UI

1. Open http://localhost:8080
2. Click "Topics"
3. Look for topics with prefix `client_{your_client_id}`
4. Click on a topic to see messages

---

## Troubleshooting

### Problem: Connector fails to start

**Check logs:**
```bash
docker logs debezium-connect
```

**Common issues:**
- ❌ Binary logging not enabled → Check with `test_utils.py`
- ❌ Wrong credentials → Test connection first
- ❌ Database not accessible → Check firewall/network
- ❌ Invalid configuration → Use `validate_connector_config()`

### Problem: No data in topics

**Check:**
1. Connector status: `python manage.py test_debezium --status CONNECTOR_NAME`
2. Kafka UI topics: http://localhost:8080
3. Does source database have data?
4. Is binary log enabled? (MySQL)

### Problem: Connector status shows FAILED

**Actions:**
1. Get detailed status with trace
2. Check Debezium logs
3. Restart connector: `manager.restart_connector(name)`
4. If persistent, delete and recreate

---

## Next Steps - Step 3

Now that Debezium integration is complete, we need to build the **Kafka Consumer** that:

1. Reads messages from Debezium topics
2. Parses CDC events (INSERT/UPDATE/DELETE)
3. Writes data to client databases
4. Handles schema changes
5. Manages errors and retries

Ready to proceed to Step 3? 🚀