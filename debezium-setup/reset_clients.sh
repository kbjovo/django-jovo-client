#!/bin/bash
set -e

echo "🔻 Stopping and removing containers + volumes..."
docker compose down -v

echo "🚀 Starting containers..."
docker compose up -d

echo "🧹 Removing old migrations..."
docker compose exec django bash -c "rm -rf client/migrations/"
echo "✔️ Deleted client/migrations/"

echo "🛠 Running makemigrations + migrate..."
docker compose exec django python manage.py makemigrations client
docker compose exec django python manage.py migrate

echo "📦 Installing database drivers..."
docker compose exec django pip install --upgrade pip
docker compose exec django pip install pymssql
docker compose exec django pip install oracledb  # ✅ ADD THIS LINE

echo "✅ Verifying Oracle driver installation..."
docker compose exec django python -c "import oracledb; print(f'✔️ oracledb version: {oracledb.__version__}')"

echo "➕ Resetting client & adding DB connectors..."

docker compose exec -T django python manage.py shell << 'EOF'
from client.models.client import Client
from client.models.database import ClientDatabase

Client.objects.filter(email="test@example.com").delete()

client = Client.objects.create(
    name="Test Client",
    email="test@example.com",
    phone="9999999999",
    db_name="replica_db",
    company_name="Test Company",
    address="123 Street",
    city="Mumbai",
    state="Maharashtra",
    country="India",
    postal_code="400001",
)

ClientDatabase.objects.create(
    client=client,
    connection_name="mysql-connector",
    db_type="mysql",
    host="192.168.0.50",
    port=3306,
    username="vivek",
    password="root",
    database_name="kbe",
)

ClientDatabase.objects.create(
    client=client,
    connection_name="postgres-connector",
    db_type="postgresql",
    host="192.168.0.50",
    port=5432,
    username="root",
    password="root",
    database_name="kbbio",
)

ClientDatabase.objects.create(
    client=client,
    connection_name="sqlserver-connector",
    db_type="mssql",
    host="192.168.0.50",
    port=1433,
    username="jovo",
    password="Admin@123",
    database_name="AppDB",
)

ClientDatabase.objects.create(
    client=client,
    connection_name="oracle-connector",
    db_type="oracle",
    host="192.168.0.50",
    port=1521,
    username="jovo",
    password="Admin@123",
    database_name="ORCLPDB1",
)

print("✔️ Client and database connectors created successfully!")
EOF

echo "🔍 Testing Oracle connection..."
docker compose exec -T django python manage.py shell << 'EOF'
from client.models.database import ClientDatabase
oracle_db = ClientDatabase.objects.get(db_type='oracle')
print(f"\n📊 Testing Oracle connection: {oracle_db.connection_name}")
print(f"   Host: {oracle_db.host}:{oracle_db.port}")
print(f"   Service: {oracle_db.database_name}")
print(f"   User: {oracle_db.username}")

try:
    oracle_db.check_connection_status(save=False)
    print(f"✅ Oracle connection successful!")
except Exception as e:
    print(f"❌ Oracle connection failed: {e}")
EOF

echo "🎉 Reset complete!"