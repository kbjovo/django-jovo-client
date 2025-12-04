#!/bin/bash

set -e  # Exit on error

echo "🔻 Stopping and removing containers + volumes..."
docker compose down -v

echo "🚀 Starting containers..."
docker compose up -d

echo "🧹 Removing old migrations..."
docker compose exec django bash -c "
    rm -rf clients/migrations/
"
echo "✔️ Deleted clients/migrations/"

echo "🛠 Running makemigrations + migrate..."
docker compose exec django python manage.py makemigrations client
docker compose exec django python manage.py migrate

echo "➕ Creating test Client entry..."

docker compose exec -T django python manage.py shell << 'EOF'
from client.models.client import Client

Client.objects.create(
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
print("✔️ Client created successfully!")
EOF

echo "🎉 All done!"
