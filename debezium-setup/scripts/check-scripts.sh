#!/bin/bash

echo "========================================="
echo "   Services Status Check                 "
echo "========================================="
echo ""

# Check if docker-compose is running
if ! docker-compose ps &>/dev/null; then
    echo "❌ Docker Compose not running in this directory"
    echo "Run: docker-compose up -d"
    exit 1
fi

echo "📦 Container Status:"
docker-compose ps
echo ""

echo "1️⃣  MySQL:"
docker exec mysql_wsl mysqladmin ping -u root -proot 2>/dev/null && echo "✅ MySQL is running" || echo "❌ MySQL is not responding"
echo ""

echo "2️⃣  MySQL Databases:"
docker exec mysql_wsl mysql -u root -proot -e "SHOW DATABASES;" 2>/dev/null || echo "❌ Cannot list databases"
echo ""

echo "3️⃣  MySQL Binary Log (for Debezium):"
docker exec mysql_wsl mysql -u root -proot -e "SHOW VARIABLES LIKE 'log_bin';" 2>/dev/null || echo "❌ Cannot check binlog"
echo ""

echo "4️⃣  Redis:"
docker exec redis redis-cli ping 2>/dev/null && echo "✅ Redis is running" || echo "❌ Redis is not responding"
echo ""

echo "5️⃣  Kafka:"
if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
    echo "✅ Kafka is running"
    docker exec kafka kafka-cluster cluster-id --bootstrap-server localhost:9092 2>/dev/null
else
    echo "⏳ Kafka is starting... (this can take 30-60 seconds)"
fi
echo ""

echo "6️⃣  Kafka Connect:"
CONNECT_STATUS=$(curl -s http://localhost:8083/ 2>/dev/null)
if [ -n "$CONNECT_STATUS" ]; then
    echo "✅ Kafka Connect is running"
    echo "$CONNECT_STATUS" | jq -r '.version // empty' 2>/dev/null
else
    echo "⏳ Kafka Connect is starting..."
fi
echo ""

echo "7️⃣  Schema Registry:"
curl -s http://localhost:8081/ &>/dev/null && echo "✅ Schema Registry is running" || echo "⏳ Schema Registry is starting..."
echo ""

echo "========================================="
echo "   Access URLs                           "
echo "========================================="
echo "🌐 Kafka UI:     http://localhost:8080"
echo "🗄️  Adminer:      http://localhost:8082"
echo "🔌 Kafka Connect: http://localhost:8083"
echo "📋 Schema Reg:    http://localhost:8081"
echo "========================================="
