#!/bin/bash
# ============================================
# Docker Startup Script
# ============================================
# This script helps you get started with the Dockerized environment

set -e

echo "========================================"
echo "Django CDC Replication - Docker Setup"
echo "========================================"
echo ""

# Check if .env.docker exists
if [ ! -f ".env.docker" ]; then
    echo "⚠️  .env.docker not found!"
    echo "Creating .env.docker from template..."
    cp .env.docker .env.docker
    echo "✅ Created .env.docker"
    echo ""
    echo "📝 Please review and update .env.docker with your configuration"
    echo "   Then run this script again."
    exit 1
fi

# Navigate to debezium-setup directory
cd debezium-setup

echo "🔨 Building Docker images..."
docker compose build

echo ""
echo "🚀 Starting services..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

echo ""
echo "🔍 Checking service health..."
docker compose ps

echo ""
echo "✅ All services started!"
echo ""
echo "========================================"
echo "Access Points:"
echo "========================================"
echo "Django Admin:      http://localhost:8000/admin"
echo "Kafka UI:          http://localhost:8080"
echo "Schema Registry:   http://localhost:8081"
echo "Kafka Connect:     http://localhost:8083"
echo "Adminer (DB):      http://localhost:8082"
echo ""
echo "========================================"
echo "Useful Commands:"
echo "========================================"
echo "View logs:         docker compose logs -f [service-name]"
echo "Stop all:          docker compose down"
echo "Restart service:   docker compose restart [service-name]"
echo "Shell access:      docker compose exec django bash"
echo "Run migrations:    docker compose exec django python manage.py migrate"
echo ""
echo "Services: django, celery-worker, celery-consumer, celery-beat"
echo "          mysql, redis, kafka, kafka-connect, schema-registry"
echo "========================================"
