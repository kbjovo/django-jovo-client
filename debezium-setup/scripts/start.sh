#!/bin/bash

echo "========================================="
echo "   Starting All Services                 "
echo "========================================="
echo ""

# Start services
echo "Starting docker-compose services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to start (60 seconds)..."
sleep 60

echo ""
./scripts/check-services.sh
