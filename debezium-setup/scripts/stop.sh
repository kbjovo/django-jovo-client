#!/bin/bash

echo "Stopping all services..."
docker-compose down

echo ""
echo "✅ All services stopped"
echo ""
echo "Data is preserved in Docker volumes:"
docker volume ls | grep debezium
