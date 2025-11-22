#!/bin/bash
# CDC Replication Diagnostic Script
# Usage: ./debug_cdc.sh [connector_name]

set -e

CONNECTOR_NAME=${1:-""}
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CDC REPLICATION DIAGNOSTICS${NC}"
echo -e "${BLUE}========================================${NC}\n"

# 1. Check Container Health
echo -e "${YELLOW}1. CONTAINER HEALTH STATUS${NC}"
echo "----------------------------------------"
docker compose ps
echo ""

# 2. Check Kafka Connect
echo -e "${YELLOW}2. KAFKA CONNECT STATUS${NC}"
echo "----------------------------------------"
if curl -s http://localhost:8083/ > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Kafka Connect is running${NC}"
    
    # List connectors
    echo -e "\n${BLUE}Available Connectors:${NC}"
    CONNECTORS=$(curl -s http://localhost:8083/connectors)
    echo "$CONNECTORS" | jq -r '.[]' 2>/dev/null || echo "$CONNECTORS"
    
    # If connector name provided, check its status
    if [ -n "$CONNECTOR_NAME" ]; then
        echo -e "\n${BLUE}Connector Status: $CONNECTOR_NAME${NC}"
        curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/status" | jq '.' 2>/dev/null || echo "Connector not found"
        
        echo -e "\n${BLUE}Connector Config: $CONNECTOR_NAME${NC}"
        curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME" | jq '.config' 2>/dev/null || echo "Connector not found"
        
        echo -e "\n${BLUE}Connector Tasks: $CONNECTOR_NAME${NC}"
        curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/tasks" | jq '.' 2>/dev/null || echo "Connector not found"
    fi
else
    echo -e "${RED}✗ Kafka Connect is NOT running${NC}"
fi
echo ""

# 3. Check MySQL Binlog
echo -e "${YELLOW}3. MYSQL BINARY LOG STATUS${NC}"
echo "----------------------------------------"
if docker exec mysql_wsl mysql -uroot -proot -e "SHOW VARIABLES LIKE 'log_bin';" 2>/dev/null | grep -q "ON"; then
    echo -e "${GREEN}✓ Binary logging is enabled${NC}"
    echo -e "\n${BLUE}Binary Logs:${NC}"
    docker exec mysql_wsl mysql -uroot -proot -e "SHOW BINARY LOGS;" 2>/dev/null
    echo -e "\n${BLUE}Master Status:${NC}"
    docker exec mysql_wsl mysql -uroot -proot -e "SHOW MASTER STATUS;" 2>/dev/null
else
    echo -e "${RED}✗ Binary logging is NOT enabled${NC}"
fi
echo ""

# 4. Check Kafka Topics
echo -e "${YELLOW}4. KAFKA TOPICS${NC}"
echo "----------------------------------------"
TOPICS=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Kafka is accessible${NC}"
    echo -e "\n${BLUE}Available Topics:${NC}"
    echo "$TOPICS" | grep -v "^__" | head -20  # Skip internal topics, show first 20
    TOPIC_COUNT=$(echo "$TOPICS" | grep -v "^__" | wc -l)
    echo -e "\n${BLUE}Total non-internal topics: $TOPIC_COUNT${NC}"
    
    # If connector name provided, show related topics
    if [ -n "$CONNECTOR_NAME" ]; then
        echo -e "\n${BLUE}Topics related to $CONNECTOR_NAME:${NC}"
        echo "$TOPICS" | grep -i "$CONNECTOR_NAME" || echo "No topics found"
    fi
else
    echo -e "${RED}✗ Cannot access Kafka${NC}"
fi
echo ""

# 5. Check Consumer Groups
echo -e "${YELLOW}5. KAFKA CONSUMER GROUPS${NC}"
echo "----------------------------------------"
GROUPS=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Found consumer groups${NC}"
    echo "$GROUPS" | head -10
    
    # Show lag for first non-empty group
    FIRST_GROUP=$(echo "$GROUPS" | head -1)
    if [ -n "$FIRST_GROUP" ]; then
        echo -e "\n${BLUE}Consumer Group Details: $FIRST_GROUP${NC}"
        docker exec kafka kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --group "$FIRST_GROUP" \
            --describe 2>/dev/null | head -20
    fi
else
    echo -e "${RED}✗ Cannot access consumer groups${NC}"
fi
echo ""

# 6. Check Schema Registry
echo -e "${YELLOW}6. SCHEMA REGISTRY${NC}"
echo "----------------------------------------"
if curl -s http://localhost:8081/ > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Schema Registry is running${NC}"
    echo -e "\n${BLUE}Registered Schemas:${NC}"
    curl -s http://localhost:8081/subjects | jq -r '.[]' 2>/dev/null | head -20 || echo "No schemas registered"
else
    echo -e "${RED}✗ Schema Registry is NOT running${NC}"
fi
echo ""

# 7. Recent Errors
echo -e "${YELLOW}7. RECENT ERRORS (Last 5 minutes)${NC}"
echo "----------------------------------------"
echo -e "${BLUE}Kafka Connect Errors:${NC}"
docker compose logs --since 5m kafka-connect 2>/dev/null | grep -i "error\|exception\|failed" | tail -10 || echo "No recent errors"

echo -e "\n${BLUE}Celery Consumer Errors:${NC}"
docker compose logs --since 5m celery-consumer 2>/dev/null | grep -i "error\|exception\|failed" | tail -10 || echo "No recent errors"

echo -e "\n${BLUE}MySQL Errors:${NC}"
docker compose logs --since 5m mysql 2>/dev/null | grep -i "error" | tail -5 || echo "No recent errors"
echo ""

# 8. Quick Health Summary
echo -e "${YELLOW}8. HEALTH SUMMARY${NC}"
echo "----------------------------------------"

check_health() {
    local service=$1
    local port=$2
    if docker compose ps | grep -q "$service.*Up"; then
        if curl -s "http://localhost:$port" > /dev/null 2>&1 || nc -z localhost $port 2>/dev/null; then
            echo -e "${GREEN}✓ $service: Healthy${NC}"
        else
            echo -e "${YELLOW}⚠ $service: Running but not responding${NC}"
        fi
    else
        echo -e "${RED}✗ $service: Not running${NC}"
    fi
}

check_health "kafka-connect" "8083"
check_health "kafka" "9092"
check_health "schema-registry" "8081"
check_health "mysql" "3306"
check_health "redis" "6379"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}DIAGNOSTIC COMPLETE${NC}"
echo -e "${BLUE}========================================${NC}"

# Usage tips
echo -e "\n${YELLOW}💡 Useful Commands:${NC}"
echo "  • Watch Kafka Connect logs: docker compose logs -f kafka-connect"
echo "  • Watch Consumer logs: docker compose logs -f celery-consumer"
echo "  • Check specific connector: ./debug_cdc.sh YOUR_CONNECTOR_NAME"
echo "  • Create connector: curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @connector.json"
echo "  • Delete connector: curl -X DELETE http://localhost:8083/connectors/YOUR_CONNECTOR_NAME"
echo "  • Restart connector: curl -X POST http://localhost:8083/connectors/YOUR_CONNECTOR_NAME/restart"