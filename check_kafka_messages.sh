#!/bin/bash

echo "Checking Kafka topics for messages..."
echo "=========================================="

for topic in "client_1_db_2.kbe.tally_accounts" "client_1_db_2.kbe.tally_outstanding_mapping" "client_1_db_2.kbe.tally_purchase_return_detailed"; do
    echo ""
    echo "Topic: $topic"
    echo "----------------------------------------"

    # Get topic info
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka:29092 \
        --topic "$topic" \
        --time -1 2>/dev/null | while read line; do
        partition=$(echo $line | cut -d: -f2)
        offset=$(echo $line | cut -d: -f3)
        echo "  Partition $partition: $offset messages"
    done
done

echo ""
echo "=========================================="
echo "Checking consumer groups..."
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list 2>/dev/null | grep cdc_consumer