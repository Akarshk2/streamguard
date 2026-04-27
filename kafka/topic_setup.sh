#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# StreamGuard — Kafka Topic Setup
# Run after docker-compose up -d and kafka container is healthy
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_CONTAINER="streamguard_kafka"
BOOTSTRAP="localhost:9092"

echo "======================================================"
echo "  StreamGuard — Creating Kafka Topics"
echo "======================================================"

create_topic() {
  TOPIC=$1
  PARTITIONS=$2
  RETENTION=$3
  docker exec $KAFKA_CONTAINER kafka-topics \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --bootstrap-server "$BOOTSTRAP" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION"
  echo "  ✓ Created: $TOPIC (partitions=$PARTITIONS)"
}

# Main event topics
create_topic "orders_topic"       3  86400000   # 1 day retention
create_topic "clickstream_topic"  6  43200000   # 12 hour retention
create_topic "inventory_topic"    2  86400000   # 1 day retention

# Dead Letter Queue
create_topic "streamguard_dlq"    3  604800000  # 7 day retention (for reprocessing)

echo ""
echo "All topics created. Listing:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --list --bootstrap-server "$BOOTSTRAP"

echo ""
echo "======================================================"
echo "  Done! Kafka UI: http://localhost:8090"
echo "======================================================"
