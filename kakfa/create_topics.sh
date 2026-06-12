#!/bin/bash

BOOTSTRAP_SERVER="localhost:9092"
TOPIC="wikimedia"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "Creating Kafka topic: $TOPIC"

kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic $TOPIC \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR \
  --if-not-exists

echo "Done. Current topics:"
kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER