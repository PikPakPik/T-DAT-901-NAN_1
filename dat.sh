#!/bin/bash

echo "Starting services..."
docker compose up -d --wait --force-recreate --build --scale stream-processor=3

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Ensuring topic 'trade' has 3 partitions..."
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic trade --partitions 3 2>/dev/null || \
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic trade --partitions 3 --replication-factor 1 2>/dev/null || \
echo "⚠️  Could not create/alter topic (Kafka might still be starting)"

echo "Tailing stream-processor logs... (Ctrl+C to stop)"
docker compose logs -f stream-processor | cat