#!/bin/bash

echo "Starting services..."
docker compose up -d --wait --force-recreate --build
echo "Tailing stream-processor logs... (Ctrl+C to stop)"
docker compose logs -f stream-processor | cat