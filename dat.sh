#!/bin/bash

echo "Starting services..."
docker-compose up --scale spark-worker=3 --force-recreate --build
