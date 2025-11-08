#!/bin/bash
# Manual test script for RealtyBot-Bali
# This script runs the bot manually for testing without cron

echo "================================"
echo "RealtyBot-Bali Manual Test Run"
echo "================================"

# Run the bot inside the Docker container
docker-compose exec bot python3 /app/src/main.py

echo ""
echo "================================"
echo "Test run completed"
echo "================================"
