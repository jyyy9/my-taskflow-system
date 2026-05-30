#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <worker_id>"
    echo "Example: $0 4"
    exit 1
fi

WORKER_ID=$1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "Starting Worker-$WORKER_ID..."

if [ ! -f "build/worker" ]; then
    echo "Error: build/worker not found. Please build the project first."
    exit 1
fi

./build/worker --id=$WORKER_ID > logs/worker-$WORKER_ID.log 2>&1 &

PID=$!
echo "Worker-$WORKER_ID started (PID: $PID)"
echo "Logs: logs/worker-$WORKER_ID.log"
