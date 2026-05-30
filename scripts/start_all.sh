#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "Starting etcd and Redis if not running..."

sudo systemctl start etcd 2>/dev/null || echo "etcd service not available, trying to start manually..."
sudo systemctl start redis-server 2>/dev/null || sudo redis-server --daemonize yes 2>/dev/null || echo "Redis may already be running"

sleep 2

echo "Building project if needed..."
if [ ! -d "build" ]; then
    mkdir -p build
    cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j$(nproc)
    cd ..
fi

echo "Starting all TaskFlow services..."

if [ -f "build/gateway" ]; then
    ./build/gateway > logs/gateway.log 2>&1 &
    echo "Gateway started (PID: $!)"
fi

if [ -f "build/scheduler" ]; then
    ./build/scheduler > logs/scheduler.log 2>&1 &
    echo "Scheduler started (PID: $!)"
fi

if [ -f "build/worker" ]; then
    ./build/worker --id=1 > logs/worker-1.log 2>&1 &
    echo "Worker-1 started (PID: $!)"
    
    ./build/worker --id=2 > logs/worker-2.log 2>&1 &
    echo "Worker-2 started (PID: $!)"
    
    ./build/worker --id=3 > logs/worker-3.log 2>&1 &
    echo "Worker-3 started (PID: $!)"
fi

if [ -f "build/tracker" ]; then
    ./build/tracker > logs/tracker.log 2>&1 &
    echo "Tracker started (PID: $!)"
fi

if [ -f "build/stats" ]; then
    ./build/stats > logs/stats.log 2>&1 &
    echo "Stats started (PID: $!)"
fi

sleep 2

echo ""
echo "All services started successfully!"
echo ""
echo "Service endpoints:"
echo "  Gateway HTTP:  http://localhost:8080"
echo "  Scheduler:     localhost:50051"
echo "  Workers:       localhost:50052, 50053, 50054"
echo "  Tracker:       localhost:50055"
echo "  Stats:         localhost:50056"
echo ""
echo "Check logs in ./logs/ directory"
