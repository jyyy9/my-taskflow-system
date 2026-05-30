#!/bin/bash

echo "Stopping all TaskFlow services..."

pkill -f "./build/gateway" 2>/dev/null && echo "Gateway stopped" || echo "Gateway not running"
pkill -f "./build/scheduler" 2>/dev/null && echo "Scheduler stopped" || echo "Scheduler not running"
pkill -f "./build/worker" 2>/dev/null && echo "Workers stopped" || echo "Workers not running"
pkill -f "./build/tracker" 2>/dev/null && echo "Tracker stopped" || echo "Tracker not running"
pkill -f "./build/stats" 2>/dev/null && echo "Stats stopped" || echo "Stats not running"

echo "All services stopped"
