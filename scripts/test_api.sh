#!/bin/bash

GATEWAY_URL="http://localhost:8080"

echo "=== TaskFlow API Test Script ==="
echo ""

echo "1. Testing Health Check..."
curl -s -X GET "$GATEWAY_URL/health" | jq .
echo ""

echo "2. Submitting a HIGH priority video task..."
RESPONSE=$(curl -s -X POST "$GATEWAY_URL/task" \
    -H "Content-Type: application/json" \
    -d '{"type":"video","priority":2,"data":"test_video.mp4"}')
echo "$RESPONSE" | jq .
TASK_ID=$(echo "$RESPONSE" | jq -r '.task_id')
echo "Task ID: $TASK_ID"
echo ""

sleep 2

echo "3. Checking task status..."
curl -s -X GET "$GATEWAY_URL/task?id=$TASK_ID" | jq .
echo ""

echo "4. Submitting multiple tasks (20 tasks)..."
for i in $(seq 1 20); do
    PRIORITY=$((i % 3))
    TYPE="video"
    if [ $((i % 3)) -eq 1 ]; then
        TYPE="image"
    elif [ $((i % 3)) -eq 2 ]; then
        TYPE="data"
    fi
    
    curl -s -X POST "$GATEWAY_URL/task" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"$TYPE\",\"priority\":$PRIORITY,\"data\":\"task_$i\"}" > /dev/null
    echo -n "."
done
echo ""
echo "20 tasks submitted"
echo ""

sleep 3

echo "5. Getting worker status..."
curl -s -X GET "$GATEWAY_URL/workers" | jq .
echo ""

echo "6. Waiting for tasks to complete..."
sleep 10

echo "7. Checking final task status..."
curl -s -X GET "$GATEWAY_URL/task?id=$TASK_ID" | jq .
echo ""

echo "=== API Test Complete ==="
