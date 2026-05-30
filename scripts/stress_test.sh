#!/bin/bash

GATEWAY_URL="http://localhost:8080"
TASK_COUNT=${1:-1000}
CONCURRENT=${2:-10}

echo "=== TaskFlow Stress Test ==="
echo "Tasks: $TASK_COUNT"
echo "Concurrent: $CONCURRENT"
echo ""

START_TIME=$(date +%s)
SUCCESS_COUNT=0
FAIL_COUNT=0
TASK_IDS=()

echo "Submitting $TASK_COUNT tasks..."

for i in $(seq 1 $TASK_COUNT); do
    PRIORITY=$((i % 3))
    TYPE="video"
    if [ $((i % 3)) -eq 1 ]; then
        TYPE="image"
    elif [ $((i % 3)) -eq 2 ]; then
        TYPE="data"
    fi
    
    RESPONSE=$(curl -s -X POST "$GATEWAY_URL/task" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"$TYPE\",\"priority\":$PRIORITY,\"data\":\"stress_task_$i\"}")
    
    if echo "$RESPONSE" | grep -q '"success":true'; then
        ((SUCCESS_COUNT++))
        TASK_ID=$(echo "$RESPONSE" | jq -r '.task_id')
        TASK_IDS+=("$TASK_ID")
    else
        ((FAIL_COUNT++))
    fi
    
    if [ $((i % 100)) -eq 0 ]; then
        echo "Progress: $i / $TASK_COUNT"
    fi
    
    if [ $((i % CONCURRENT)) -eq 0 ]; then
        sleep 0.1
    fi
done

echo ""
echo "Submission complete!"
echo "Success: $SUCCESS_COUNT"
echo "Failed: $FAIL_COUNT"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "Waiting for tasks to complete..."
sleep 30

echo ""
echo "=== Stress Test Results ==="
echo "Total tasks: $TASK_COUNT"
echo "Submitted successfully: $SUCCESS_COUNT"
echo "Failed to submit: $FAIL_COUNT"
echo "Time elapsed: ${ELAPSED}s"
echo "Avg response time: $((ELAPSED * 1000 / TASK_COUNT))ms"

echo ""
echo "Worker distribution:"
curl -s -X GET "$GATEWAY_URL/workers" | jq .

echo ""
echo "Stress test complete!"
