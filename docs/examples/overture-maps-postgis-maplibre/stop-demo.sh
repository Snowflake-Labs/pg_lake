#!/bin/bash

# Stop script for Site Selection Demo

echo "=========================================="
echo "🛑 Stopping Site Selection Demo"
echo "=========================================="
echo ""

# Stop backend
if [ -f "backend.pid" ]; then
    BACKEND_PID=$(cat backend.pid)
    if ps -p $BACKEND_PID > /dev/null 2>&1; then
        echo "Stopping backend (PID: $BACKEND_PID)..."
        kill $BACKEND_PID
        rm backend.pid
        echo "✓ Backend stopped"
    else
        echo "Backend process not running"
        rm backend.pid
    fi
else
    echo "No backend PID file found"
fi

echo ""
echo "=========================================="
echo "✅ Demo stopped"
echo "=========================================="

