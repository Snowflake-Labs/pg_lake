#!/bin/bash

# Quick start script for Site Selection Demo

set -e

echo "=========================================="
echo "🎯 Site Selection Demo - Quick Start"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js 18+"
    exit 1
fi
echo "✓ Node.js found: $(node --version)"

if ! command -v npm &> /dev/null; then
    echo "❌ npm not found"
    exit 1
fi
echo "✓ npm found: $(npm --version)"

if ! command -v psql &> /dev/null; then
    echo "⚠️  psql not found. Database setup will be skipped."
    echo "   Install PostgreSQL client: brew install postgresql"
    SKIP_DB=true
else
    echo "✓ psql found"
    SKIP_DB=false
fi

echo ""

# Database setup
if [ "$SKIP_DB" = false ]; then
    echo "=========================================="
    echo "📊 Setting up database..."
    echo "=========================================="
    
    if [ ! -f ".db-initialized" ]; then
        ./setup-database.sh
        touch .db-initialized
        echo "✓ Database initialized"
    else
        echo "✓ Database already initialized (delete .db-initialized to reinit)"
    fi
    echo ""
fi

# Install and start backend
echo "=========================================="
echo "🔧 Starting backend API..."
echo "=========================================="
cd backend
if [ ! -d "node_modules" ]; then
    echo "Installing backend dependencies..."
    npm install
fi
echo "✓ Backend ready"

# Start backend in background
echo "Starting backend server..."
npm start > ../backend.log 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > ../backend.pid
echo "✓ Backend running on http://localhost:3001 (PID: $BACKEND_PID)"
cd ..
echo ""

# Install and start frontend
echo "=========================================="
echo "🎨 Starting frontend..."
echo "=========================================="
cd frontend
if [ ! -d "node_modules" ]; then
    echo "Installing frontend dependencies..."
    npm install
fi
echo "✓ Frontend ready"
cd ..
echo ""

# Wait for backend to be ready
echo "Waiting for backend to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:3001/health > /dev/null 2>&1; then
        echo "✓ Backend is healthy"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "⚠️  Backend health check timeout"
    fi
    sleep 1
done

echo ""
echo "=========================================="
echo "✅ Demo is ready!"
echo "=========================================="
echo ""
echo "📍 Frontend:        http://localhost:5173"
echo "🔧 Backend API:     http://localhost:3001"
echo ""
echo "To start the frontend (in a new terminal):"
echo "  cd frontend && npm run dev"
echo ""
echo "To stop everything:"
echo "  ./stop-demo.sh"
echo ""
echo "=========================================="

