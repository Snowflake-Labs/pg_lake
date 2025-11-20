#!/bin/bash

# Setup script for Site Selection Demo database
# This script initializes the database schema and loads seed data

set -e

# Get database URL from environment variable
DB_URL="${DATABASE_URL}"

if [ -z "$DB_URL" ]; then
    echo "❌ ERROR: DATABASE_URL environment variable is not set"
    echo ""
    echo "Please set DATABASE_URL to your PostgreSQL connection string:"
    echo "  export DATABASE_URL='postgresql://user:password@host:5432/database'"
    echo ""
    echo "Or copy .env.example to .env and configure it, then run:"
    echo "  source .env"
    exit 1
fi

echo "=========================================="
echo "Site Selection Demo - Database Setup"
echo "=========================================="
echo ""

# Check if psql is installed
if ! command -v psql &> /dev/null; then
    echo "❌ psql not found. Please install PostgreSQL client."
    echo "   macOS: brew install postgresql"
    echo "   Ubuntu: sudo apt-get install postgresql-client"
    exit 1
fi

echo "✓ psql found"
echo ""

# Test database connection
echo "Testing database connection..."
if psql "$DB_URL" -c "SELECT version();" > /dev/null 2>&1; then
    echo "✓ Database connection successful"
else
    echo "❌ Cannot connect to database"
    exit 1
fi
echo ""

# Check for pg_lake extension
echo "Checking for pg_lake extension..."
if psql "$DB_URL" -tAc "SELECT 1 FROM pg_extension WHERE extname = 'pg_lake';" | grep -q 1; then
    echo "✓ pg_lake extension is installed"
else
    echo "⚠️  pg_lake extension not found"
    echo "   The database might not have pg_lake installed yet."
    echo "   Some features may not work until pg_lake is available."
fi
echo ""

# Run initialization script
echo "Running database initialization..."
psql "$DB_URL" -f database/init.sql
if [ $? -eq 0 ]; then
    echo "✓ Database schema created"
else
    echo "❌ Failed to create database schema"
    exit 1
fi
echo ""

# Load seed data
echo "Loading seed data..."
psql "$DB_URL" -f database/seed-stores.sql
if [ $? -eq 0 ]; then
    echo "✓ Seed data loaded"
else
    echo "❌ Failed to load seed data"
    exit 1
fi
echo ""

# Summary
echo "=========================================="
echo "✅ Database setup complete!"
echo "=========================================="
echo ""
echo "Quick test queries:"
echo ""
echo "1. List all stores:"
echo "   psql \"$DB_URL\" -c \"SELECT name, city, state FROM local.stores;\""
echo ""
echo "2. Run site analysis (San Francisco):"
echo "   psql \"$DB_URL\" -c \"SELECT * FROM local.analyze_site(37.7749, -122.4194, 1.0);\""
echo ""
echo "3. Find nearby competitors:"
echo "   psql \"$DB_URL\" -c \"SELECT name, category, distance_miles FROM local.get_nearby_competitors(37.7749, -122.4194, 1.0);\""
echo ""
echo "=========================================="

