#!/bin/bash

# Setup script for Agentic Ledger
echo "🔧 Setting up Agentic Ledger..."

# Check Python version
python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [[ "$python_version" < "3.11" ]]; then
    echo "❌ Python 3.11+ required (found $python_version)"
    exit 1
fi
echo "✅ Python $python_version found"

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL not found. Please install PostgreSQL 16+"
    exit 1
fi
echo "✅ PostgreSQL found"

# Install UV if not present
if ! command -v uv &> /dev/null; then
    echo "📦 Installing UV package manager..."
    pip install uv
fi

# Create virtual environment and install dependencies
echo "📦 Creating virtual environment..."
uv venv
source .venv/bin/activate || source .venv/Scripts/activate
echo "✅ Virtual environment created"

echo "📦 Installing dependencies..."
uv sync
echo "✅ Dependencies installed"

# Create .env if not exists
if [ ! -f .env ]; then
    echo "📝 Creating .env from template..."
    cp .env.example .env
    echo "✅ .env created - please edit with your configuration"
fi

# Create database
echo "🗄️  Creating database (if not exists)..."
createdb agentic_ledger 2>/dev/null || echo "Database may already exist"

# Run migrations
echo "🗄️  Running migrations..."
psql -d agentic_ledger -f src/core/schema.sql

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env with your configuration"
echo "2. Run tests: pytest tests/ -v"
echo "3. Start developing: code ."
echo ""
echo "Happy coding! 🚀"