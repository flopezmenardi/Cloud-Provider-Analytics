#!/bin/bash
#
# Setup Cassandra/AstraDB tables for Serving Layer
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "Cassandra/AstraDB Setup"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 17)
source "$PROJECT_ROOT/setup.sh"

# Run Cassandra setup
echo "Setting up Cassandra keyspace and tables..."
python -m src.serving.cassandra_setup

echo ""
echo "✓ Cassandra setup completed!"
