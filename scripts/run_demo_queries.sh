#!/bin/bash
#
# Run demo CQL queries on Cassandra Serving Layer
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "Demo CQL Queries"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 17)
source "$PROJECT_ROOT/setup.sh"

# Run demo queries
echo "Executing demo queries..."
python -m src.serving.demo_queries

echo ""
echo "✓ Demo queries completed!"
