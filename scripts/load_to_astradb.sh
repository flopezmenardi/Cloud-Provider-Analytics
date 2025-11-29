#!/bin/bash
#
# Load Gold Layer data into AstraDB Serving Layer
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "Load Gold → AstraDB"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 17)
source "$PROJECT_ROOT/setup.sh"

# Run data loader
echo "Loading Gold marts to AstraDB..."
python -m src.serving.load_to_astradb

echo ""
echo "✓ Data loading completed!"
