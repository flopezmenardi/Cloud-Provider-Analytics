#!/bin/bash
#
# Setup AstraDB collections for Serving Layer
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "AstraDB Setup"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 17)
source "$PROJECT_ROOT/setup.sh"

# Run AstraDB setup
echo "Setting up AstraDB collections..."
python -m src.serving.astradb_setup

echo ""
echo "✓ AstraDB setup completed!"
