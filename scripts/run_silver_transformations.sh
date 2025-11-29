#!/bin/bash
#
# Execute all Silver layer transformations
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "Silver Layer Transformations"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 11)
source "$PROJECT_ROOT/setup.sh"

# Run Silver transformations
echo "Running all Silver transformations..."
python -m src.silver.transform_all

echo ""
echo "✓ Silver layer transformations completed!"
