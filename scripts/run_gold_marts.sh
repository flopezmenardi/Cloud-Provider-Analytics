#!/bin/bash
#
# Execute all Gold layer mart creations
#

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================="
echo "Gold Layer Mart Creation"
echo "================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Source environment setup (activates venv and sets Java 17)
source "$PROJECT_ROOT/setup.sh"

# Run Gold mart creation
echo "Creating all Gold marts..."
python -m src.gold.create_all_marts

echo ""
echo "✓ Gold layer marts completed!"
