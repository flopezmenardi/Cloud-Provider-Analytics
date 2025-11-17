#!/bin/bash
# Quick activation script - source this in your terminal or add to your shell profile
# This sets up Java 8 for Spark compatibility

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Source the setup script
source "$SCRIPT_DIR/scripts/setup_env.sh"

echo ""
echo "✓ Environment activated! Java 8 is now active for Spark."
echo ""

