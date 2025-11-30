#!/bin/bash
# Script to run Speed Layer streaming jobs

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup
source "$PROJECT_ROOT/setup.sh"

echo "========================================="
echo "Speed Layer - Streaming Processing"
echo "========================================="
echo ""

# Parse arguments
MODE="${1:-all}"
DURATION="${2:-}"

echo "Mode: $MODE"
if [ -n "$DURATION" ]; then
    echo "Duration: $DURATION seconds"
else
    echo "Duration: indefinite (Ctrl+C to stop)"
fi
echo ""

# Build command
CMD="python3 -m src.speed.streaming_aggregations --mode $MODE"
if [ -n "$DURATION" ]; then
    CMD="$CMD --duration $DURATION"
fi

echo "Running: $CMD"
echo ""
$CMD

echo ""
echo "========================================="
echo "Speed layer completed"
echo "========================================="


