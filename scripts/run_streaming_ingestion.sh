#!/bin/bash
# Script to run streaming ingestion for usage events

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup (activates venv and sets Java 11)
source "$PROJECT_ROOT/setup.sh"

MODE=${1:-streaming}  # Default to streaming, can pass "batch" for batch mode

echo "========================================="
echo "Streaming Ingestion - Usage Events"
echo "Mode: $MODE"
echo "========================================="
echo ""

if [ "$MODE" == "batch" ]; then
    echo "Running in batch mode (one-time load)..."
    python3 -m src.streaming.ingest_usage_events_stream batch
else
    echo "Running in streaming mode (continuous)..."
    echo "Press Ctrl+C to stop"
    python3 -m src.streaming.ingest_usage_events_stream
fi

echo ""
echo "========================================="
echo "Streaming ingestion completed"
echo "========================================="

