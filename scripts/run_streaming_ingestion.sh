#!/bin/bash
# Script to run streaming ingestion for usage events

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup (sets JAVA_HOME to Java 8/11 if available)
source "$SCRIPT_DIR/setup_env.sh" 2>/dev/null || {
    # Fallback: try to set Java 8
    JAVA_8_HOME=$(/usr/libexec/java_home -v 1.8 2>/dev/null || echo "")
    if [ -n "$JAVA_8_HOME" ]; then
        export JAVA_HOME="$JAVA_8_HOME"
        export PATH="$JAVA_HOME/bin:$PATH"
    fi
}

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

