#!/bin/bash
# Script to run batch ingestion for all CSV sources

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

echo "========================================="
echo "Batch Ingestion - Bronze Layer"
echo "========================================="
echo ""

# Run batch ingestion for all sources
python3 -m src.batch.ingest_all_batch

echo ""
echo "========================================="
echo "Batch ingestion completed successfully"
echo "========================================="

