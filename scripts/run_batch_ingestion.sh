#!/bin/bash
# Script to run batch ingestion for all CSV sources

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup (activates venv and sets Java 11)
source "$PROJECT_ROOT/setup.sh"

echo "========================================="
echo "Batch Ingestion - Bronze Layer"
echo "========================================="
echo ""

# Run batch ingestion for all sources
python3 -m src.ingestion.batch.ingest_all_batch

echo ""
echo "========================================="
echo "Batch ingestion completed successfully"
echo "========================================="

