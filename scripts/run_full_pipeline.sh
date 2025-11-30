#!/bin/bash
# Master script to run the full data pipeline
# Usage: ./run_full_pipeline.sh [options]
#
# Options:
#   --serving     Include AstraDB loading
#   --speed       Include speed layer streaming
#   --fail-fast   Stop on first failure

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup
source "$PROJECT_ROOT/setup.sh"

echo ""
echo "========================================="
echo "Cloud Provider Analytics"
echo "Full Pipeline Execution"
echo "========================================="
echo ""
echo "Start time: $(date)"
echo ""

# Build command from arguments
CMD="python3 -m src.orchestration.pipeline_orchestrator --full"

# Pass through all arguments
for arg in "$@"; do
    CMD="$CMD $arg"
done

echo "Command: $CMD"
echo ""

# Run the orchestrator
$CMD
EXIT_CODE=$?

echo ""
echo "========================================="
echo "Pipeline completed"
echo "End time: $(date)"
echo "Exit code: $EXIT_CODE"
echo "========================================="

exit $EXIT_CODE


