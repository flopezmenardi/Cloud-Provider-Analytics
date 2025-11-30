#!/bin/bash
# Script to run the test suite

set -e

# Get the project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Source environment setup
source "$PROJECT_ROOT/setup.sh"

echo "========================================="
echo "Cloud Provider Analytics - Test Suite"
echo "========================================="
echo ""

# Parse arguments
TEST_TYPE="${1:-all}"
VERBOSE="${2:-}"

echo "Test type: $TEST_TYPE"
echo ""

# Build pytest command
PYTEST_ARGS="-v"

if [ "$VERBOSE" == "-v" ] || [ "$VERBOSE" == "--verbose" ]; then
    PYTEST_ARGS="-v -s"
fi

case $TEST_TYPE in
    "unit")
        echo "Running unit tests only..."
        python3 -m pytest $PYTEST_ARGS tests/test_data_quality.py
        ;;
    "transform")
        echo "Running transformation tests..."
        python3 -m pytest $PYTEST_ARGS tests/test_transformations.py
        ;;
    "integration")
        echo "Running integration tests..."
        python3 -m pytest $PYTEST_ARGS tests/test_integration.py
        ;;
    "all")
        echo "Running all tests..."
        python3 -m pytest $PYTEST_ARGS tests/
        ;;
    *)
        echo "Unknown test type: $TEST_TYPE"
        echo "Usage: $0 [unit|transform|integration|all] [-v|--verbose]"
        exit 1
        ;;
esac

EXIT_CODE=$?

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "All tests passed!"
else
    echo "Some tests failed. Exit code: $EXIT_CODE"
fi
echo "========================================="

exit $EXIT_CODE


