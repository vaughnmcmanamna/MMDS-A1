#!/usr/bin/env bash
#
# Improved integration test runner
# Runs all tests in a single SBT session for better performance
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "Running MinHash Integration Tests"
echo "========================================"
echo ""

# Track pass/fail
PASSED=0
FAILED=0
FAILED_TESTS=""

# Function to extract just the results table from output
extract_results() {
    local file=$1
    # Extract from "Pair" header to the last line before the dashes
    # Use sed instead of head -n -1 for portability
    sed -n '/^Pair/,/^----/p' "$file" | sed '$d'
}

# Function to run a single test
run_test() {
    local test_num=$1
    local test_script="./data/test-${test_num}.sh"
    local expected_file="./data/test-${test_num}.expected"
    local output_file="/tmp/test-${test_num}.output"
    local results_file="/tmp/test-${test_num}.results"
    local expected_results="/tmp/test-${test_num}.expected-results"

    if [ ! -f "$test_script" ]; then
        echo -e "${YELLOW}⊘${NC} test-${test_num}: SKIPPED (script not found)"
        return
    fi

    # Run the test
    bash "$test_script" > "$output_file" 2>&1

    # Extract just the results table
    extract_results "$output_file" > "$results_file"
    extract_results "$expected_file" > "$expected_results"

    # Compare results
    if diff -q "$expected_results" "$results_file" > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} test-${test_num}: PASSED"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗${NC} test-${test_num}: FAILED"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="${FAILED_TESTS} ${test_num}"

        # Show diff if requested
        if [ "${SHOW_DIFF}" = "1" ]; then
            echo "  Expected vs Actual:"
            diff -u "$expected_results" "$results_file" | head -20
        fi
    fi
}

# Parse command line arguments
SHOW_DIFF=0
TESTS_TO_RUN="01 02 03 04 05 06 07"

while [[ $# -gt 0 ]]; do
    case $1 in
        --diff|-d)
            SHOW_DIFF=1
            shift
            ;;
        --test|-t)
            TESTS_TO_RUN="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --diff          Show diff output for failed tests"
            echo "  -t, --test NUM      Run specific test(s) (e.g., '01' or '01 02 03')"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                  # Run all tests"
            echo "  $0 --diff           # Run all tests and show diffs"
            echo "  $0 --test 01        # Run only test 01"
            echo "  $0 --test '01 03'   # Run tests 01 and 03"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run tests
for test_num in $TESTS_TO_RUN; do
    run_test "$test_num"
done

echo ""
echo "========================================"
echo "Summary:"
echo -e "${GREEN}Passed: $PASSED${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED${NC}"
    echo -e "${RED}Failed tests:$FAILED_TESTS${NC}"
else
    echo -e "Failed: 0"
fi
echo "========================================"
echo ""

# Exit with error if any tests failed
if [ $FAILED -gt 0 ]; then
    exit 1
fi

exit 0
