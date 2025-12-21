#!/bin/bash
# =============================================================================
# check_max_lines.sh
# =============================================================================
# Enforces the max 500 lines per file policy for MatterStack Python source files.
#
# Usage:
#   ./scripts/check_max_lines.sh
#
# Exit codes:
#   0 - All files are within the line limit
#   1 - One or more files exceed the line limit
# =============================================================================

set -e

MAX_LINES=500
SOURCE_DIR="matterstack"

echo "=============================================="
echo "MatterStack Line Limit Enforcement Check"
echo "=============================================="
echo "Max allowed lines: $MAX_LINES"
echo "Scanning directory: $SOURCE_DIR"
echo ""

# Find all Python files and check their line counts
OFFENDERS=$(find "$SOURCE_DIR" -name "*.py" -type f -exec wc -l {} + | \
            awk -v max="$MAX_LINES" '$1 > max && !/total$/ {print $0}' | \
            sort -rn)

if [ -n "$OFFENDERS" ]; then
    echo "❌ FILES EXCEEDING $MAX_LINES LINES:"
    echo "----------------------------------------------"
    echo "$OFFENDERS"
    echo "----------------------------------------------"
    OFFENDER_COUNT=$(echo "$OFFENDERS" | wc -l)
    echo ""
    echo "Found $OFFENDER_COUNT file(s) exceeding the limit."
    echo ""
    echo "To fix:"
    echo "  1. Review the file structure"
    echo "  2. Extract cohesive modules or use mixins"
    echo "  3. See REFACTOR_MAP.md for examples"
    exit 1
else
    echo "✅ All files are within the $MAX_LINES line limit"
    echo ""
    
    # Show top 10 largest files for reference
    echo "Top 10 largest files:"
    echo "----------------------------------------------"
    find "$SOURCE_DIR" -name "*.py" -type f -exec wc -l {} + | \
        grep -v "total$" | \
        sort -rn | \
        head -10
    echo "----------------------------------------------"
    exit 0
fi
