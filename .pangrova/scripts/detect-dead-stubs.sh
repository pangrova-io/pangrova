#!/usr/bin/env bash
# CI Dead-Stub Detection Script
#
# Scans production Java code for stub methods that:
#   1. Only log "not implemented" warnings
#   2. Only throw UnsupportedOperationException (bare or with generic message)
#
# Distinguishes TRUE STUBS from:
#   - Guard clauses: throw inside switch/if with descriptive message about unsupported input
#   - Read-only contracts: throw with "read-only" or "immutable" message (null-object pattern)
#
# Stubs are allowed if:
#   - They are in test code (src/test/)
#   - The containing class has @Experimental annotation
#   - The method/class has an explicit tracking issue reference (GitHub/Jira URL)
#   - They are in SPI default methods (interface default implementations are intentional contracts)
#
# Exit code: 0 = clean, 1 = violations found

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

VIOLATIONS=0
VIOLATION_DETAILS=""

# Patterns that indicate a stub method body (the method does nothing useful)
STUB_PATTERNS=(
    'throw new UnsupportedOperationException\(\)'
    'throw new UnsupportedOperationException\(""\)'
    'LOG\.warn\("not implemented'
    'LOG\.warn\("Not implemented'
    'log\.warn\("not implemented'
    'log\.warn\("Not implemented'
    'logger\.warn\("not implemented'
    'logger\.warn\("Not implemented'
)

# Patterns that indicate a guard clause or intentional contract (NOT a stub)
# These are legitimate runtime checks for unsupported input types/values
GUARD_CLAUSE_PATTERNS=(
    'Unsupported.*:'
    'Unsupported.*operator'
    'Unsupported.*expression'
    'Unsupported.*type'
    'Unknown.*type'
    'Unknown.*literal'
    'Expected.*got:'
    'is read-only'
    'is immutable'
    'not supported for'
    'does not support'
    "doesn.t support"
)

# Patterns that indicate an allowed exception (tracking issue or experimental marker)
ALLOW_PATTERNS=(
    '@Experimental'
    'https://github.com/'
    'https://issues\.'
    'JIRA-'
    'TODO.*issue'
    'TODO.*ticket'
    'tracking.issue'
)

# Directories to exclude from scanning (third-party code, build output)
EXCLUDE_DIRS=(
    "deps-download"
    "target"
    "generated-sources"
    ".git"
)

# Check if a file is in test code
is_test_code() {
    local file="$1"
    [[ "$file" == */src/test/* ]]
}

# Check if a file is an SPI interface (default methods are intentional contracts)
is_spi_interface() {
    local file="$1"
    [[ "$file" == *pangrova-spi/* ]]
}

# Check if a file is in an excluded directory
is_excluded_dir() {
    local file="$1"
    for dir in "${EXCLUDE_DIRS[@]}"; do
        if [[ "$file" == */"$dir"/* ]]; then
            return 0
        fi
    done
    return 1
}

# Check if the matched line is a guard clause (not a true stub)
is_guard_clause() {
    local line_content="$1"
    for pattern in "${GUARD_CLAUSE_PATTERNS[@]}"; do
        if echo "$line_content" | grep -qiE "$pattern"; then
            return 0
        fi
    done
    return 1
}

# Check if the file or its surrounding context has an allow pattern
has_allow_pattern() {
    local file="$1"
    local line_num="$2"

    # Check class-level annotations (first 50 lines of file)
    local header
    header=$(head -50 "$file" 2>/dev/null || true)
    for pattern in "${ALLOW_PATTERNS[@]}"; do
        if echo "$header" | grep -qE "$pattern"; then
            return 0
        fi
    done

    # Check surrounding context (5 lines before and after the stub line)
    local start=$((line_num - 5))
    if [ "$start" -lt 1 ]; then
        start=1
    fi
    local end=$((line_num + 5))
    local context
    context=$(sed -n "${start},${end}p" "$file" 2>/dev/null || true)
    for pattern in "${ALLOW_PATTERNS[@]}"; do
        if echo "$context" | grep -qE "$pattern"; then
            return 0
        fi
    done

    return 1
}

# Check if the throw is the only meaningful statement in the method body.
# A true stub method has throw as essentially its entire body.
# A guard clause is a throw inside a method that has other real logic.
is_stub_method() {
    local file="$1"
    local line_num="$2"

    # Look backwards from the throw to find surrounding context.
    # If there are many other statements nearby, this is a guard clause inside a real method.
    local start=$((line_num - 15))
    if [ "$start" -lt 1 ]; then
        start=1
    fi
    local end=$((line_num + 3))
    local context
    context=$(sed -n "${start},${end}p" "$file" 2>/dev/null || true)

    # Count non-trivial statements in the method context
    # (excluding braces, blank lines, comments, annotations, the throw itself)
    local other_statements
    other_statements=$(echo "$context" | grep -cE '^\s*(if |for |while |switch |return |var |final |[a-zA-Z].*=|[a-zA-Z].*\.[a-zA-Z]+\(.*\);)' 2>/dev/null || true)

    # If there are multiple other statements near the throw, it is likely a guard clause
    if [ "$other_statements" -gt 2 ]; then
        return 1
    fi
    return 0
}

# Git enumerates repository sources without traversing ignored build/dependency
# trees. Include untracked active sources locally; CI sees the tracked revision.
SOURCE_FILES=$(mktemp "${TMPDIR:-/tmp}/pangrova-stub-sources.XXXXXX")
trap 'rm -f "$SOURCE_FILES"' EXIT
git -C "$PROJECT_ROOT" ls-files --cached --others --exclude-standard -z -- \
    ':(glob)**/src/main/java/**/*.java' > "$SOURCE_FILES"
STUB_PATTERN=$(IFS='|'; printf '%s' "${STUB_PATTERNS[*]}")

echo "=========================================="
echo " CI Dead-Stub Detection"
echo "=========================================="
echo ""
echo "Scanning production code for stub methods..."
echo ""

# Find all production Java files (exclude test code, build output, generated sources, third-party)
while IFS= read -r -d '' java_file; do
    java_file="$PROJECT_ROOT/$java_file"
    # Skip test code
    if is_test_code "$java_file"; then
        continue
    fi

    # Skip SPI default methods (intentional contract behavior)
    if is_spi_interface "$java_file"; then
        continue
    fi

    # Skip excluded directories (deps-download, target, generated-sources)
    if is_excluded_dir "$java_file"; then
        continue
    fi

    # Match all patterns in one scan per production file.
    # Find matching lines with line numbers
    while IFS=: read -r line_num line_content; do
        [ -z "$line_num" ] && continue

        # Skip guard clauses (legitimate runtime checks for unsupported input)
        if is_guard_clause "$line_content"; then
            continue
        fi

        # Check if this is actually a stub method (not a guard inside a real method)
        if ! is_stub_method "$java_file" "$line_num"; then
            continue
        fi

        # Check if this stub is allowed via annotation or tracking issue
        if has_allow_pattern "$java_file" "$line_num"; then
            continue
        fi

        # This is a violation
        VIOLATIONS=$((VIOLATIONS + 1))
        relative_path="${java_file#"$PROJECT_ROOT"/}"
        VIOLATION_DETAILS="${VIOLATION_DETAILS}  ${RED}✗${NC} ${relative_path}:${line_num}\n"
        VIOLATION_DETAILS="${VIOLATION_DETAILS}    ${YELLOW}${line_content}${NC}\n\n"
    done < <(grep -nE "$STUB_PATTERN" "$java_file" 2>/dev/null || true)
done < "$SOURCE_FILES"

echo ""
echo "=========================================="
echo " Results"
echo "=========================================="
echo ""

if [ "$VIOLATIONS" -eq 0 ]; then
    echo -e "${GREEN}✓ No dead stubs found in production code.${NC}"
    echo ""
    echo "No unapproved stub patterns detected; reachability and runtime behavior require their own tests."
    exit 0
else
    echo -e "${RED}✗ Found ${VIOLATIONS} dead stub(s) in production code:${NC}"
    echo ""
    echo -e "$VIOLATION_DETAILS"
    echo "To fix violations, either:"
    echo "  1. Implement the method fully"
    echo "  2. Add @Experimental(trackingIssue = \"<url>\") to the class"
    echo "  3. Add a tracking issue URL comment near the stub"
    echo ""
    echo "Stubs are allowed in:"
    echo "  - Test code (src/test/)"
    echo "  - SPI interfaces (default method contracts)"
    echo "  - Classes annotated with @Experimental"
    echo "  - Methods with explicit tracking issue references"
    echo "  - Guard clauses for unsupported input types (not stubs)"
    exit 1
fi
