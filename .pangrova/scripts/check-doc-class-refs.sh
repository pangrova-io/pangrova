#!/usr/bin/env bash
# Verify class names referenced in docs/architecture/*.md actually exist in
# the codebase. Catches documentation drift where docs reference renamed or
# deleted classes.
#
# Heuristic: extracts backtick-quoted PascalCase identifiers from architecture
# docs and checks if a matching .java file or class declaration exists.
#
# Exit codes:
#   0 - all referenced classes found
#   1 - missing class references detected

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DOCS_DIR="${REPO_ROOT}/docs/architecture"

if [ ! -d "$DOCS_DIR" ]; then
    echo "⚠️  docs/architecture/ directory not found — skipping class reference check."
    exit 0
fi

# Extract backtick-quoted PascalCase identifiers that look like class names.
# Pattern: starts with uppercase, contains at least one lowercase, length >= 4,
# no spaces, no dots (excludes package names), no parens (excludes method calls).
# Uses -E (extended regex) for portability across GNU and BSD grep.
extract_class_refs() {
    grep -ohE '`[A-Z][a-zA-Z0-9]{3,}`' "$DOCS_DIR"/*.md 2>/dev/null \
        | sed 's/`//g' \
        | sort -u
}

# Known non-class identifiers to skip (keywords, acronyms, config names)
is_excluded() {
    local name="$1"
    case "$name" in
        # SQL keywords
        ANTLR|ANTLR4|VECTOR|FLOAT|DOUBLE|NULL|TRUE|FALSE|SELECT|INSERT|UPDATE|DELETE)
            return 0 ;;
        MERGE|CREATE|ALTER|DROP|TABLE|INDEX|WHERE|FROM|JOIN|LEFT|RIGHT|INNER|OUTER)
            return 0 ;;
        BRANCH|TAG|LIMIT|OFFSET|ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT)
            return 0 ;;
        # Log levels and enum-like constants
        INFO|DEBUG|WARN|ERROR|TRACE|SKIP|KEEP|UNKNOWN)
            return 0 ;;
        # Index types
        HNSW|BTREE|HASH|BITMAP|INVERTED|SERIAL|IDENTITY)
            return 0 ;;
        # Third-party product names
        PostgreSQL|MySQL|ZooKeeper|Kubernetes|Docker|Hadoop|Parquet|Kafka)
            return 0 ;;
        Calcite|Velox|Substrait|Arrow|Iceberg|Paimon|Netty|Jetty|Jersey)
            return 0 ;;
        TestContainers|JUnit|GoogleTest|CMake|Maven|Flyway|HikariCP)
            return 0 ;;
        Protobuf|SnakeYAML|Jackson|Guava|Caffeine|Jedis|Redis)
            return 0 ;;
        # Generic architecture terms (not class names)
        Coordinator|Worker|Client|Server|Cluster|Node|Partition|Replica)
            return 0 ;;
        Query|Plan|Task|Stage|Split|Batch|Page|Stream|Table|Column|Schema)
            return 0 ;;
        Leader|Follower|Primary|Secondary|Master|Slave)
            return 0 ;;
        *)
            return 1 ;;
    esac
}

# Check if a class name exists in the codebase
class_exists() {
    local class_name="$1"

    # Check for .java file with this name
    local java_file
    java_file=$(find "$REPO_ROOT" -name "${class_name}.java" \
        -not -path "*/target/*" 2>/dev/null | head -1)
    if [ -n "$java_file" ]; then
        return 0
    fi

    # Check for class/interface/enum/record declaration in source
    local decl
    decl=$(grep -rl "\b\(class\|interface\|enum\|record\) ${class_name}\b" \
        "$REPO_ROOT" --include="*.java" --exclude-dir=target 2>/dev/null | head -1)
    if [ -n "$decl" ]; then
        return 0
    fi

    # Check for .proto message with this name
    local proto_msg
    proto_msg=$(grep -rl "message ${class_name}" \
        "$REPO_ROOT" --include="*.proto" --exclude-dir=target 2>/dev/null | head -1)
    if [ -n "$proto_msg" ]; then
        return 0
    fi

    return 1
}

echo "=== Documentation-Code Class Reference Check ==="
echo ""

MISSING=()
CHECKED=0

CLASS_REFS=$(extract_class_refs)

if [ -z "$CLASS_REFS" ]; then
    echo "No class references found in docs/architecture/*.md"
    exit 0
fi

while IFS= read -r class_name; do
    if is_excluded "$class_name"; then
        continue
    fi

    CHECKED=$((CHECKED + 1))

    if ! class_exists "$class_name"; then
        MISSING+=("$class_name")
    fi
done <<< "$CLASS_REFS"

echo "Checked ${CHECKED} class references."
echo ""

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "❌ Class names referenced in docs but not found in codebase:"
    for m in "${MISSING[@]}"; do
        doc_file=$(grep -l "\`${m}\`" "$DOCS_DIR"/*.md 2>/dev/null | head -1 | xargs basename 2>/dev/null || echo "unknown")
        echo "  ✗ ${m} (referenced in ${doc_file})"
    done
    echo ""
    echo "Action: Update documentation to match current class names, or restore missing classes."
    exit 1
fi

echo "✅ All ${CHECKED} class references in docs/architecture/ exist in codebase."
exit 0
