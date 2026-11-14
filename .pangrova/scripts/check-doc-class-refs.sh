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
# Fenced blocks are excluded because architecture documents intentionally contain
# pseudocode, target interfaces, and third-party API examples; inline references
# are the assertions that readers are most likely to interpret as repository types.
# Pattern: starts with uppercase, contains at least one lowercase, length >= 4,
# no spaces, no dots (excludes package names), no parens (excludes method calls).
# Uses -E (extended regex) for portability across GNU and BSD grep.
extract_class_refs() {
    for doc in "$DOCS_DIR"/*.md; do
        if head -n 30 "$doc" | grep -Eiq 'Status.*(Draft|Target architecture)'; then
            continue
        fi
        awk '/^```/{in_fence = !in_fence; next} !in_fence{print}' "$doc"
    done \
        | grep -ivE 'proposed|planned|target design|remaining work|new (class|component)|phase [0-9]|does not exist|not implemented|已删除|删除|退役|移除|迁到|迁入|迁至|移至|归入|原 `|removed|retired|migrated' \
        | grep -ohE '`[A-Z][a-zA-Z0-9]{3,}`' 2>/dev/null \
        | sed 's/`//g' \
        | grep -E '[a-z]' \
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
        # JDK types and common third-party API types
        AtomicReference|CompletableFuture|ConcurrentHashMap|CopyOnWriteArrayList)
            return 0 ;;
        ScheduledExecutorService|Semaphore|ServiceLoader|Closeable|AutoCloseable|Exception)
            return 0 ;;
        IllegalStateException|UnsupportedOperationException)
            return 0 ;;
        IllegalArgumentException|WebApplicationException|ForbiddenException|FlightRuntimeException)
            return 0 ;;
        # Third-party observability API types, not Pangrova implementations
        Meter|Tracer)
            return 0 ;;
        RelNode|LogicalTableModify|LogicalValues|TableModify|TableScan|CalciteSchema)
            return 0 ;;
        AppendFiles|ExpireSnapshots|RewriteDataFiles|RewriteManifests)
            return 0 ;;
        DeleteOrphanFiles|RemoveOrphanFilesAction|ExpireSnapshotsAction)
            return 0 ;;
        CompactAction|OrphanFilesClean|OptimisticLockException)
            return 0 ;;
        ArrowArray|ArrowSchema|FixedSizeListArray|VectorSchemaRoot|RecordBatch|RowVector)
            return 0 ;;
        FlightTicket|CallStatus|Roaring64Map|PrestoVectorSerde|AsyncDataCache)
            return 0 ;;
        # Generic type roles that do not claim a Pangrova implementation
        Abstract|Cache|Codec|DataWriter|Deserializer|EvictionPolicy|Exchange)
            return 0 ;;
        FileSystem|Partitioner|Serializer|Ticket|Utils|VectorFunction)
            return 0 ;;
        FormatReader|ParquetFormatReader|OrcFormatReader|MemoryManager)
            return 0 ;;
        ShuffleReader|ShuffleWriter)
            return 0 ;;
        # Third-party and reference examples, not Pangrova repository types
        FlightProducer|RelCollation|AbstractCatalog|Supplier)
            return 0 ;;
        *)
            return 1 ;;
    esac
}

# Build the code symbol index once. The previous implementation recursively
# scanned the repository for every referenced name, including large generated
# and dependency directories, which made this gate impractical on real docs.
build_symbol_index() {
    {
        rg --no-filename --only-matching \
            '(class|struct|interface|enum|record|message)[[:space:]]+[A-Z][A-Za-z0-9]*' \
            "$REPO_ROOT" \
            --glob '*.java' --glob '*.proto' --glob '*.h' --glob '*.hpp' \
            --glob '*.cc' --glob '*.cpp' \
            --glob '!**/target/**' --glob '!website/node_modules/**' \
            --glob '!website/build/**' --glob '!website/.docusaurus/**' \
            --glob '!.git/**' 2>/dev/null \
            | awk '{print $2}'
        rg --files "$REPO_ROOT" \
            -g '*.java' -g '*.proto' -g '*.h' -g '*.hpp' -g '*.cc' -g '*.cpp' \
            -g '!**/target/**' -g '!website/node_modules/**' -g '!website/build/**' \
            -g '!website/.docusaurus/**' -g '!.git/**' \
            | awk -F/ '{name=$NF; sub(/\.[^.]+$/, "", name); print name}'
    } | sort -u
}

class_exists() {
    local class_name="$1"
    grep -Fqx "$class_name" <<< "$CODE_SYMBOLS"
}

echo "=== Documentation-Code Class Reference Check ==="
echo ""

MISSING=()
CHECKED=0

CLASS_REFS=$(extract_class_refs)
CODE_SYMBOLS=$(build_symbol_index)

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
