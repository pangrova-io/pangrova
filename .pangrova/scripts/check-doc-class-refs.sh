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
DOCS_DIR="${PANGROVA_DOC_CLASS_REFS_DIR:-${REPO_ROOT}/docs/architecture}"

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
# A reference is skipped only when that occurrence is itself recorded as removed
# or migrated ("retired `X`", "removed the `X`", "`X` was removed", "| Removed ..."
# disposition cells). Other
# active types on the same line are still checked.
extract_class_refs() {
    python3 - "$DOCS_DIR" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
status = re.compile(r'Status.*(Draft|Target architecture)', re.I)
design = re.compile(
    r'proposed|planned|target design|remaining work|new (class|component)'
    r'|phase [0-9]|does not exist|not implemented', re.I)
reference = re.compile(r'`([A-Z][A-Za-z0-9]{3,})`')
removed_before = re.compile(r'\b(retired|removed|deleted|former|legacy|old)\s*$', re.I)
removed_verb_before = re.compile(
    r'\b(removed|deleted|retired|moved to|migrated to|merged into)\b[^,.;|]{0,12}$', re.I)
removed_after = re.compile(
    r'^\s*(was|is|were|are|has been|have been)\s+(retired|removed|deleted)\b', re.I)
disposition_verb = re.compile(r'^(removed|deleted|retired|moved|migrated|merged)\b', re.I)
sentence_stop = re.compile(r'[.;](?=\s|$)')


def cell_spans(line):
    spans = []
    start = 0
    for index, char in enumerate(line):
        if char == '|':
            spans.append((start, index))
            start = index + 1
    spans.append((start, len(line)))
    return spans


def sentence_bounds(line, start, end):
    stops = [match.end() for match in sentence_stop.finditer(line, 0, start)]
    following = sentence_stop.search(line, end)
    return (stops[-1] if stops else 0), (following.start() if following else len(line))


active = {}
for doc in sorted(root.glob('*.md')):
    text = doc.read_text(encoding='utf-8')
    if status.search('\n'.join(text.splitlines()[:30])):
        continue
    text = re.sub(r'^```.*?^```', '', text, flags=re.S | re.M)
    for line in text.splitlines():
        if design.search(line):
            continue
        table = line.lstrip().startswith('|')
        spans = cell_spans(line) if table else None
        if spans:
            spans = [span for span in spans if span[1] > span[0]]
        for match in reference.finditer(line):
            name = match.group(1)
            if not re.search('[a-z]', name):
                continue
            if table:
                cell_index, (cell_start, cell_end) = next(
                    (index, span) for index, span in enumerate(spans)
                    if span[0] <= match.start() <= span[1])
                cell = line[cell_start:cell_end]
                local_start = match.start() - cell_start
                local_end = match.end() - cell_start
                prefix = cell[max(0, local_start - 24):local_start]
                suffix = cell[local_end:local_end + 24]
                historical = bool(
                    removed_before.search(prefix)
                    or removed_verb_before.search(prefix)
                    or removed_after.search(suffix))
                if not historical and cell_index == 0 and len(spans) > 1:
                    disposition = line[spans[1][0]:spans[1][1]].strip()
                    historical = bool(disposition_verb.search(disposition))
            else:
                begin, end = sentence_bounds(line, match.start(), match.end())
                prefix = line[max(begin, match.start() - 24):match.start()]
                suffix = line[match.end():min(end, match.end() + 24)]
                historical = bool(
                    removed_before.search(prefix)
                    or removed_verb_before.search(prefix)
                    or removed_after.search(suffix))
            active[name] = active.get(name, False) or not historical
print('\n'.join(sorted(name for name, is_active in active.items() if is_active)))
PY
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
    python3 - "$REPO_ROOT" <<'PY'
import pathlib
import re
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
files = subprocess.check_output(
    ['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard'])
extensions = {'.java', '.proto', '.h', '.hpp', '.cc', '.cpp'}
declaration = re.compile(r'\b(?:class|struct|interface|enum|record|message)\s+([A-Z][A-Za-z0-9]*)')
symbols = set()
for raw in files.split(b'\0'):
    if not raw:
        continue
    path = pathlib.Path(raw.decode('utf-8', errors='surrogateescape'))
    if (path.suffix not in extensions or 'target' in path.parts
            or any(part.startswith('.') for part in path.parts)
            or path.parts[:2] in (('website', 'node_modules'), ('website', 'build'))):
        continue
    source = root / path
    if source.is_symlink() or not source.is_file():
        continue
    symbols.add(path.stem)
    symbols.update(declaration.findall(source.read_text(encoding='utf-8', errors='replace')))
print('\n'.join(sorted(symbols)))
PY
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
