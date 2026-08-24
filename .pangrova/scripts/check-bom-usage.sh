#!/usr/bin/env bash
# Verify all BOM dependencies declared in the parent pom.xml are actually
# imported (used) somewhere in the codebase. Flags unused dependencies that
# should be removed or integrated.
#
# Exit codes:
#   0 - all BOM dependencies are used
#   1 - unused BOM dependencies found

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PARENT_POM="${REPO_ROOT}/pom.xml"

# For each BOM artifact, check if any child module pom.xml actually declares
# it as a dependency (not just in dependencyManagement).
check_usage() {
    local artifact="$1"
    local found
    found=$(find "$REPO_ROOT" -name "pom.xml" -not -path "$PARENT_POM" \
        -not -path "*/target/*" \
        -exec grep -l "<artifactId>${artifact}</artifactId>" {} \; 2>/dev/null | head -1)
    if [ -n "$found" ]; then
        return 0
    fi
    return 1
}

# Check if the artifact's classes are imported in Java source
check_import_usage() {
    local group_id="$1"
    local artifact="$2"

    local package_prefix=""
    case "$group_id" in
        com.zaxxer) package_prefix="com.zaxxer" ;;
        org.flywaydb) package_prefix="org.flywaydb" ;;
        org.postgresql) package_prefix="org.postgresql" ;;
        redis.clients) package_prefix="redis.clients" ;;
        com.github.ben-manes.caffeine) package_prefix="com.github.benmanes.caffeine" ;;
        io.jsonwebtoken) package_prefix="io.jsonwebtoken" ;;
        org.apache.kafka) package_prefix="org.apache.kafka" ;;
        org.yaml) package_prefix="org.yaml.snakeyaml" ;;
        *) return 1 ;;
    esac

    if [ -z "$package_prefix" ]; then
        return 1
    fi

    local found
    found=$(find "$REPO_ROOT" -name "*.java" -not -path "*/target/*" \
        -exec grep -l "import ${package_prefix}" {} \; 2>/dev/null | head -1)
    if [ -n "$found" ]; then
        return 0
    fi
    return 1
}

echo "=== BOM Dependency Usage Check ==="
echo ""

UNUSED=()
WARNINGS=()

# Key third-party dependencies to verify (non-internal, non-test-only)
# Format: groupId:artifactId
DEPS_TO_CHECK=(
    "com.zaxxer:HikariCP"
    "org.flywaydb:flyway-core"
    "org.postgresql:postgresql"
    "redis.clients:jedis"
    "com.github.ben-manes.caffeine:caffeine"
    "io.jsonwebtoken:jjwt-api"
    "org.apache.kafka:kafka-clients"
    "org.yaml:snakeyaml"
)

for dep in "${DEPS_TO_CHECK[@]}"; do
    group_id="${dep%%:*}"
    artifact="${dep##*:}"

    if check_usage "$artifact"; then
        if check_import_usage "$group_id" "$artifact"; then
            echo "  ✓ ${artifact} — declared and imported"
        else
            WARNINGS+=("${artifact} (${group_id}) — declared in module POM but no Java import found")
        fi
    else
        UNUSED+=("${artifact} (${group_id})")
    fi
done

echo ""

if [ ${#WARNINGS[@]} -gt 0 ]; then
    echo "⚠️  Dependencies declared but not imported in source code:"
    for w in "${WARNINGS[@]}"; do
        echo "  ⚠ ${w}"
    done
    echo ""
fi

if [ ${#UNUSED[@]} -gt 0 ]; then
    echo "❌ BOM dependencies not used in any module:"
    for u in "${UNUSED[@]}"; do
        echo "  ✗ ${u}"
    done
    echo ""
    echo "Action: Remove unused dependencies from BOM or integrate them."
    exit 1
fi

echo "✅ All checked BOM dependencies are used."
exit 0
