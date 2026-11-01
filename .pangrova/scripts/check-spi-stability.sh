#!/usr/bin/env bash
# Enforces that @Stable-annotated SPI interfaces have at least one
# implementation in the main source tree (production code).
#
# An SPI marked @Stable with zero implementors is aspirational — it promises
# contract stability but has never been proven in production. Such types
# should be downgraded to @Experimental until a real implementation exists.
#
# This gate complements check-spi-annotations.sh (which enforces annotation
# presence) by enforcing annotation *accuracy*.
#
# Usage:  .pangrova/scripts/check-spi-stability.sh
# Exit:   0 = OK, 1 = violations found, 2 = environment error.
#
# Compatible with bash 3.2 (macOS default).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Contract roots are every artifact declared in .pangrova/modules.yaml whose
# artifactId is pangrova-spi or ends in -api or -spi. Adding a contract artifact to the manifest
# enrolls it in this gate automatically.
CONTRACT_ROOTS=()
while IFS= read -r artifact; do
    dir=$(awk -v a="${artifact}" '
        $0 == "  " a ":" { found = 1; next }
        found && /^  [A-Za-z0-9_.-]+:/ { exit }
        found && /^    path:/ { print $2; exit }
    ' "${ROOT_DIR}/.pangrova/modules.yaml")
    [[ -n "${dir}" && -d "${ROOT_DIR}/${dir}/src/main/java" ]] \
        && CONTRACT_ROOTS+=("${ROOT_DIR}/${dir}/src/main/java")
done < <(sed -n -e 's/^  \(pangrova-.*-api\):$/\1/p' \
    -e 's/^  \(pangrova-.*-spi\):$/\1/p' \
    -e 's/^  \(pangrova-spi\):$/\1/p' "${ROOT_DIR}/.pangrova/modules.yaml")

if [[ ${#CONTRACT_ROOTS[@]} -eq 0 ]]; then
    echo "ERROR: no -api/-spi contract roots found in .pangrova/modules.yaml" >&2
    exit 2
fi

# Known exceptions: @Stable types that have indirect implementations
# (e.g., composed via a parent interface, or used structurally as return type).
#
# Format: one relative path per line (from repo root).
# Each exception must have a comment explaining why it is legitimate.
EXCEPTIONS=(
    # CatalogAccessControl, SchemaAccessControl, TableAccessControl,
    # ColumnAccessControl are composed into AccessControl which has
    # real implementors (RbacAccessControl, DynamicRbacAccessControl).
    "pangrova-spi/src/main/java/org/pangrova/spi/security/CatalogAccessControl.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/security/SchemaAccessControl.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/security/TableAccessControl.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/security/ColumnAccessControl.java"
    # TextMapSetter: implemented via anonymous inner class in
    # FlightClientTracingInterceptor.GrpcMetadataTextMapSetter.
    "pangrova-spi/src/main/java/org/pangrova/spi/observability/TextMapSetter.java"
    # Scope: structural type (returned by Span.makeCurrent()); has Scope.NOOP
    # constant and lambda implementations. Not meant for external impl.
    "pangrova-spi/src/main/java/org/pangrova/spi/observability/Scope.java"
    # ConnectorTransactionHandle: implemented via anonymous inner class
    # (EMPTY constant) and used as parameter type throughout connectors.
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/ConnectorTransactionHandle.java"
    # ConnectorMetadata sub-interfaces: composed into ConnectorMetadata which
    # is implemented by IcebergMetadata and PaimonMetadata.
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/ConnectorWriteMetadata.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/ConnectorSchemaMetadata.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/ConnectorQueryLifecycle.java"
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/ConnectorSnapshotMetadata.java"
    # Constraint: used as a parameter type (e.g., in ConnectorSplitManager.getSplits,
    # ConnectorMetadata.getTableStatistics); no class "implements Constraint" — the
    # sole implementation is the EMPTY lambda constant on the interface itself.
    "pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/Constraint.java"
)

violations=0
checked=0

while IFS= read -r -d '' file; do
    # Skip annotation package
    case "${file}" in
        */annotation/*) continue ;;
    esac

    # Only check interfaces and abstract classes with @Stable annotation
    # (not @Stable in Javadoc comments — match only lines starting with @)
    if ! grep -qE '^@Stable\b' "${file}"; then
        continue
    fi
    if ! grep -qE '^public (interface|abstract class) ' "${file}"; then
        continue
    fi

    # Extract the type name from the public declaration line.
    # Finds the first capitalized word after "public interface|abstract class",
    # stripping generic type parameters (<T>) and opening braces.
    type_name=$(grep -E '^public (interface|abstract class) ' "${file}" \
                | head -1 \
                | awk '{for(i=1;i<=NF;i++) if($i ~ /^[A-Z]/) {sub(/[<{].*/,"",$i); print $i; exit}}')
    [[ -z "${type_name}" ]] && continue

    rel_path="${file#${ROOT_DIR}/}"

    # Check exceptions
    skip=false
    for exc in "${EXCEPTIONS[@]}"; do
        if [[ "${rel_path}" == "${exc}" ]]; then
            skip=true
            break
        fi
    done
    if ${skip}; then
        continue
    fi

    checked=$((checked + 1))

    # Search for "implements <type>" or "extends <type>" in main source
    # (excluding the file itself, test directories, and the SPI module tests).
    # grep exits 1 when it matches nothing, which `set -e` would treat as fatal,
    # so both greps are guarded with `|| true` and the count comes from wc.
    impl_count=$({
        { grep -rl "implements.*\b${type_name}\b\|extends.*\b${type_name}\b" \
            --include="*.java" \
            "${ROOT_DIR}"/pangrova-*/src/main \
            "${ROOT_DIR}"/pangrova-*/*/src/main \
            2>/dev/null || true; } | grep -v "${file}" || true
    } | wc -l | tr -d ' ')

    if [[ "${impl_count}" -eq 0 ]]; then
        echo "VIOLATION: @Stable with zero implementors: ${rel_path} (${type_name})" >&2
        echo "  -> Downgrade to @Experimental or add a production implementation." >&2
        violations=$((violations + 1))
    fi
done < <(find "${CONTRACT_ROOTS[@]}" -type f -name '*.java' -print0)

if [[ ${violations} -gt 0 ]]; then
    echo "" >&2
    echo "FAIL: ${violations} @Stable SPI type(s) have zero production implementors." >&2
    echo "See G-SPI-06: @Stable must be backed by at least one real implementation." >&2
    exit 1
fi

echo "OK: SPI stability gate passed."
echo "  @Stable types checked: ${checked}"
echo "  All checked types have at least one production implementor."
exit 0
