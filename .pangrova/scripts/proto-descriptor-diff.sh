#!/usr/bin/env bash
# Proto descriptor diff gate (Task 28.2).
#
# Generates FileDescriptorSet binaries for both the Java-side proto schemas
# (pangrova-protocol/src/main/proto) and the C++/native-side proto schemas
# (pangrova-worker-native/proto). Compares each generated descriptor against a
# committed golden file under .pangrova/scripts/proto-descriptors/.
#
# Detects:
#   - Unannounced breaking changes (renamed/removed/retyped fields).
#   - Schema drift where Java + native sides drift apart silently.
#
# Workflow for intentional proto changes:
#   1. Modify the .proto file.
#   2. Run .pangrova/scripts/proto-descriptor-diff.sh --update to refresh the golden.
#   3. Commit both the .proto and the updated golden descriptor in the same PR.
#
# Why descriptor diff rather than text diff:
#   protoc descriptors carry resolved type info (field numbers, wire types,
#   reserved ranges, oneof groupings), so a binary descriptor diff catches
#   wire-incompatible changes that a textual diff might miss.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GOLDEN_DIR="${ROOT_DIR}/.pangrova/scripts/proto-descriptors"
JAVA_PROTO_DIR="${ROOT_DIR}/pangrova-protocol/src/main/proto"
NATIVE_PROTO_DIR="${ROOT_DIR}/pangrova-worker-native/proto"
# Only diff pangrova-owned protos; vendored protos (e.g. substrait) are upstream
# schemas that change independently and would create noise here.
NATIVE_PANGROVA_SUBDIR="pangrova/native/protocol/v1"

UPDATE=false
if [[ "${1:-}" == "--update" ]]; then
    UPDATE=true
fi

# Locate protoc. Prefer the one bundled by protobuf-maven-plugin so the version
# stays in sync with what the build uses. Fall back to system protoc.
find_protoc() {
    local candidates=(
        "${PROTOC:-}"
        "$(command -v protoc || true)"
    )
    # Maven-cached protoc (multiple versions possible — pick the latest).
    local mvn_protoc
    mvn_protoc=$(find "${HOME}/.m2/repository/com/google/protobuf/protoc" \
                    -type f -name 'protoc-*.exe' 2>/dev/null | sort | tail -1 || true)
    if [[ -n "${mvn_protoc}" ]]; then
        candidates+=("${mvn_protoc}")
    fi

    for c in "${candidates[@]}"; do
        if [[ -n "${c}" && -x "${c}" ]]; then
            echo "${c}"
            return 0
        fi
    done
    return 1
}

PROTOC_BIN=$(find_protoc) || {
    echo "ERROR: protoc not found. Install protoc or run a maven build first" >&2
    echo "       so protobuf-maven-plugin caches it under ~/.m2/repository." >&2
    exit 2
}
echo "Using protoc: ${PROTOC_BIN}"

mkdir -p "${GOLDEN_DIR}"

generate_descriptor() {
    local side="$1"          # "java" or "native"
    local proto_root="$2"
    local out_file="$3"
    local subdir="${4:-}"    # optional: restrict to a sub-package

    # Collect the .proto files to include. When a subdir is given, only
    # pangrova-owned protos under that subdir are diffed (vendored protos
    # are excluded).
    local search_root="${proto_root}"
    [[ -n "${subdir}" ]] && search_root="${proto_root}/${subdir}"

    # Exclude protos importing vendored upstream schemas (e.g. substrait) —
    # those introduce transitive deps we don't want to drag into the gate.
    local protos
    protos=$(find "${search_root}" -type f -name '*.proto' \
                 -exec grep -L 'import.*substrait' {} \; \
                 | sort)
    if [[ -z "${protos}" ]]; then
        echo "ERROR: no .proto files found under ${search_root}" >&2
        return 1
    fi

    # shellcheck disable=SC2086
    "${PROTOC_BIN}" \
        --proto_path="${proto_root}" \
        --descriptor_set_out="${out_file}" \
        --include_imports \
        ${protos}
}

diff_or_update() {
    local side="$1"
    local generated="$2"
    local golden="${GOLDEN_DIR}/${side}.descriptor"

    if [[ "${UPDATE}" == "true" ]]; then
        cp "${generated}" "${golden}"
        echo "UPDATED golden: ${golden#${ROOT_DIR}/}"
        return 0
    fi

    if [[ ! -f "${golden}" ]]; then
        echo "ERROR: golden descriptor missing for ${side} side: ${golden#${ROOT_DIR}/}" >&2
        echo "       Run: .pangrova/scripts/proto-descriptor-diff.sh --update to seed it." >&2
        return 1
    fi

    if ! cmp -s "${generated}" "${golden}"; then
        echo "FAIL: ${side}-side proto descriptor drift detected." >&2
        echo "  Generated: ${generated}" >&2
        echo "  Golden   : ${golden#${ROOT_DIR}/}" >&2
        echo "" >&2
        echo "If this change is intentional, regenerate the golden with:" >&2
        echo "  .pangrova/scripts/proto-descriptor-diff.sh --update" >&2
        echo "and commit the updated descriptor alongside the .proto change." >&2

        # Print human-friendly diff using protoc --decode if possible.
        if command -v protoc >/dev/null 2>&1; then
            local descriptor_proto
            descriptor_proto=$(find "${ROOT_DIR}" -path '*/google/protobuf/descriptor.proto' \
                                -not -path '*/build/*' 2>/dev/null | head -1)
            if [[ -n "${descriptor_proto}" ]]; then
                echo "" >&2
                echo "Decoded diff (first 80 lines):" >&2
                diff \
                    <("${PROTOC_BIN}" --decode=google.protobuf.FileDescriptorSet \
                          "${descriptor_proto}" < "${golden}" 2>/dev/null) \
                    <("${PROTOC_BIN}" --decode=google.protobuf.FileDescriptorSet \
                          "${descriptor_proto}" < "${generated}" 2>/dev/null) \
                    | head -80 >&2 || true
            fi
        fi
        return 1
    fi
    echo "OK: ${side}-side proto descriptor matches golden."
}

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

status=0

# Java side.
if [[ -d "${JAVA_PROTO_DIR}" ]]; then
    JAVA_OUT="${TMPDIR}/java.descriptor"
    generate_descriptor "java" "${JAVA_PROTO_DIR}" "${JAVA_OUT}"
    diff_or_update "java" "${JAVA_OUT}" || status=1
else
    echo "WARN: java proto dir missing: ${JAVA_PROTO_DIR}" >&2
fi

# Native side (pangrova-owned protos only; substrait/* is vendored upstream).
if [[ -d "${NATIVE_PROTO_DIR}/${NATIVE_PANGROVA_SUBDIR}" ]]; then
    NATIVE_OUT="${TMPDIR}/native.descriptor"
    generate_descriptor "native" "${NATIVE_PROTO_DIR}" "${NATIVE_OUT}" "${NATIVE_PANGROVA_SUBDIR}"
    diff_or_update "native" "${NATIVE_OUT}" || status=1
else
    echo "WARN: native proto dir missing: ${NATIVE_PROTO_DIR}/${NATIVE_PANGROVA_SUBDIR}" >&2
fi

if [[ ${status} -eq 0 ]]; then
    echo "OK: proto descriptor diff gate passed."
fi
exit ${status}
