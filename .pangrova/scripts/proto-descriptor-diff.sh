#!/usr/bin/env bash
# Proto descriptor diff gate (Task 28.2).
#
# Generates one FileDescriptorSet from the canonical process/JNI schemas in
# pangrova-protocol. Maven and CMake consume this same tree; a second "native"
# descriptor would recreate the ownership split this gate is meant to prevent.
#
# Detects:
#   - Unannounced breaking changes (renamed/removed/retyped fields).
#   - Accidental reintroduction of a second Pangrova boundary schema owner.
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
PROTO_IMPORT_DIR="${ROOT_DIR}/pangrova-protocol/src/main/proto-imports"
CANONICAL_SUBDIR="pangrova/protocol/v2"

# Keep the descriptor compiler aligned with the version pinned by the Maven
# reactor.  Protobuf's Java runtime uses the 4.x artifact line while protoc
# reports the corresponding language version (for example 4.35.1 -> 35.1).
PROTOBUF_ARTIFACT_VERSION=$(sed -n \
    's:.*<protobuf.version>\([^<]*\)</protobuf.version>.*:\1:p' \
    "${ROOT_DIR}/pom.xml" | head -1)
if [[ -z "${PROTOBUF_ARTIFACT_VERSION}" ]]; then
    echo "ERROR: protobuf.version is not defined in the root pom.xml" >&2
    exit 2
fi
PROTOC_LANGUAGE_VERSION="${PROTOBUF_ARTIFACT_VERSION#*.}"
MAVEN_REPOSITORY="${MAVEN_REPO_LOCAL:-${HOME}/.m2/repository}"
MAVEN_BIN="${MAVEN_BIN:-mvn}"
MAVEN_DEPENDENCY_PLUGIN_VERSION="3.8.1"

UPDATE=false
PREPARE_ONLY=false
case "${1:-}" in
    --update) UPDATE=true ;;
    --prepare-only) PREPARE_ONLY=true ;;
    "") ;;
    *)
        echo "ERROR: unknown argument: $1" >&2
        exit 2
        ;;
esac

resolve_protoc_classifier() {
    local os arch
    os=$(uname -s)
    arch=$(uname -m)
    case "${os}:${arch}" in
        Linux:x86_64|Linux:amd64) echo "linux-x86_64" ;;
        Linux:aarch64|Linux:arm64) echo "linux-aarch_64" ;;
        Darwin:x86_64|Darwin:amd64) echo "osx-x86_64" ;;
        Darwin:arm64|Darwin:aarch64) echo "osx-aarch_64" ;;
        MINGW*:x86_64|MSYS*:x86_64|CYGWIN*:x86_64) echo "windows-x86_64" ;;
        *)
            echo "ERROR: no pinned protoc classifier mapping for ${os}/${arch}" >&2
            return 1
            ;;
    esac
}

PROTOC_CLASSIFIER=$(resolve_protoc_classifier)
PINNED_PROTOC="${MAVEN_REPOSITORY}/com/google/protobuf/protoc/${PROTOBUF_ARTIFACT_VERSION}/protoc-${PROTOBUF_ARTIFACT_VERSION}-${PROTOC_CLASSIFIER}.exe"
PROTOBUF_JAVA_JAR="${MAVEN_REPOSITORY}/com/google/protobuf/protobuf-java/${PROTOBUF_ARTIFACT_VERSION}/protobuf-java-${PROTOBUF_ARTIFACT_VERSION}.jar"

fetch_maven_artifact() {
    local artifact="$1"
    "${MAVEN_BIN}" -q \
        -Dmaven.repo.local="${MAVEN_REPOSITORY}" \
        "org.apache.maven.plugins:maven-dependency-plugin:${MAVEN_DEPENDENCY_PLUGIN_VERSION}:get" \
        -Dtransitive=false \
        -Dartifact="${artifact}"
}

prepare_proto_toolchain() {
    mkdir -p "${MAVEN_REPOSITORY}"
    if [[ ! -f "${PINNED_PROTOC}" ]]; then
        echo "Fetching pinned protoc ${PROTOBUF_ARTIFACT_VERSION} (${PROTOC_CLASSIFIER})..."
        fetch_maven_artifact \
            "com.google.protobuf:protoc:${PROTOBUF_ARTIFACT_VERSION}:exe:${PROTOC_CLASSIFIER}"
    fi
    chmod +x "${PINNED_PROTOC}"
    if [[ ! -f "${PROTOBUF_JAVA_JAR}" ]]; then
        echo "Fetching pinned protobuf-java ${PROTOBUF_ARTIFACT_VERSION}..."
        fetch_maven_artifact \
            "com.google.protobuf:protobuf-java:${PROTOBUF_ARTIFACT_VERSION}:jar"
    fi
    local actual_version
    actual_version=$("${PINNED_PROTOC}" --version 2>/dev/null || true)
    if [[ "${actual_version}" != "libprotoc ${PROTOC_LANGUAGE_VERSION}" ]]; then
        echo "ERROR: pinned protoc reports '${actual_version}', expected " \
             "'libprotoc ${PROTOC_LANGUAGE_VERSION}'" >&2
        exit 2
    fi
}

prepare_proto_toolchain
if [[ "${PREPARE_ONLY}" == "true" ]]; then
    echo "OK: pinned proto toolchain is ready in ${MAVEN_REPOSITORY}."
    exit 0
fi

# Locate the exact protoc pinned by protobuf-maven-plugin. An explicit PROTOC
# override is supported, but is rejected if its language version differs.
find_protoc() {
    local candidates=()
    if [[ -n "${PROTOC:-}" ]]; then
        candidates+=("${PROTOC}")
    fi
    candidates+=("${PINNED_PROTOC}")
    while IFS= read -r candidate; do
        candidates+=("${candidate}")
    done < <(find "${MAVEN_REPOSITORY}/com/google/protobuf/protoc/${PROTOBUF_ARTIFACT_VERSION}" \
                    -type f -name "protoc-${PROTOBUF_ARTIFACT_VERSION}-*.exe" \
                    2>/dev/null | sort)
    candidates+=(
        "$(command -v protoc || true)"
        "${PANGROVA_DEPS_PREFIX:-}/bin/protoc"
        "${HOME}/.pangrova/native-deps/bin/protoc"
    )

    for c in "${candidates[@]}"; do
        if [[ -n "${c}" && -x "${c}" && \
              "$("${c}" --version 2>/dev/null)" == "libprotoc ${PROTOC_LANGUAGE_VERSION}" ]]; then
            echo "${c}"
            return 0
        fi
    done
    return 1
}

PROTOC_BIN=$(find_protoc) || {
    echo "ERROR: protoc ${PROTOC_LANGUAGE_VERSION} (artifact ${PROTOBUF_ARTIFACT_VERSION}) not found." >&2
    echo "       The pinned Maven artifact could not be resolved." >&2
    exit 2
}
echo "Using protoc: ${PROTOC_BIN}"

mkdir -p "${GOLDEN_DIR}"
if [[ "${UPDATE}" == "true" ]]; then
    # The repository has one canonical boundary. Remove the retired split
    # snapshot during the same explicit update operation that writes its v2 replacement.
    rm -f "${GOLDEN_DIR}/canonical-v1.descriptor"
fi

generate_descriptor() {
    local side="$1"
    local proto_root="$2"
    local out_file="$3"
    local subdir="${4:-}"    # optional: restrict to a sub-package

    # Collect the .proto files to include. When a subdir is given, only
    # pangrova-owned protos under that subdir are diffed (vendored protos
    # are excluded).
    local search_root="${proto_root}"
    [[ -n "${subdir}" ]] && search_root="${proto_root}/${subdir}"

    local protos
    protos=$(find "${search_root}" -type f -name '*.proto' | sort)
    if [[ -z "${protos}" ]]; then
        echo "ERROR: no .proto files found under ${search_root}" >&2
        return 1
    fi

    # shellcheck disable=SC2086
    "${PROTOC_BIN}" \
        --proto_path="${proto_root}" \
        --proto_path="${PROTO_IMPORT_DIR}" \
        --proto_path="${WKT_PROTO_DIR}" \
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

DESCRIPTOR_TMP_DIR=$(mktemp -d)
trap 'rm -rf "${DESCRIPTOR_TMP_DIR}"' EXIT

# protobuf-java ships the well-known-type IDL used by Substrait. Extract the
# files from the exact pinned runtime instead of borrowing headers from a
# machine-specific system protobuf installation.
if [[ ! -f "${PROTOBUF_JAVA_JAR}" ]]; then
    echo "ERROR: pinned protobuf runtime not found: ${PROTOBUF_JAVA_JAR}" >&2
    echo "       The pinned Maven artifact could not be resolved." >&2
    exit 2
fi
WKT_PROTO_DIR="${DESCRIPTOR_TMP_DIR}/protobuf-wkt"
mkdir -p "${WKT_PROTO_DIR}"
(cd "${WKT_PROTO_DIR}" && jar xf "${PROTOBUF_JAVA_JAR}")
if [[ ! -f "${WKT_PROTO_DIR}/google/protobuf/any.proto" ]]; then
    echo "ERROR: protobuf-java ${PROTOBUF_ARTIFACT_VERSION} does not contain WKT IDL" >&2
    exit 2
fi

status=0

if [[ -d "${JAVA_PROTO_DIR}/${CANONICAL_SUBDIR}" ]]; then
    CANONICAL_OUT="${DESCRIPTOR_TMP_DIR}/canonical-v2.descriptor"
    generate_descriptor "canonical-v2" "${JAVA_PROTO_DIR}" \
        "${CANONICAL_OUT}" "${CANONICAL_SUBDIR}"
    diff_or_update "canonical-v2" "${CANONICAL_OUT}" || status=1
else
    echo "ERROR: canonical proto dir missing: ${JAVA_PROTO_DIR}/${CANONICAL_SUBDIR}" >&2
    status=1
fi

if find "${ROOT_DIR}/pangrova-worker-native/proto" -type f \
        -path "${ROOT_DIR}/pangrova-worker-native/proto/pangrova/*" \
        -name '*.proto' -print -quit | grep -q .; then
    echo "FAIL: Pangrova-owned boundary proto found under worker-native/proto." >&2
    status=1
fi

if [[ ${status} -eq 0 ]]; then
    echo "OK: proto descriptor diff gate passed."
fi
exit ${status}
