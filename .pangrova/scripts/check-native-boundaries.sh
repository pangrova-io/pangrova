#!/usr/bin/env bash
# Enforces the Native source layout and the JNI boundary.
#
# - Sources live only in the declared per-target directories under src/.
# - Java_* / JNI_OnLoad entry points are defined only in src/bridge/.
# - jni.h is included only by the bridge and the registered Native->Java upcall
#   adapters, so any new producer of JNI types is an explicit, reviewed change.
#
# Usage:  .pangrova/scripts/check-native-boundaries.sh
# Exit:   0 = conforming, 1 = violation.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NATIVE_DIR="${ROOT_DIR}/pangrova-worker-native"
SRC_DIR="${NATIVE_DIR}/src"

TARGET_DIRS=(bridge capabilities engine indexing lance udf vector)
JNI_UPCALL_ADAPTERS=(
    "engine/memory/PangrovaAllocationListener.h"
    "engine/shuffle/ShuffleEgressExecutor.h"
    "engine/velox/runtime/NativeEngineLifecycle.h"
    "engine/write/NativeWriteExecutor.h"
    "udf/JvmScalarFunction.h"
)
# Seams where the execution core reaches into capability/index implementations.
# See .pangrova/rules/architecture.md; inverting them is required before the
# execution core can be link-independent of those implementations.
ENGINE_CAPABILITY_SEAMS=(
    "engine/executor/driver/PlanExecutionState.h"
    "engine/executor/driver/TaskExecutor.h"
    "engine/velox/filter/DynamicFilterConverter.cpp"
    "engine/velox/plan/ArtifactBuildOperator.cpp"
    "engine/velox/plan/ArtifactBuildOperator.h"
    "engine/velox/plan/FilteredArtifactRetrievalOperator.h"
    "engine/velox/plan/GeneratedValuesOverlayOperator.h"
    "engine/velox/plan/RetrievalMergeOperator.h"
    "engine/velox/plan/SubstraitArtifactBuildConversion.cpp"
    "engine/velox/plan/SubstraitFilteredRetrievalConversion.cpp"
    "engine/velox/plan/SubstraitGeneratedValuesConversion.cpp"
    "engine/velox/plan/SubstraitRelConversion.cpp"
    "engine/velox/plan/SubstraitRetrievalMergeConversion.cpp"
    "engine/velox/plan/SubstraitRetrievalSourceConversion.cpp"
    "engine/velox/plan/SubstraitSortConversion.cpp"
    "engine/velox/runtime/NativeEngineLifecycle.cpp"
    "engine/velox/runner/VeloxTaskRunner.cpp"
)

if [[ ! -d "${SRC_DIR}" ]]; then
    echo "FAIL: native source directory is missing: ${SRC_DIR}" >&2
    exit 1
fi

fail=0

while IFS= read -r entry; do
    name="$(basename "${entry}")"
    expected=0
    for target in "${TARGET_DIRS[@]}"; do
        [[ "${name}" == "${target}" ]] && expected=1
    done
    if [[ "${expected}" -eq 0 ]]; then
        echo "FAIL: undeclared native source group: ${entry#"${NATIVE_DIR}/"}" >&2
        fail=1
    fi
done < <(find "${SRC_DIR}" -mindepth 1 -maxdepth 1 -type d | sort)

while IFS= read -r hit; do
    echo "FAIL: JNI entry point defined outside src/bridge: ${hit#"${NATIVE_DIR}/"}" >&2
    fail=1
done < <(grep -rlnE '(^|[^A-Za-z0-9_])(Java_[A-Za-z0-9_]+|JNI_OnLoad)[[:space:]]*\(' \
    "${SRC_DIR}" --include='*.cpp' --include='*.h' --include='*.cc' \
    | grep -v "^${SRC_DIR}/bridge/" | sort || true)

allowlist="$(printf '%s\n' "${JNI_UPCALL_ADAPTERS[@]}" | sort)"
while IFS= read -r hit; do
    relative="${hit#"${SRC_DIR}/"}"
    [[ "${relative}" == bridge/* ]] && continue
    if ! grep -qxF "${relative}" <<< "${allowlist}"; then
        echo "FAIL: jni.h included outside the bridge/upcall boundary: ${hit#"${NATIVE_DIR}/"}" >&2
        fail=1
    fi
done < <(grep -rlE '^[[:space:]]*#include[[:space:]]*[<"]jni\.h[>"]' \
    "${SRC_DIR}" --include='*.cpp' --include='*.h' --include='*.cc' | sort || true)

seams="$(printf '%s\n' "${ENGINE_CAPABILITY_SEAMS[@]}" | sort)"
while IFS= read -r hit; do
    relative="${hit#"${SRC_DIR}/"}"
    if ! grep -qxF "${relative}" <<< "${seams}"; then
        echo "FAIL: new execution-core dependency on a capability/index implementation: ${hit#"${NATIVE_DIR}/"}" >&2
        fail=1
    fi
done < <(grep -rlE '^[[:space:]]*#include[[:space:]]*[<"](capabilities/artifact|indexing)/' \
    "${SRC_DIR}/engine" --include='*.cpp' --include='*.h' --include='*.cc' | sort || true)

if [[ "${fail}" -ne 0 ]]; then
    exit 1
fi

echo "PASS: native layout and JNI boundary are conforming"
