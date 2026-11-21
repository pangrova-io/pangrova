#!/usr/bin/env bash
# Read the generation identity from the actual SDK archive before recording it.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ $# == 2 ]] || { printf 'Usage: record-function-provenance.sh SOURCE ARCHIVE\n' >&2; exit 1; }
sdk_source="$1"
sdk_archive="$2"
[[ -f "$sdk_archive" && -f "$sdk_source/velox/expression/FunctionRegistryIdentity.h" ]] \
  || { printf 'SDK archive or compiled identity API is missing; rebuild the SDK\n' >&2; exit 1; }
archive_hash="$(openssl dgst -sha256 "$sdk_archive" | awk '{print $NF}')"
[[ "$archive_hash" =~ ^[0-9a-f]{64}$ ]] \
  || { printf 'Invalid actual SDK archive identity\n' >&2; exit 1; }
inspector="$sdk_archive.pangrova-identity-inspector"
compiler="${CXX:-c++}"
flags=(-std=c++20 -Wall -Wextra -Werror)
if [[ "${PANGROVA_FUNCTION_INSPECTOR_ASAN:-0}" == 1 ]]; then
  flags+=(-fsanitize=address -fno-omit-frame-pointer)
fi
"$compiler" "${flags[@]}" "-DPANGROVA_INSPECTED_ARCHIVE_SHA256=\"$archive_hash\"" \
  -I "$sdk_source" "$script_dir/../tools/function-contracts/FunctionGenerationIdentity.cpp" \
  "$sdk_archive" -o "$inspector"
PANGROVA_FUNCTION_PYTHON="${PANGROVA_FUNCTION_CONTRACT_PYTHON:-python3}" \
  bash "$script_dir/apply-velox-patches.sh" record "$sdk_source" "$sdk_archive" "$inspector"
