#!/usr/bin/env bash
# Apply and verify the repository-owned registration and Vector/Arrow ownership patch.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patch_file="$script_dir/../patches/velox-function-metadata-snapshot.patch"
pinned_commit=3da0390118fa4b0addbfbdeb03f438ec1b1237a6
source_files=(velox/expression/SimpleFunctionRegistry.h velox/expression/SimpleFunctionRegistry.cpp
  velox/vector/BaseVector.h velox/vector/BaseVector.cpp velox/vector/arrow/Bridge.cpp)
base_blobs=(77a3acb529dda19faf155e3e277956b5ce52faa7 2fa36c309dec1fd30ff18f5ec9df54bc1bc9b82d
  16fb99fc09d3deb28d8a2b7c6515a1fda38d5d70 454e72bf2aa1559d0b8e194abda4a8ee20766e53
  f980599e79a709e50941156f3398f75c43d1c96a)
patched_blobs=(edf6180d4785ebfd4571021ddc2007562e0c3c0c 701c242887a0a953326fe39fd213072ba43e5673
  f8d7e9600c182955a9b13fff2845348072c4d28b 26ca432896b1aa049be643062d49cb65994964b2
  544e2356e98d9983ff749c17e1c342f10d3311c5)

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
digest() { openssl dgst -sha256 "$1" | awk '{print $NF}'; }

[[ $# -ge 2 ]] || die "Usage: apply-velox-patches.sh apply|verify|record SOURCE [ARCHIVE]"
action="$1"
source_dir="$2"
[[ -d "$source_dir/.git" && -f "$patch_file" ]] || die "Pinned source or patch is missing"
[[ "$(git -C "$source_dir" rev-parse HEAD)" == "$pinned_commit" ]] || \
  die "Velox patch requires the exact pinned source commit"
for index in "${!source_files[@]}"; do
  [[ "$(git -C "$source_dir" rev-parse "HEAD:${source_files[$index]}")" == "${base_blobs[$index]}" ]] || \
    die "Pinned source blob differs from the reviewed patch base: ${source_files[$index]}"
done

has_blobs() {
  local state="$1" index expected
  for index in "${!source_files[@]}"; do
    expected="${base_blobs[$index]}"
    if [[ "$state" == patched || ( "$state" == snapshot && "$index" -lt 2 ) ]]; then
      expected="${patched_blobs[$index]}"
    fi
    [[ "$(git -C "$source_dir" hash-object "${source_files[$index]}")" == "$expected" ]] || return 1
  done
}

verify_source() {
  has_blobs patched || die "Velox source is missing the complete patch or has unreviewed drift"
  git -C "$source_dir" apply --reverse --check "$patch_file"
}

case "$action" in
  apply)
    if has_blobs base; then
      git -C "$source_dir" apply --check "$patch_file"
      git -C "$source_dir" apply "$patch_file"
    elif has_blobs snapshot; then
      # Exact upgrade from the prior reviewed metadata-only checkpoint.
      ownership_paths=(--include=velox/vector/BaseVector.h --include=velox/vector/BaseVector.cpp
        --include=velox/vector/arrow/Bridge.cpp)
      git -C "$source_dir" apply --check "${ownership_paths[@]}" "$patch_file"
      git -C "$source_dir" apply "${ownership_paths[@]}" "$patch_file"
    elif ! has_blobs patched; then
      die "Unsupported partial patch or unreviewed source drift"
    fi
    verify_source
    ;;
  verify) verify_source ;;
  record)
    verify_source
    [[ $# == 3 && -f "$3" ]] || die "Recording requires an actual rebuilt archive"
    nm -g "$3" | awk 'NF >= 2 && $(NF-1) ~ /^[TW]$/ && $NF ~ /getFunctionMetadataSnapshot/ { found=1 } END { exit !found }' || \
      die "Archive does not define the actual registration snapshot API; rebuild it"
    nm -g "$3" | awk 'NF >= 2 && $(NF-1) ~ /^[TW]$/ && $NF ~ /ownsMemoryPool/ { found=1 } END { exit !found }' || \
      die "Archive does not define Vector pool ownership; rebuild it"
    printf '%s\n%s\n%s\n' "$pinned_commit" "$(digest "$patch_file")" "$(digest "$3")" \
      > "$3.pangrova-provenance"
    ;;
  *) die "Unknown patch operation: $action" ;;
esac
