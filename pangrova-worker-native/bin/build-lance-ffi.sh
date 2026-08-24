#!/usr/bin/env bash
# ==============================================================================
# build-lance-ffi.sh — Build Lance C FFI static library for target platforms
#
# Produces liblance_c_ffi.a in lance-ffi/target/<target>/release/
# The CMake build (FindLanceCFFI.cmake) auto-detects this location.
#
# Usage:
#   ./bin/build-lance-ffi.sh              # Build for host platform
#   ./bin/build-lance-ffi.sh --all        # Build for all supported targets
#   ./bin/build-lance-ffi.sh --target <t> # Build for specific target triple
#
# Supported targets:
#   aarch64-apple-darwin     (macOS Apple Silicon)
#   x86_64-unknown-linux-gnu (Linux x86_64)
#   aarch64-unknown-linux-gnu (Linux ARM64)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LANCE_FFI_DIR="${SCRIPT_DIR}/../lance-ffi"

if [ ! -f "${LANCE_FFI_DIR}/Cargo.toml" ]; then
  echo "ERROR: lance-ffi/Cargo.toml not found at ${LANCE_FFI_DIR}"
  exit 1
fi

# Determine host target
detect_host_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "${os}" in
    Darwin)
      echo "aarch64-apple-darwin"
      ;;
    Linux)
      case "${arch}" in
        x86_64)  echo "x86_64-unknown-linux-gnu" ;;
        aarch64) echo "aarch64-unknown-linux-gnu" ;;
        *)       echo "x86_64-unknown-linux-gnu" ;;
      esac
      ;;
    *)
      echo "x86_64-unknown-linux-gnu"
      ;;
  esac
}

ALL_TARGETS=(
  "aarch64-apple-darwin"
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
)

build_target() {
  local target="$1"
  echo "=== Building Lance C FFI for ${target} ==="
  cd "${LANCE_FFI_DIR}"
  cargo build --release --target "${target}"
  local lib_path="target/${target}/release/liblance_c_ffi.a"
  if [ -f "${lib_path}" ]; then
    local size
    size=$(du -h "${lib_path}" | cut -f1)
    echo "  OK: ${lib_path} (${size})"
  else
    echo "  ERROR: ${lib_path} not found after build"
    exit 1
  fi
}

# Parse arguments
TARGET=""
BUILD_ALL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      BUILD_ALL=true
      shift
      ;;
    --target)
      TARGET="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--all | --target <triple>]"
      exit 1
      ;;
  esac
done

# Execute builds
if [ "${BUILD_ALL}" = true ]; then
  for t in "${ALL_TARGETS[@]}"; do
    build_target "${t}"
  done
elif [ -n "${TARGET}" ]; then
  build_target "${TARGET}"
else
  HOST_TARGET="$(detect_host_target)"
  build_target "${HOST_TARGET}"
fi

echo ""
echo "=== Lance C FFI build complete ==="
echo "CMake will auto-detect the library via FindLanceCFFI.cmake"
