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
PANGROVA_HOME="${PANGROVA_HOME:-$HOME/.pangrova}"
DEPS_PREFIX="${PANGROVA_DEPS_PREFIX:-$PANGROVA_HOME/native-deps}"

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

if [ ! -f "${LANCE_FFI_DIR}/Cargo.toml" ]; then
  die "lance-ffi/Cargo.toml not found at ${LANCE_FFI_DIR}"
fi

command -v cargo >/dev/null 2>&1 || die "cargo not found; install the pinned Rust toolchain"
command -v rustc >/dev/null 2>&1 || die "rustc not found; install the pinned Rust toolchain"

# One reviewed SDK dependency tree for every caller, never a registry mutation.
bash "${SCRIPT_DIR}/prepare-lance-sdk.sh" prepare \
  "$(cd "${SCRIPT_DIR}/.." && pwd)/deps-download/lance-sdk-owned"

if [ -z "${PROTOC:-}" ]; then
  if [ -x "${DEPS_PREFIX}/bin/protoc" ]; then
    PROTOC="${DEPS_PREFIX}/bin/protoc"
  else
    PROTOC="$(command -v protoc || true)"
  fi
fi
[ -n "${PROTOC:-}" ] && [ -x "${PROTOC}" ] || \
  die "protoc not found; run setup-native-build-env.sh first or set PROTOC"

if [ -z "${PROTOC_INCLUDE:-}" ]; then
  if [ -f "${DEPS_PREFIX}/include/google/protobuf/empty.proto" ]; then
    PROTOC_INCLUDE="${DEPS_PREFIX}/include"
  else
    protoc_prefix="$(cd "$(dirname "${PROTOC}")/.." && pwd)"
    PROTOC_INCLUDE="${protoc_prefix}/include"
  fi
fi
[ -f "${PROTOC_INCLUDE}/google/protobuf/empty.proto" ] || \
  die "protobuf well-known types not found under PROTOC_INCLUDE=${PROTOC_INCLUDE}"
export PROTOC PROTOC_INCLUDE

# Determine host target
detect_host_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "${os}" in
    Darwin)
      case "${arch}" in
        arm64|aarch64) echo "aarch64-apple-darwin" ;;
        x86_64)        echo "x86_64-apple-darwin" ;;
        *)             die "Unsupported macOS architecture: ${arch}" ;;
      esac
      ;;
    Linux)
      case "${arch}" in
        x86_64)  echo "x86_64-unknown-linux-gnu" ;;
        aarch64) echo "aarch64-unknown-linux-gnu" ;;
        *)       die "Unsupported Linux architecture: ${arch}" ;;
      esac
      ;;
    *)
      die "Unsupported operating system: ${os}"
      ;;
  esac
}

ALL_TARGETS=(
  "aarch64-apple-darwin"
  "x86_64-apple-darwin"
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
)

require_supported_target() {
  local requested="$1"
  local supported
  for supported in "${ALL_TARGETS[@]}"; do
    if [ "${requested}" = "${supported}" ]; then
      return 0
    fi
  done
  die "Unsupported Lance C FFI target: ${requested}"
}

build_target() {
  local target="$1"
  require_supported_target "${target}"
  echo "=== Building Lance C FFI for ${target} ==="
  cd "${LANCE_FFI_DIR}"
  if [[ "${target}" == *-apple-darwin ]]; then
    # Match Pangrova Native's deployment baseline. Without an explicit value,
    # Rust cc-based transitive dependencies inherit the build host's SDK floor
    # and produce a static archive that links with hundreds of newer-macOS
    # warnings (or fails on older production workers).
    export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
  fi
  local build_log
  build_log="$(mktemp "${TMPDIR:-/tmp}/pangrova-lance-build.XXXXXX")"
  if ! cargo build --color never --locked --release --target "${target}" 2>&1 | tee "$build_log"; then
    die "Lance compilation failed; diagnostics retained at ${build_log}"
  fi
  if grep -Eq '^warning(\[|:)' "$build_log"; then
    die "Lance compilation emitted warnings; diagnostics retained at ${build_log}"
  fi
  rm -- "$build_log"
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
  require_supported_target "${TARGET}"
  build_target "${TARGET}"
else
  HOST_TARGET="$(detect_host_target)"
  build_target "${HOST_TARGET}"
fi

echo ""
echo "=== Lance C FFI build complete ==="
echo "CMake will auto-detect the library via FindLanceCFFI.cmake"
