#!/usr/bin/env bash
# ==============================================================================
# build-native.sh — One-command native build pipeline
#
# Idempotent: safe to run repeatedly. Skips already-built deps.
#
# Usage:
#   ./pangrova-worker-native/bin/build-native.sh              # full pipeline
#   ./pangrova-worker-native/bin/build-native.sh --skip-deps  # skip deps rebuild
#   ./pangrova-worker-native/bin/build-native.sh --skip-velox # skip Velox rebuild
#   ./pangrova-worker-native/bin/build-native.sh --clean      # clean build dir
#
# What it does:
#   1. Run setup-native-build-env.sh (builds hermetic deps + Velox)
#   2. Source env.sh (activates all paths)
#   3. Build the required Lance Rust FFI
#   4. cmake --preset dev-release (configure)
#   5. cmake --build (compile + link)
#   6. package-native-lib.sh (copy dylib to classpath resources)
#
# Environment:
#   All paths are derived from ~/.pangrova/ — no manual configuration needed.
#   Override with: PANGROVA_HOME, PANGROVA_DEPS_PREFIX, VELOX_HOME, etc.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NATIVE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$NATIVE_ROOT/.." && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$HOME/.pangrova}"
NPROC="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)"

CLEAN=false
for arg in "$@"; do
  case "$arg" in
    --skip-deps)  export SKIP_DEPS=1 ;;
    --skip-velox) export SKIP_VELOX=1 ;;
    --clean)      CLEAN=true ;;
    *)            echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

log() { printf "\n=== %s ===\n" "$*"; }

log "Step 1/6: Setup native build environment"
"$SCRIPT_DIR/setup-native-build-env.sh"

log "Step 2/6: Activate environment"
# shellcheck disable=SC1091
source "$PANGROVA_HOME/env.sh"
echo "  PANGROVA_DEPS_PREFIX=$PANGROVA_DEPS_PREFIX"
echo "  VELOX_HOME=$VELOX_HOME"
echo "  VELOX_BUILD_DIR=$VELOX_BUILD_DIR"
echo "  VELOX_INSTALL_PREFIX=$VELOX_INSTALL_PREFIX"

log "Step 3/6: Build required Lance C FFI"
"$SCRIPT_DIR/build-lance-ffi.sh"

log "Step 4/6: CMake configure"
if $CLEAN && [[ -d "$NATIVE_ROOT/build" ]]; then
  echo "  Cleaning build directory"
  rm -rf "$NATIVE_ROOT/build"
fi
cmake --preset dev-release -S "$NATIVE_ROOT"

log "Step 5/6: CMake build (-j$NPROC)"
cmake --build "$NATIVE_ROOT/build" -j "$NPROC"

log "Step 6/6: Package native library"
"$SCRIPT_DIR/package-native-lib.sh"

log "Native build pipeline complete"
ls -lh "$NATIVE_ROOT/build/libpangrova_native."*
