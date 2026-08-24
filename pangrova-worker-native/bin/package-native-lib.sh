#!/usr/bin/env bash
# ==============================================================================
# package-native-lib.sh — Copy built native library into classpath resource dir
#
# Usage:
#   ./pangrova-worker-native/bin/package-native-lib.sh [path-to-dylib-or-so]
#
# If no argument is given, auto-detects from pangrova-worker-native/build/.
#
# Resource layout (sqlite-jdbc / Netty tcnative pattern):
#   pangrova-worker/src/main/resources/META-INF/native/{os}/{arch}/lib...
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NATIVE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$NATIVE_ROOT/.." && pwd)"
RESOURCE_BASE="$PROJECT_ROOT/pangrova-worker/src/main/resources/META-INF/native"

detect_platform() {
    local uname_s uname_m
    uname_s="$(uname -s)"
    uname_m="$(uname -m)"
    case "$uname_s" in
        Darwin) OS="osx" ;;
        Linux)  OS="linux" ;;
        *)      echo "ERROR: Unsupported OS: $uname_s" >&2; exit 1 ;;
    esac
    case "$uname_m" in
        arm64|aarch64) ARCH="aarch64" ;;
        x86_64|amd64)  ARCH="x86_64" ;;
        *)             echo "ERROR: Unsupported arch: $uname_m" >&2; exit 1 ;;
    esac
}

find_native_lib() {
    if [[ $# -ge 1 && -f "$1" ]]; then
        NATIVE_LIB="$1"
        return
    fi
    local build_dir="$NATIVE_ROOT/build"
    if [[ -f "$build_dir/libpangrova_native.dylib" ]]; then
        NATIVE_LIB="$build_dir/libpangrova_native.dylib"
    elif [[ -f "$build_dir/libpangrova_native.so" ]]; then
        NATIVE_LIB="$build_dir/libpangrova_native.so"
    else
        echo "ERROR: No native library found in $build_dir" >&2
        echo "  Build first: ./pangrova-worker-native/bin/build-native.sh" >&2
        exit 1
    fi
}

main() {
    detect_platform
    find_native_lib "$@"
    local target_dir="$RESOURCE_BASE/$OS/$ARCH"
    local file_name
    file_name="$(basename "$NATIVE_LIB")"
    mkdir -p "$target_dir"
    cp "$NATIVE_LIB" "$target_dir/$file_name"
    echo "Packaged: $NATIVE_LIB"
    echo "      -> $target_dir/$file_name"
    echo "Platform: $OS/$ARCH"
    file "$target_dir/$file_name"
}

main "$@"
