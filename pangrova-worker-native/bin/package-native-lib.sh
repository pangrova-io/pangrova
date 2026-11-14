#!/usr/bin/env bash
# ==============================================================================
# package-native-lib.sh — Copy built native library into classpath resource dir
#
# Usage:
#   ./pangrova-worker-native/bin/package-native-lib.sh [--production] [path-to-dylib-or-so]
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
    if [[ $# -ge 1 ]]; then
        [[ $# -eq 1 && -f "$1" ]] || {
            echo "ERROR: Expected one existing native library path" >&2; exit 1;
        }
        NATIVE_LIB="$1"
        return
    fi
    local build_dir="$NATIVE_ROOT/build"
    # The build redirects CMAKE_LIBRARY_OUTPUT_DIRECTORY into the SDK cohort, so
    # honour the configured output directory before falling back to the build root.
    local lib_dir="$build_dir"
    if [[ -f "$build_dir/CMakeCache.txt" ]]; then
        local configured_dir
        configured_dir="$(sed -n 's/^CMAKE_LIBRARY_OUTPUT_DIRECTORY:[A-Z]*=//p' \
            "$build_dir/CMakeCache.txt" | head -n 1)"
        if [[ -n "$configured_dir" ]]; then
            lib_dir="$configured_dir"
        fi
    fi
    if [[ -f "$lib_dir/libpangrova_native.dylib" ]]; then
        NATIVE_LIB="$lib_dir/libpangrova_native.dylib"
    elif [[ -f "$lib_dir/libpangrova_native.so" ]]; then
        NATIVE_LIB="$lib_dir/libpangrova_native.so"
    elif [[ -f "$build_dir/libpangrova_native.dylib" ]]; then
        NATIVE_LIB="$build_dir/libpangrova_native.dylib"
    elif [[ -f "$build_dir/libpangrova_native.so" ]]; then
        NATIVE_LIB="$build_dir/libpangrova_native.so"
    else
        echo "ERROR: No native library found in $build_dir" >&2
        echo "  Build first: ./pangrova-worker-native/bin/build-native.sh" >&2
        exit 1
    fi
}

verify_lance_linkage() {
    command -v nm >/dev/null 2>&1 || {
        echo "ERROR: nm is required to verify the packaged native library" >&2
        exit 1
    }
    if ! nm -j "$NATIVE_LIB" 2>/dev/null \
        | grep -E '^_?lance_ffi_abi_version$' >/dev/null; then
        echo "ERROR: Native library does not contain the required Lance C FFI ABI" >&2
        exit 1
    fi
}

verify_linux_production() {
    [[ "$OS" == "linux" ]] || { echo "ERROR: Production validation requires Linux" >&2; exit 1; }
    command -v readelf >/dev/null || { echo "ERROR: readelf is required for production packaging" >&2; exit 1; }
    local header dynamic machine library
    header="$(readelf -h "$NATIVE_LIB")"
    case "$ARCH" in
        x86_64) machine='Advanced Micro Devices X86-64' ;;
        aarch64) machine='AArch64' ;;
    esac
    grep -F "Machine:" <<< "$header" | grep -F "$machine" >/dev/null || {
        echo "ERROR: Native ELF architecture differs from the packaging platform" >&2; exit 1;
    }
    dynamic="$(readelf -d "$NATIVE_LIB")"
    if grep -E '\((RPATH|RUNPATH)\)' <<< "$dynamic" >/dev/null; then
        echo "ERROR: Production JNI must not depend on a build-tree RPATH" >&2; exit 1;
    fi
    while IFS= read -r library; do
        case "$library" in
            libc.so.6|libm.so.6|libpthread.so.0|libdl.so.2|librt.so.1|libresolv.so.2|\
            libstdc++.so.6|libgcc_s.so.1|ld-linux-x86-64.so.2|ld-linux-aarch64.so.1) ;;
            *) echo "ERROR: Production JNI requires non-system shared dependency: $library" >&2; exit 1 ;;
        esac
    done < <(sed -n 's/.*(NEEDED).*\[\([^]]*\)\].*/\1/p' <<< "$dynamic")
}

main() {
    local production=false
    if [[ "${1:-}" == "--production" ]]; then
        production=true
        shift
    fi
    detect_platform
    find_native_lib "$@"
    if $production; then verify_linux_production; fi
    verify_lance_linkage
    local target_dir="$RESOURCE_BASE/$OS/$ARCH"
    local file_name
    file_name="$(basename "$NATIVE_LIB")"
    mkdir -p "$target_dir"
    cp "$NATIVE_LIB" "$target_dir/$file_name"
    echo "Packaged: $NATIVE_LIB"
    echo "      -> $target_dir/$file_name"
    echo "Platform: $OS/$ARCH"
    echo "Lance C FFI: linked and verified"
    file "$target_dir/$file_name"
}

main "$@"
