#!/usr/bin/env bash
# Carry only a completed same-run macOS dependency stage into a fresh runner.
set -euo pipefail

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ $# == 3 ]] || die "Usage: $0 pack|restore core|sdk BUNDLE_DIR"
operation="$1"
stage="$2"
bundle="$3"
root="${PANGROVA_HOME:-$HOME/.pangrova}"
case "$operation" in pack|restore) ;; *) die "Unknown operation: $operation";; esac
case "$stage" in core|sdk) ;; *) die "Unknown stage: $stage";; esac
[[ "$(uname -s)" == Darwin ]] || die "Native stage bundles are macOS only"
: "${GITHUB_RUN_ID:?missing GitHub run identity}"
: "${GITHUB_SHA:?missing revision identity}"
: "${GITHUB_WORKSPACE:?missing workspace path}"
: "${PANGROVA_LLVM_PREFIX:?missing selected compiler}"
: "${SDKROOT:?missing selected macOS SDK}"

fingerprint() {
    printf '%s\n' 'pangrova-macos-native-stage-v1' "stage=$stage" \
        "run=$GITHUB_RUN_ID" "revision=$GITHUB_SHA" "workspace=$GITHUB_WORKSPACE" \
        "prefix=$root" "architecture=$(uname -m)" "image=${ImageVersion:-unknown}" \
        "sdk=$SDKROOT" "compiler=$PANGROVA_LLVM_PREFIX"
    sw_vers -buildVersion
    xcodebuild -version
    "$PANGROVA_LLVM_PREFIX/bin/clang++" --version
    xcrun --sdk macosx --show-sdk-version
    brew list --versions openssl@3 libevent
}

require_file() {
    [[ -f "$root/$1" && -s "$root/$1" ]] || die "Stage $stage lacks required output: $1"
}

require_architecture() {
    xcrun lipo "$root/$1" -verify_arch "$(uname -m)" \
        || die "Stage $stage has an incompatible binary architecture: $1"
}

require_executable() {
    require_file "$1"
    [[ -x "$root/$1" ]] || die "Stage $stage required tool is not executable: $1"
    require_architecture "$1"
}

verify_outputs() {
    require_executable native-deps/bin/protoc
    require_executable native-deps/bin/grpc_cpp_plugin
    require_file native-deps/lib/cmake/protobuf/protobuf-config.cmake
    require_file native-deps/lib/cmake/grpc/gRPCConfig.cmake
    require_file native-deps/lib/cmake/re2/re2Config.cmake
    require_file native-deps/lib/libre2.a
    require_file velox/velox/core/PlanNode.h
    require_file velox/deps-install/lib/libfolly.a
    require_file velox/deps-install/.pangrova-folly-alignment
    require_file velox/deps-install/.pangrova-fbthrift-ready
    require_file velox/deps-install/lib/cmake/Snappy/SnappyConfig.cmake
    require_file velox/deps-install/lib/libsnappy.a
    require_file velox/deps-install/lib/cmake/AWSSDK/AWSSDKConfig.cmake
    [[ -d "$root/velox/.git" ]] || die "Velox source provenance checkout is absent"
    require_architecture native-deps/lib/libre2.a
    require_architecture velox/deps-install/lib/libfolly.a
    if [[ "$stage" == sdk ]]; then
        require_file native-deps/lib/libarrow_flight.dylib
        require_file velox/_build/release/lib/libvelox.a
        require_file velox/_build/release/lib/libvelox.a.pangrova-provenance
        require_file velox/_build/release/CMakeCache.txt
        require_architecture native-deps/lib/libarrow_flight.dylib
        require_architecture velox/_build/release/lib/libvelox.a
    fi
}

if [[ "$operation" == pack ]]; then
    verify_outputs
    mkdir -p "$bundle"
    fingerprint > "$bundle/identity.txt"
    directories=(native-deps velox)
    # Keep headers, source identity, static/shared libraries and generated
    # configuration. Compilation intermediates and download archives are not
    # inputs to the downstream Pangrova consumer; don't ship those large trees.
    tar -czf "$bundle/prefix.tar.gz" --exclude='*.o' --exclude='*.obj' \
        --exclude='velox/archive-cache' -C "$root" "${directories[@]}"
    (cd "$bundle" && shasum -a 256 prefix.tar.gz > prefix.sha256)
    du -h "$bundle/prefix.tar.gz"
else
    expected="$(fingerprint)"
    [[ -f "$bundle/identity.txt" && "$(cat "$bundle/identity.txt")" == "$expected" ]] \
        || die "Stage $stage revision, run, architecture or toolchain does not match"
    (cd "$bundle" && shasum -a 256 -c prefix.sha256)
    # Never merge a stage into another restored/partially built SDK tree.
    [[ ! -e "$root/native-deps" && ! -e "$root/velox" ]] \
        || die "Stage $stage requires a fresh dependency prefix"
    mkdir -p "$root"
    tar -xzf "$bundle/prefix.tar.gz" -C "$root"
    verify_outputs
fi
