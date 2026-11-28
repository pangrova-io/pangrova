#!/usr/bin/env bash
# Carry only a completed same-run macOS dependency stage into a fresh runner.
set -euo pipefail

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
operation="${1:-}"
stage="${2:-}"
bundle="${3:-}"
root="${PANGROVA_HOME:-$HOME/.pangrova}"
case "$operation" in
    pack|restore) [[ $# == 3 ]] || die "Usage: $0 $operation core|sdk BUNDLE_DIR" ;;
    restore-cached) [[ $# == 3 && "$stage" == sdk ]] || die "Usage: $0 restore-cached sdk BUNDLE_DIR" ;;
    cache-key) [[ $# == 2 && "$stage" == sdk ]] || die "Usage: $0 cache-key sdk" ;;
    *) die "Unknown operation: $operation" ;;
esac
case "$stage" in core|sdk) ;; *) die "Unknown stage: $stage";; esac
[[ "$(uname -s)" == Darwin ]] || die "Native stage bundles are macOS only"
: "${GITHUB_RUN_ID:?missing GitHub run identity}"
: "${GITHUB_SHA:?missing revision identity}"
: "${GITHUB_WORKSPACE:?missing workspace path}"
: "${PANGROVA_LLVM_PREFIX:?missing selected compiler}"
: "${SDKROOT:?missing selected macOS SDK}"
: "${MACOSX_DEPLOYMENT_TARGET:?missing macOS deployment target}"

toolchain_identity() {
    printf '%s\n' "workspace=$GITHUB_WORKSPACE" "prefix=$root" "architecture=$(uname -m)" \
        "sdk=$SDKROOT" "compiler=$PANGROVA_LLVM_PREFIX" "deployment=$MACOSX_DEPLOYMENT_TARGET"
    sw_vers -buildVersion
    xcodebuild -version
    "$PANGROVA_LLVM_PREFIX/bin/clang++" --version
    LC_ALL=C shasum -a 256 "$PANGROVA_LLVM_PREFIX/bin/clang++"
    xcrun --sdk macosx --show-sdk-version
    brew list --versions openssl@3 libevent
}

same_run_identity() {
    printf '%s\n' 'pangrova-macos-native-stage-v3' "stage=$stage" \
        "run=$GITHUB_RUN_ID" "revision=$GITHUB_SHA"
    toolchain_identity
}

cache_identity() (
    export LC_ALL=C
    printf '%s\n' 'pangrova-macos-sdk-cache-v1' "runner-image=${ImageVersion:-unknown}"
    toolchain_identity
    cd "$GITHUB_WORKSPACE"
    git ls-files -z -- .pangrova/scripts/macos-native-stage.sh \
        .github/actions/setup-native-build .github/actions/setup-macos-native-cache \
        pangrova-worker-native/dependency-versions.txt pangrova-worker-native/arrow_unexported_symbols_macos.list \
        pangrova-worker-native/patch_flight_types.py pangrova-worker-native/patches \
        pangrova-worker-native/bin/setup-native-build-env.sh \
        pangrova-worker-native/bin/apply-velox-patches.sh \
        pangrova-worker-native/bin/isolate-arrow-macos-exports.py \
        pangrova-worker-native/bin/record-function-provenance.sh \
        pangrova-worker-native/bin/resolve-darwin-libevent.sh \
        pangrova-worker-native/bin/resolve-darwin-openssl.sh \
        pangrova-worker-native/bin/prepare-aws-sdk-source.sh \
        pangrova-worker-native/cmake/dependency-checks pangrova-worker-native/tools/function-contracts \
        pangrova-protocol/tools/function-contracts \
        pangrova-protocol/src/main/resources/function-contracts/execution-selection.json \
        | xargs -0 shasum -a 256
)

if [[ "$operation" == cache-key ]]; then
    cache_identity | shasum -a 256 | cut -d' ' -f1
    exit
fi

printf 'macOS Native stage %s %s on runner image %s\n' "$stage" "$operation" "${ImageVersion:-unknown}"

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
    same_run_identity > "$bundle/identity.txt"
    if [[ "$stage" == sdk ]]; then
        cache_identity > "$bundle/cache-identity.txt"
    fi
    exclusions=(--exclude='*.o' --exclude='*.obj')
    # The scheduled baseline publishes its source archive cache for diagnostics.
    # Ordinary SDK and test consumers require only installed and generated files.
    if [[ "$stage" == core && "${PANGROVA_MACOS_STAGE_ARCHIVES:-0}" == 1 ]]; then
        [[ -d "$root/velox/archive-cache" ]] || die "Required Velox source archives are absent"
    else
        exclusions+=(--exclude='velox/archive-cache')
    fi
    tar -czf "$bundle/prefix.tar.gz" "${exclusions[@]}" -C "$root" native-deps velox
    (cd "$bundle" && shasum -a 256 prefix.tar.gz > prefix.sha256)
    du -h "$bundle/prefix.tar.gz"
else
    [[ -f "$bundle/identity.txt" ]] || die "Stage $stage lacks its identity receipt"
    if [[ "$operation" == restore-cached ]]; then
        [[ -s "$bundle/cache-identity.txt" ]] || die "Cached SDK lacks its dependency identity"
        if ! cache_identity | diff -u "$bundle/cache-identity.txt" - >&2; then
            die "Cached SDK dependency inputs, runner image or toolchain do not match"
        fi
    elif ! same_run_identity | diff -u "$bundle/identity.txt" - >&2; then
        die "Stage $stage revision, run, architecture or toolchain does not match"
    fi
    (cd "$bundle" && shasum -a 256 -c prefix.sha256)
    # Never merge a stage into another restored/partially built SDK tree.
    [[ ! -e "$root/native-deps" && ! -e "$root/velox" ]] \
        || die "Stage $stage requires a fresh dependency prefix"
    mkdir -p "$root"
    tar -xzf "$bundle/prefix.tar.gz" -C "$root"
    verify_outputs
fi
