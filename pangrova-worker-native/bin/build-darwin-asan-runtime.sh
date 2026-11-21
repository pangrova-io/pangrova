#!/usr/bin/env bash
# Builds a private compiler-rt 17.0.6 runtime. Never installs over the compiler.
set -euo pipefail
export LC_ALL=C

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ $# -eq 1 ]] || fail "Usage: $0 /new/private/output-prefix"
[[ $(uname -s) == Darwin && $(uname -m) == arm64 ]] || fail "Requires macOS arm64"
: "${CC:?Set CC to the matching clang 17.0.6 compiler}"
: "${CXX:?Set CXX to the matching clang++ 17.0.6 compiler}"
: "${LLVM_CONFIG:?Set LLVM_CONFIG to its llvm-config}"
cmake_command=${PANGROVA_RUNTIME_CMAKE:-cmake}
[[ $("$LLVM_CONFIG" --version) == 17.0.6 ]] || fail "Requires LLVM 17.0.6"
"$CC" --version | head -1 | grep -q '17.0.6' || fail "CC version mismatch"
"$CXX" --version | head -1 | grep -q '17.0.6' || fail "CXX version mismatch"
[[ $("$cmake_command" --version | head -1) == 'cmake version 3.28.'* ]] ||
    fail "Use CMake 3.28.x; upstream 17.0.6 uses old CMake policies"
[[ $1 == /* && ! -e $1 ]] || fail "Output must be a new absolute path"
mkdir -p "$1"
runtime_prefix=$(cd "$1" && pwd)
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

fetch_source() {
    local component=$1 expected=$2 archive="$runtime_prefix/$1-17.0.6.src.tar.xz"
    if [[ -n ${PANGROVA_RUNTIME_SOURCE_ARCHIVES:-} ]]; then
        cp "$PANGROVA_RUNTIME_SOURCE_ARCHIVES/$component-17.0.6.src.tar.xz" "$archive"
    else
        curl --fail --location --retry 2 --connect-timeout 15 --max-time 180 \
            "https://github.com/llvm/llvm-project/releases/download/llvmorg-17.0.6/$component-17.0.6.src.tar.xz" \
            --output "$archive"
    fi
    [[ $(shasum -a 256 "$archive" | cut -d ' ' -f 1) == "$expected" ]] || fail "$component SHA mismatch"
    tar -xf "$archive" -C "$runtime_prefix"
}
fetch_source compiler-rt 11b8d09dcf92a0f91c5c82defb5ad9ff4acf5cf073a80c317204baa922d136b4
fetch_source cmake 807f069c54dc20cb47b21c1f6acafdd9c649f3ae015609040d6182cab01140f4
fetch_source llvm b638167da139126ca11917b6880207cc6e8f9d1cbb1a48d87d017f697ef78188
ln -s llvm-17.0.6.src "$runtime_prefix/llvm"
ln -s cmake-17.0.6.src "$runtime_prefix/cmake"
source_dir="$runtime_prefix/compiler-rt-17.0.6.src"
mac_source="$source_dir/lib/sanitizer_common/sanitizer_mac.cpp"
[[ $(shasum -a 256 "$mac_source" | cut -d ' ' -f 1) == \
    ace9026a7612d8da2448dce61b80ee2aeb1de335103b912e335119d3b26ba5b2 ]] || fail "Source mismatch"
patch --batch --forward -d "$source_dir" -p1 < \
    "$script_dir/../patches/compiler-rt-17-darwin-arm64-tls.patch"
[[ $(shasum -a 256 "$mac_source" | cut -d ' ' -f 1) == \
    deeb6da0fff41746a8edfaa6d31aab4b790abe0d538e1aa447b0957a09cec0a5 ]] || fail "Patched SHA mismatch"

"$cmake_command" -S "$source_dir" -B "$runtime_prefix/build" -G 'Unix Makefiles' \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER="$CC" -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_C_COMPILER_TARGET=arm64-apple-darwin23.0.0 \
    -DCMAKE_CXX_COMPILER_TARGET=arm64-apple-darwin23.0.0 \
    -DCMAKE_OSX_ARCHITECTURES=arm64 -DCMAKE_OSX_DEPLOYMENT_TARGET=10.10 \
    -DDARWIN_osx_ARCHS=arm64 -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
    -DLLVM_CONFIG_PATH="$LLVM_CONFIG" -DLLVM_MAIN_SRC_DIR="$runtime_prefix/llvm" \
    -DCOMPILER_RT_INCLUDE_TESTS=OFF -DCOMPILER_RT_BUILD_SANITIZERS=ON \
    -DCOMPILER_RT_SANITIZERS_TO_BUILD=asan -DCOMPILER_RT_BUILD_BUILTINS=OFF \
    -DCOMPILER_RT_BUILD_XRAY=OFF -DCOMPILER_RT_BUILD_LIBFUZZER=OFF \
    -DCOMPILER_RT_BUILD_PROFILE=OFF -DCOMPILER_RT_BUILD_MEMPROF=OFF \
    -DCOMPILER_RT_BUILD_ORC=OFF -DCOMPILER_RT_BUILD_GWP_ASAN=OFF \
    -DCOMPILER_RT_ENABLE_IOS=OFF -DCOMPILER_RT_ENABLE_WATCHOS=OFF \
    -DCOMPILER_RT_ENABLE_TVOS=OFF -DCMAKE_INSTALL_PREFIX="$runtime_prefix/install" \
    2>&1 | tee "$runtime_prefix/configure.log"
"$cmake_command" --build "$runtime_prefix/build" --target clang_rt.asan_osx_dynamic \
    --parallel "${PANGROVA_RUNTIME_BUILD_JOBS:-3}" 2>&1 | tee "$runtime_prefix/build.log"
if grep -Eq '(^| )warning:' "$runtime_prefix/build.log"; then fail "Compiler warnings in build.log"; fi
runtime="$runtime_prefix/build/lib/darwin/libclang_rt.asan_osx_dynamic.dylib"
[[ -f $runtime ]] || fail "Runtime not produced"
shasum -a 256 "$runtime" "$mac_source" > "$runtime_prefix/artifact-sha256.txt"
printf 'Private runtime: %s\nRun calibration controls before Native validation.\n' "$runtime"
