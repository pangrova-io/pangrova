#!/usr/bin/env bash
set -euo pipefail

# Compare source, not a caller-provided tag: Java and JNI must share one checkout.
if [[ $# -ne 2 ]]; then
    echo "Usage: verify-native-source.sh <expected-checkout> <native-checkout>" >&2
    exit 1
fi
expected="$1"
native_checkout="$2"
for relative in \
    pangrova-protocol/src/main/proto pangrova-protocol/src/main/proto-imports \
    pangrova-protocol/src/main/resources/function-contracts pangrova-protocol/tools/function-contracts \
    pangrova-worker-native/src pangrova-worker-native/cmake pangrova-worker-native/bin \
    pangrova-worker-native/patches pangrova-worker-native/tools/function-contracts \
    pangrova-worker-native/CMakeLists.txt pangrova-worker-native/CMakePresets.json \
    pangrova-worker-native/jni_exported_symbols.map pangrova-worker-native/jni_exported_symbols_macos.list \
    pangrova-worker-native/patch_flight_types.py \
    pangrova-worker-native/lance-ffi/src pangrova-worker-native/lance-ffi/include \
    pangrova-worker-native/lance-ffi/.cargo pangrova-worker-native/lance-ffi/build.rs \
    pangrova-worker-native/lance-ffi/Cargo.toml \
    pangrova-worker-native/lance-ffi/Cargo.lock pangrova-worker-native/lance-ffi/rust-toolchain.toml; do
    diff -rq -x __pycache__ "$expected/$relative" "$native_checkout/$relative"
done
