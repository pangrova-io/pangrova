---
title: "Darwin Function Sanitizer"
description: "Darwin LLVM function sanitizer qualification"
---

# Darwin LLVM function sanitizer qualification

LLVM 17's function sanitizer emits a signature and type hash before each
instrumented function. On Mach-O, those bytes lack a binding to the actual
function entry; subsection coalescing can separate them. A valid Folly callback
then receives another function's hash and reports an incorrect function type.
The fix is upstream LLVM commit
[`c23135c5488fbfaf6439433a10b3ddef33ff112c`](https://github.com/llvm/llvm-project/commit/c23135c5488fbfaf6439433a10b3ddef33ff112c):
associate the metadata with a private symbol and mark the real function as an
alternative entry. Switching linkers alone did not resolve the complete Native
reproducer.

## Private SDK build

The builder supports the tested macOS arm64 Homebrew LLVM 17.0.6 Release SDK.
It backports the upstream emission fix without changing the installed compiler,
runtime or sanitizer flags. Python 3.11+ and the SDK's static archives are needed:

```sh
python3 pangrova-worker-native/bin/build-darwin-llvm-function-prefix.py \
  --llvm-prefix "$PANGROVA_HOME/llvm" --output /new/private/compiler-prefix
```

All 20 source inputs from LLVM 17.0.6 are SHA-256 pinned in
`pangrova-worker-native/patches/llvm-17-function-prefix-sources.json`.
`--source-cache /path/to/original/files` replaces network access with an explicit
local source directory; the same hashes are required. The output must be new.
The script builds only AsmPrinter, replaces that object in a private archive,
and relinks LLVM using the installed SDK's archives and actual dynamic library
dependencies. It excludes TableGen as upstream llvm-shlib does. The installed
SDK omits private `config.h`; this translation unit uses the public SDK config
header and explicit matching Homebrew package name/version/vendor macros.

The result is `toolchain/bin/clang` and `clang++`. Build the isolated ASan Folly
with these compiler paths, retaining the selected dependency cohort's CPU/ABI
flags and `FOLLY_LIBRARY_SANITIZE_ADDRESS=ON`. On macOS, continue passing
`pangrova-worker-native/cmake/AsanFollyCompat.cmake` as
`CMAKE_PROJECT_folly_INCLUDE`; this existing compatibility setting concerns the
unexported libc++ memory-resource vptr metadata and is separate from this fix.
Configure Pangrova with that Folly install as `PANGROVA_ASAN_FOLLY_PREFIX`.

## Qualification and evidence

The builder verifies that the private LLVM library is actually loaded and that
the emitted sanitizer IR contains `.alt_entry`. A valid function call must
succeed without a diagnostic; intentional wrong-function and heap-overflow
controls must both fail with their corresponding sanitizer diagnostic. Commands,
logs, source/patch hashes, SDK archive/tool inputs and output hashes remain in
the private output directory. A successful build includes `build-evidence.json`
and `canary-evidence.json`; directory presence alone is not qualification.

The final builder replay passes all three controls. Rebuilding Folly with the fixed
compiler eliminates function-type findings in both independent CPU and IO
thread-pool reproducers using the original Apple linker. Those reproducers still
report the separately audited unsigned arithmetic and all-bits futex masks;
these are defined operations, not suppressed findings. The rebuilt Native cohort
passes the two original write/TLS reproductions, then 68 other affected
write/JNI/TLS/HMAC/exchange cases, for a disjoint rolling union of 70/70. Both
retained CTest logs contain zero incorrect-function diagnostics and zero ASan
memory errors. The ASan dylib's 23 exported symbols exactly match Release.

The expanded run additionally reports the unsigned timestamp truncation in
`DistributedMutex-inl.h`: `strip` retains 56 timestamp bits and reserves the
bottom eight bits; its caller deliberately applies it to the maximum unsigned
timestamp sentinel. `pangrova-worker-native/test-tools/toolchain/FollyIntegerIntentControls.cpp`
checks the real Folly functions against independent 128-bit arithmetic,
hash inversion, timestamp recovery and all-bits masks. It passes 100,962
boundary/deterministic inputs, including all upper-byte values and the actual
maximum-timestamp sentinel. Compile this standalone control with the selected
Folly cohort's include/link inputs and ASan flags; it is outside GoogleTest
discovery. It explains the intentional integer operations without altering
Folly's algorithm or disabling checks. Full final Native regression remains
pending; 70 cases are not the full discovered suite.

Local evidence is `/tmp/pangrova-llvm-prefix-native-first-two-20261021.json`,
`/tmp/pangrova-llvm-prefix-native-remaining-evidence-20261021.json`, and
`/tmp/pangrova-folly-integer-intent-{command,build,run}-20261021.{json,log}`
(command is JSON; build/run are logs). Complete per-test logs remain in each
runner-owned fixture directory recorded by those JSON files.

The later Iceberg entry projection cohort passes 22/22 matching Release and
ASan cases, retaining 66 optional integer diagnostics at `Hash.h:163/167` and
`SpookyHashV2.h:131`. The new site is `Rot64` on unsigned 64-bit values; every
rotation constant used by the hash is in 1..63. The tracked standalone control
now compares that real function with 128-bit multiplication/modulo/division for
4,141,053 rotations, including every shift 1..63 and the observed input. Its
`--rotations-only` mode permits incremental qualification without repeating the
earlier hash/mutex controls. This control passes with address and undefined
sanitizers, with no check-disabling flags. The Native cohort keeps its original
checks and full diagnostics; it has zero ASan memory errors and zero incorrect
function-type reports, and is not described as a zero-diagnostic UBSan run.
Evidence: `/tmp/pangrova-nested-entry-native-qualified-20261021.json` and
`/tmp/pangrova-folly-rotation-controls-{command,build,run}-20261021.{json,log}`.

This compiler fix is separate from the private compiler-rt TLS investigation in
[Darwin ASan runtime qualification](darwin-arm64-asan-runtime.md).
