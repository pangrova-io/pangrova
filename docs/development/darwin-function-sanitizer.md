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

## Qualification

The builder verifies that the private LLVM library is actually loaded and that
the emitted sanitizer IR contains `.alt_entry`. A valid function call must
succeed without a diagnostic; intentional wrong-function and heap-overflow
controls must both fail with their corresponding sanitizer diagnostic. Commands,
logs, source/patch hashes, SDK archive/tool inputs and output hashes remain in
the private output directory. A successful build includes `build-evidence.json`
and `canary-evidence.json`; directory presence alone is not qualification.

`pangrova-worker-native/test-tools/toolchain/FollyIntegerIntentControls.cpp` checks
the real Folly functions against independent 128-bit arithmetic, hash inversion,
timestamp recovery, all-bits masks and every rotation count 1..63; `--rotations-only`
runs only the rotation controls. Compile it standalone with the selected Folly
cohort's include/link inputs, ASan/UBSan flags and `-fno-sanitize-recover=all`; it is
outside GoogleTest discovery. It explains intentional integer operations without
altering Folly's algorithms or disabling checks.

This compiler fix is separate from the private compiler-rt TLS investigation in
[Darwin ASan runtime qualification](darwin-arm64-asan-runtime.md).

## Build the integer-corrected dependency

The checked-in patches target Folly v2026.01.05.00 and libc++ from LLVM
17.0.6. The builder copies the Folly source and libc++ headers into a new
directory. It verifies each affected file before and after applying the patch.
Use Python 3.9+, CMake, Ninja and the qualified private compiler from the
preceding steps. The dependency prefix list is the one used to build Velox.

```sh
python3 pangrova-worker-native/bin/build-darwin-asan-folly.py \
  --folly-source "$VELOX_HOME/deps-download/folly" \
  --compiler-prefix "$PANGROVA_HOME/llvm-function-prefix-17.0.6/toolchain" \
  --prefix-path "$VELOX_HOME/deps-install;$PANGROVA_HOME/homebrew" \
  --output "$PANGROVA_HOME/qualified-asan-folly" --jobs 6 \
  --openssl-root "$PANGROVA_HOME/homebrew/opt/openssl@3"
export PANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_HOME/qualified-asan-folly/install"
```

Pass `-DPANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_ASAN_FOLLY_PREFIX"` to the
Native ASan configure alongside the instrumented Protobuf and Abseil inputs.
The dependency builder checks that the compiler emits the corrected Mach-O
function metadata. The private libc++ include tree is used when compiling
Folly. The patches retain the enabled sanitizer
checks. Folly's 64-bit mixing uses equivalent modular multiplication, its
rotation masks the bits which would be discarded before the shift, and
timestamp packing selects the low 56 bits first. The all-bits futex mask is
written as the maximum unsigned value. F14 subtracts one explicitly after
testing the counter, and adds a positive delta as an unsigned value. libc++
keeps mixing indices, sums and updates in the required 32-bit word even when the caller supplies a 64-bit output range.
[Fixed Folly source version](https://github.com/facebook/folly/tree/v2026.01.05.00),
[fixed libc++ source version](https://github.com/llvm/llvm-project/blob/llvmorg-17.0.6/libcxx/include/__random/seed_seq.h),
[seed sequence recurrence](https://eel.is/c++draft/rand.util.seedseq).

## Qualification result

With only the compiler fix, the full macOS arm64 Native suite reports no ASan memory
errors or incorrect function types, but nine upstream Folly/libc++ integer sites
still emit diagnostics, so the repository's strict sanitizer scanner rejects the
run. With the integer-corrected Folly archive and libc++ headers above, the full
suite passes with the PostgreSQL/Lance fixture and the strict scanner accepts the
complete per-case log: zero integer, function-type or address diagnostics under the
repository's `detect_leaks=0` macOS profile. This does not qualify LeakSanitizer.

## Classification of the nine integer sites

The captured integer operations all have defined results for the reported operand
values. Each remains visible to the instrumented release
gate.

- Hash.h shifts a 64-bit unsigned mixing value by six. SpookyHashV2 uses
  64-bit rotations with counts from 1 through 63. DistributedMutex shifts
  the unsigned timestamp sentinel left by eight to discard its high byte.
  All observed shift counts are below the promoted operand width. The wide
  arithmetic controls exercise these bit-level results independently.
  [Shift expressions](https://eel.is/c++draft/expr.shift)
- Futex and both MemoryIdler sites supply a signed minus one as an unsigned
  32-bit wait or wake mask. The conversion selects all 32 bits.
  [Integral conversion](https://eel.is/c++draft/conv.integral),
  [Futex](https://github.com/facebook/folly/blob/v2026.01.05.00/folly/detail/Futex.h),
  [MemoryIdler](https://github.com/facebook/folly/blob/v2026.01.05.00/folly/detail/MemoryIdler.h)
- F14Table accepts a decrement only for a positive pending-insert counter.
  The signed minus one is converted to the unsigned counter width during
  addition, producing exactly the previous count less one.
  [F14Table](https://github.com/facebook/folly/blob/v2026.01.05.00/folly/container/detail/F14Table.cpp)
- libc++ seed_seq narrows a platform-width addition and a sum of output
  elements to a 32-bit mixing value. The standard requires this recurrence
  to run modulo 2 to the 32nd power.
  [Seed sequence algorithm](https://eel.is/c++draft/rand.util.seedseq),
  [libc++ implementation](https://github.com/llvm/llvm-project/blob/llvmorg-17.0.6/libcxx/include/__random/seed_seq.h)
