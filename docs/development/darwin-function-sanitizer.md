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
Folly's algorithm or disabling checks. This initial qualification covered 70
cases. The full reruns are recorded below.

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

## Full macOS arm64 regression, 2026-11-14

The Release CTest run passed all 4,350 cases in 196.33 seconds. The ASan/UBSan
test image linked with the existing Folly archive passed the same 4,350
assertions in 858.08 seconds. Its complete CTest log contained 2,511 sanitizer
diagnostics in 998 cases, including indirect-call type diagnostics from two
Folly locations. Assertion counts alone would have missed these findings.

A second full run replaced both original Folly archive references in the test
executable's link command with the same sources rebuilt using the qualified
Mach-O compiler fix. All other objects and the test definitions were reused.
With the matching real Postgres and Lance authority fixture, 4,350/4,350
passed in 487.08 seconds. No AddressSanitizer memory error or incorrect
indirect-call type was reported. All 11 NativeCommandExchangeTest cases were
free of sanitizer diagnostics. The full log still contained 2,378 integer
conversion or shift diagnostics across 998 cases at nine upstream locations.
This compiler-only cohort failed the repository's clean-sanitizer gate.

| Site | Diagnostics |
| --- | ---: |
| Folly Hash.h:167 / SpookyHashV2.h:131 | 947 / 406 |
| Folly Futex.h:90 | 603 |
| Folly MemoryIdler.h:110 / :148 | 162 / 214 |
| Folly DistributedMutex-inl.h:745 | 4 |
| Folly F14Table.cpp:43 | 24 |
| libc++ seed_seq.h:134 / :166 | 10 / 8 |

The SHA-256 of the complete per-case output was
`90cd377c8fe51aab1a95d55f528f8c4d090ea792ffd9e46d8634331804c52b6b`
for Release, `168dd3b79a968e75eefda8360f3cb3e15574a70e606d7203291443ee645856f4`
for the original ASan link and
`430d4e8c058d1432832e3f2c349ece479962f217976c28066ea294ecbf3cfe6f`
for the corrected ASan link. The rebuilt archive was
`6fda87fb3f9f45574c2378e7c4c17ed0c529aea4d4f49e213d467fecdc5883dc`.

### Rerun with the published JNI exception names

After rebuilding the JNI bridge with the current Java exception names, the
Release test binary passed 4,350/4,350 in 312.20 seconds. The rebuilt JNI
library, staged Worker resource and installed macOS arm64 classifier contained
the same 322,876,581-byte library, SHA-256
`75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929`.
The Release test image digest was
`d970d39607c57dceeaf6f117bc81d230d62265a0048b2ee7350ba6740d0db218`.
The complete Release per-case log digest was
`17e7e3a9c0d4f6d08de1a114fd84bfcfead8107cbcb87012c594919274968ea3`.

The ASan/UBSan test image linked with the corrected Folly archive had digest
`232632adc64a7d32c9efbb16a6eae470eb81d9c9338f521cbf0678fc268e2269`.
All 4,350 CTest assertions passed in 1,052.11 seconds. The 11 Native command
exchange cases produced zero sanitizer diagnostics. Across the full suite,
998 cases produced 2,360 integer diagnostics at the same nine locations.
The counts for Hash and SpookyHash were 946 and 406, for Futex 597, for
MemoryIdler 157 and 214, for DistributedMutex 2, for F14Table 24, and for
the two seed_seq operations 7 each. The per-case log contained zero
AddressSanitizer memory errors and zero incorrect-function-type diagnostics.
The repository's strict sanitizer check rejected the retained log. Its digest
was `15df7ebce6eb993a4095b1d82621ac396182503bb70181f72708da6dc3b7b549`.

### Source-corrected integer dependency

The qualified build used the same six affected Folly source files and one libc++
header as the pinned upstream tags. Each original SHA-256 was checked against the
upstream tag. The Folly archive built with the patched headers was 285,854,784
bytes, SHA-256 `89303c70e1dc8e413816e81cd39116a2e3af40208fa44dd20368c08271d3d0b2`.
The rebuilt test image linked that archive at both Folly entries in the preceding
ASan test link. Its digest was
`64bb317bf48302109cf4ea5df236275bc8f8ad9dc4c709c0c8d3781521d12597`.

Two real regression cases covered the nine historical locations between them:
`FilteredArtifactRetrievalExecutionTest.CorruptedColdImmutableObjectFailsTheActualNativeTask`
and `TaskExecutorStartTest.ProjectValuesWalksToLeaf`. They passed in 15.66 seconds
and the retained per-case output passed the strict sanitizer check. Its SHA-256
was `30f7dc5f5543d0b4bab0795da5d5830db2828a757d70cffc159d93d43a5c9cf4`.

The same image then passed all 4,350 discovered CTest cases in 1,043.17
seconds with the PostgreSQL and Lance fixture. The repository's strict
sanitizer scanner accepted the complete 11,816,868-byte per-case log,
SHA-256 `9f614f68b6c6321ef199608025dd26816cad4ec68f96c6532cb171ff22ede90b`.
It contained zero reported integer, incorrect-function-type or address
diagnostics. The macOS run used the repository's `detect_leaks=0` profile.

The numeric control was compiled with Folly's instrumentation options and
`-fno-sanitize-recover=all`. It checked 100,962 deterministic and boundary
values plus 4,141,053 rotations (digest 8360673614021038793), zero and
positive F14 counters, a guarded decrement and both 32- and 64-bit seed
outputs. The 68 generated words matched an unmodified LLVM 17.0.6 run.
Separate 64-bit output control exposed another implicit narrowing in the
same libc++ mixing loop; the patch selects the modulo-32-bit result
throughout that recurrence.

### Classification of the nine integer sites

The captured integer operations all have defined results for the operand
values reported in this run. Each remains visible to the instrumented release
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
