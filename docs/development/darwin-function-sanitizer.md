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

The initial replay passes all three controls. Rebuilding Folly with the fixed
compiler eliminates function-type findings in both independent CPU and IO
thread-pool reproducers using the original Apple linker. Those reproducers still
report the separately audited unsigned arithmetic and all-bits futex masks;
these are defined operations, not suppressed findings. Complete Native
qualification remains pending the actual rebuilt write/transport tests. Passing
the controls does not transfer an older Native result to the new Folly archive.

This compiler fix is separate from the private compiler-rt TLS investigation in
[Darwin ASan runtime qualification](darwin-arm64-asan-runtime.md).
