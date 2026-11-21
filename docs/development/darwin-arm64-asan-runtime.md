---
title: "Darwin arm64 ASAN Runtime"
description: "macOS arm64 AddressSanitizer runtime qualification"
---

# macOS arm64 ASAN runtime qualification

LLVM compiler-rt 17.0.6 reads TPIDRRO_EL0 but masks in the CPU-id bits rather
than clearing them; its arm64 TLS span is also zero. This can omit real dyld /
libc++ exception TLS roots and report a live exception allocation as leaked.
The private patch corrects the mask and covers macOS reserved plus public
pthread slots. It does not change detection flags or suppression files.

Sources: [LLVM 17.0.6 sanitizer_mac.cpp](https://github.com/llvm/llvm-project/blob/llvmorg-17.0.6/compiler-rt/lib/sanitizer_common/sanitizer_mac.cpp),
[Apple libpthread 485.60.2 TSD layout](https://github.com/apple-oss-distributions/libpthread/blob/libpthread-485.60.2/src/types_internal.h).
Apple source corroborates the layout; it is not asserted to be byte-identical to
this host's installed library. This patch is qualified for the tested macOS arm64
host only. It is not a general iOS, DriverKit or future macOS ABI guarantee.

## Private build

Set `CC`, `CXX` and `LLVM_CONFIG` to matching LLVM 17.0.6 tools. Set
`PANGROVA_RUNTIME_CMAKE` to CMake 3.28.x, then run:

```sh
bash pangrova-worker-native/bin/build-darwin-asan-runtime.sh /new/private/runtime-prefix
```

The script checks release archive and source SHA, applies only the reviewed
patch and builds arm64 ASAN in the private prefix. It does not install over a
compiler. To reuse already downloaded release archives, explicitly set
`PANGROVA_RUNTIME_SOURCE_ARCHIVES` to their directory; each archive is still
checked against its pinned SHA before use. Upstream CMake configuration deprecation diagnostics remain visible;
compilation warnings cause failure. Its 10.10 target is compiler-rt's upstream
runtime setting, not Pangrova's application deployment baseline.

## Calibration and loading

Compile `pangrova-worker-native/test-tools/toolchain/DarwinTlsRuntimeControls.cpp`
with matching clang++, `-std=c++20 -O1 -g -fsanitize=address
-fno-omit-frame-pointer -arch arm64 -mmacosx-version-min=13.0`. Run with the
private runtime directory in `DYLD_LIBRARY_PATH`; inspect `otool -L` and actual
`dladdr` / `DYLD_PRINT_LIBRARIES` output. A compiler with an absolute runtime
install name can require `install_name_tool -change` and ad hoc `codesign` on
this private calibration executable. Never rewrite installed binaries.

Use `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1` and no `LSAN_OPTIONS` for
calibration. Modes 0 and 1 must report no leaks while actual first/last public
pthread keys retain objects, then run real cleanup/destructors and join. This
host's last allocated public key is 767; a mismatch rejects the calibration.
Mode 2 clears a real owning key and must detect a 193-byte leak. Modes 3 and 4
must detect actual use-after-free and heap overflow with nonzero termination.
An abort is a valid sanitizer failure status; do not require exit code 1.

Also compile the unchanged `pangrova-worker-native/test-tools/toolchain/RuntimeErrorDiagnostic.cpp` with the
same flags and run its exception construction/copy/throw/rethrow and naturally
joined worker modes 0, 1, 2, 3, 4 and 8 at 1, 32 and 4096 iterations. Keep the original-runtime
results separately. Native CTest uses only the existing repository suppression,
unchanged. Check one actual runtime load, and capture source, native binaries,
cache, suppression and runtime SHA before and after every qualified cohort.

A qualified runtime matches all 23 calibration controls, then passes the complete
unfiltered Native CTest suite on the unchanged cohort. Build paths change the runtime
binary, so qualification belongs to one runtime SHA and source cohort; later Lance,
ANN or function-cohort changes require fresh Native builds and complete CTest runs.
