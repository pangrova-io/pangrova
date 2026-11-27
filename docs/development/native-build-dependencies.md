---
title: "Native Build Dependencies"
description: "How the C++ and Rust dependency chain is pinned, provisioned, cached, and diagnosed in local builds and CI"
---

# Native Build Dependencies

The native runtime links a long dependency chain
(abseil → protobuf → RE2 → gRPC → Velox, plus the Lance Rust FFI). Getting that
chain to build the same way locally and on every CI runner is the largest source
of native job failures, so the repository treats it as an explicit contract
rather than a set of ad hoc commands.

## Ownership

| Concern | Owner |
|---|---|
| Tool prerequisites and build sequence | `pangrova-worker-native/bin/setup-native-build-env.sh` |
| Sanitizer rebuild of the same sources | `pangrova-worker-native/bin/setup-native-asan-env.sh` |
| Rust channel | `pangrova-worker-native/lance-ffi/rust-toolchain.toml` |
| Velox revision and dependency versions | the variables at the top of the bootstrap script |
| CI provisioning and caching | `.github/actions/setup-native-build` and the native jobs in `ci.yml` |
| Pinned-version policy | `.pangrova/rules/build-pipeline.md` |

Changing a pin means changing the variable, the Rust file when applicable, and
the CI cache key inputs; the cache key already hashes both scripts.

## The Contract

`setup-native-build-env.sh --check-tools` is the single place that defines what a
usable machine looks like. It verifies:

- CMake and Ninja, and that CMake can find its own Modules directory.
- Git, Rust (`cargo`, `rustc`) and the GNU autotools (`autoconf`, `automake`,
  `libtoolize` on Linux, `glibtoolize` on macOS), plus `pkg-config` and
  `ripgrep` (`rg`), which hashes the Lance SDK source tree.
- On Linux, system OpenSSL and zlib development headers, because gRPC is built
  before Velox's own `setup-ubuntu.sh` runs.
- On macOS, the Xcode command-line tools, Homebrew, and a Clang 15+ toolchain.

CI never installs these ad hoc. Both native jobs call
`.github/actions/setup-native-build`, which installs the same set on Ubuntu or
macOS, installs rustup and the pinned Rust channel, and then runs
`--check-tools`. A missing tool fails in seconds with its name instead of after
half an hour of compilation. Tool verification does not build dependencies or
write `env.sh`: both CI and macOS release jobs run the full bootstrap afterwards,
before Lance FFI and Pangrova's CMake configure.

The bootstrap supports the system Bash on macOS, including Bash 3.2. Optional
CMake argument arrays use guarded expansion so that an empty array passes zero
arguments under `set -u`, while populated arrays preserve paths containing spaces.

CI uses Ubuntu 24.04 and GCC 13 for both Linux architectures and the sanitizer
jobs, keeping the compiler compatible with both the pinned Velox sources and the
distribution's C++ libraries. It links a small static gflags consumer before
building dependencies to catch ABI mismatches early. After a failed Native
build, CI prints Ninja's JNI library link command to show the selected inputs.
On macOS, CI selects an installed Homebrew LLVM of version 15 or newer,
preferring the existing `llvm`
(or newest installed `llvm@*`) formula instead of installing a possibly
unavailable Intel bottle; only a machine without a suitable LLVM installs
`llvm@17`. The selected prefix is exported as `PANGROVA_LLVM_PREFIX`. Without an
explicit prefix, the bootstrap uses `~/.pangrova/llvm` or Homebrew's default
LLVM. The bootstrap resolves `SDKROOT` with `xcrun` when needed and records it
in `env.sh`: CMake then treats the SDK headers as implicit system headers
instead of placing them ahead of libc++ while building gRPC. CI compiles a small
zlib consumer to verify that header ordering before building the dependencies.
The bootstrap uses the macOS 13.0 deployment target by default, matching the
packaged Lance C FFI. With LLVM 18 or newer it pairs the selected Clang driver
with the Xcode SDK's libc++ headers and links against the macOS system runtime.
It verifies exception creation and rethrow before building dependencies.
The generated driver is shared by Velox, its dependencies and the JNI build.
Same-run macOS receipts include the compiler checksum and deployment target.
The same check requires the Homebrew Bison executable to satisfy Velox's
minimum version of 3.0.4. Xcode provides Bison 2.3, so CI installs the Homebrew
keg when its executable is missing. CI also provisions the Flex keg and compiles
against its `FlexLexer.h`, which Velox's generated C++ scanner includes.
The macOS bootstrap selects Homebrew `openssl@3` before building gRPC and the
Velox dependencies. It records the same root in `env.sh` so CMake links the
Native function exporter, tests, and JNI library against OpenSSL 3. gRPC uses
the OpenSSL ENGINE API, which was removed in OpenSSL 4.
The bootstrap passes the OpenSSL root directly to fbthrift because fbthrift's
CMake wrapper otherwise queries Homebrew's default formula. It rebuilds
cached fbthrift packages whose install stamp predates this check.
macOS CI reuses preinstalled tools and installs only a missing tool without
upgrading existing Homebrew formulae. It keeps Homebrew's download cache:
Intel macOS 15 is a Tier 3 Homebrew configuration where several formulae have
no bottles, so deleting that cache turns a reusable download into a build
failure. ccache is used only when already available.

## Why the Order Matters

`setup-native-build-env.sh` builds abseil, protobuf, RE2 and gRPC into a
hermetic prefix before Velox. Velox's `setup-ubuntu.sh` installs the remaining
distro packages and a package-managed CMake later. Two consequences are easy to
miss:

- gRPC's configure needs OpenSSL and zlib headers that `setup-ubuntu.sh` would
  have installed afterwards, so CI installs them up front.
- The gRPC library builds use the generated protocol sources in the pinned
  checkout. Both the standard and ASan builds disable gRPC's optional
  configure-time archive downloads.
- The package-managed CMake can shadow the system one without a Modules
  directory, so both bootstraps validate the CMake found at startup and put its
  directory back in front of `PATH` before configuring Velox.

The bootstrap enforces these dependency contracts.

- Ubuntu's packaged static gflags archive is not position-independent. Linux
  development and sanitizer builds link the shared library, including Folly's
  configuration-specific CMake imports and absolute archive paths in imported
  interfaces. The production image builds Velox's pinned gflags version as
  PIC and links its static archive from the production dependency prefix.
- The Linux production build passes PIC flags to the pinned Boost B2 build.
  The bootstrap verifies the Filesystem and Atomic archives with a shared-library
  link before compiling Velox.
- fbthrift's CMake requires an Xxhash package that Velox's dependency set does
  not install on macOS; the bootstrap builds and installs it before fbthrift.
- Fizz and Folly developer examples are disabled during their dependency
  builds: Fizz's `BogoShim` needs libc++abi symbols that macOS 13 lacks, and
  Velox consumes the libraries, not those tools.
- The pinned Folly SpookyHash short-tail path uses alignment-safe loads in
  the regular, production and instrumented builds. The byte order and hash
  values stay compatible with the pinned upstream version. The Darwin
  bootstrap rebuilds a cached Folly archive when its alignment-patch digest
  is missing or differs from the reviewed patch.

## Sanitizer Dependency Chain

Flight shuffle copies misaligned binary offset buffers before Arrow accesses
them and retains the page payload buffers. Together with the Folly short-tail
patch, this keeps both paths covered by the existing sanitizer presets.

AddressSanitizer needs matching instrumentation across the runtime and the
libraries whose headers participate in its memory access. The
[LLVM ASan guide](https://clang.llvm.org/docs/AddressSanitizer.html) requires
sanitizer flags at compilation and final linkage, and warns that partially
instrumented container code can produce false positives.

The ASan job first runs the regular bootstrap with
`PANGROVA_NATIVE_BUILD_PHASE=dependencies`. This fetches the pinned SDK sources and
prepares dependencies without compiling Release Velox or Release Arrow Flight.
`setup-native-asan-env.sh` then rebuilds Abseil, protobuf, RE2, gRPC, Folly, Velox
and Arrow Flight into separate ASan prefixes and build directories. Folly uses C++20 and its upstream
`FOLLY_LIBRARY_SANITIZE_ADDRESS` option. The generated `asan-env.sh` records
`PANGROVA_ASAN_FOLLY_PREFIX`, which CI passes to Pangrova's CMake configure.
On macOS it also records the OpenSSL 3 root used to build those dependencies.
An explicitly supplied Folly prefix is consumed rather than overwritten, allowing
the separately qualified Darwin build described in the sanitizer guide.

Arrow's bundled resolver is selected explicitly, and its patch-step target is
exported so the checksum-verified source is prepared before building Velox. Its
ExternalProject is a separate CMake invocation, which does not inherit parent
compiler or sanitizer flags automatically. The reviewed
`velox-arrow-build-flags.patch` forwards the C/C++ compilers, compilation flags,
Release optimization flags and executable/shared linker flags. This keeps the
bundled Arrow build instrumented as well as the later Arrow Flight build.

Pangrova checks Folly's headers and archive instrumentation regardless of whether
Velox dependencies came from `deps-install` or a system prefix. Missing or
uninstrumented Folly is an error, not a reason to fall back to Release. The
`Folly::folly` CMake target carries the dependency's sanitizer runtime flags;
linking only its archive would lose the UBSan runtime that Folly also enables.
Release Folly archives found in other dependency directories are excluded from
ASan linkage. The gflags CMake package is imported before Folly because the pinned
Linux package exports a `gflags_static` target reference rather than a library
filename. Non-production Linux builds also consume Folly's installed package
instead of assuming that it was installed beneath Velox's `deps-install`.

Folly's ASan option also enables UBSan. GCC 13 rejects the pinned socket API
address assertions with UBSan enabled. The reviewed
`folly-2026.01.05-socket-function-types.patch` checks that `recvmmsg` and `sendmmsg`
are declared functions without requiring their addresses to be constant
expressions. Runtime socket calls and both sanitizers remain enabled.

GCC 13 also enables `-Wdangling-reference` under `-Wall`/`-Werror`. Helpers
that return references must not receive temporary `std::string` arguments, so
the retrieval helpers take `std::string_view`, the storage contract hoists
computed option names into named locals, and the generated capability
projection header follows the same rule through `physical_type_manifest.py`.
The connector header includes `<cstddef>` and `<cstdint>` directly instead of
relying on transitive includes from each translation unit.

Velox compiles with a CPU baseline chosen from its own scripts
(`-msse4.2`/`-mavx2` on x86, `-march=armv8-a+crc+crypto` on ARM) and records it
only in the build rules, not the cache. Pangrova asks the same helper for that
baseline and applies it to every consumer of Velox SIMD headers; without it,
`xsimd::default_arch` falls back to `sse2` and `crc32U64` has no implementation.

Native and sanitizer jobs prepare the Lance Java fixture before compiling
Native dependencies. They restore the fixture's Maven dependencies, publish
the pinned Iceberg SDK locally, and build the broker from the checked-out
reactor. The Maven dependency cache is saved as soon as the broker is ready.
CTest uses that broker in the same checkout. Local test runs prepare it at
startup unless given `--prepared-java` with a directory produced by
`--prepare-java`.

Velox's own dependency build also compiles Folly in `velox/deps-download`. On
macOS with OpenSSL 3.6 or newer, Folly's non-const assignments from
`X509_get_subject_name`, `X509_EXTENSION_get_object`, `X509_get_ext` and
`X509_EXTENSION_get_data` no longer compile, and its example/benchmark targets
fail to link on Intel macOS. The bootstrap applies the reviewed Folly source
patches (ASN.1 accessors, the OpenSSL 3.6 const results in `OpenSSLUtils.cpp`
and `OpenSSLCertUtils.cpp`, and the pre-libc++ 18 exception-construction path in
`Exception.cpp`) before configure, and patches the Wangle and Fizz const results
(`SSLUtil.cpp`, `CertUtil.h`) right after their sources are extracted,
disables its examples and benchmarks, and sets `PROMPT_ALWAYS_RESPOND=n` so the
pinned dependency sources are reused instead of re-downloaded on non-interactive
runners.
The exception patch is a source change because libc++ redefines its own
availability macro, so a `-D` override is ignored and the newer
`__cxa_init_primary_exception` symbol would remain unresolved before macOS 14.
macOS builds pin Homebrew `openssl@3` because vendored code uses APIs removed
in OpenSSL 4. The Arrow 18 bootstrap disables Thrift and the ArrowTesting
targets. The production Velox build uses Arrow IPC and Flight without linking
ArrowTesting, which requires a Boost.Process target from the Thrift path.

On AArch64 Linux the canonical native library exceeds the +/-128 MiB branch
range of `R_AARCH64_CALL26`, so GNU ld cannot reach outline-atomics helpers that
Rust and C dependencies place inside their static archives. Pangrova builds
embedded dependencies (OpenTelemetry, simdjson) as position-independent code and
selects `ld.lld` when it is installed; lld inserts range-extension thunks and the
link also passes `--undefined-version` because the JNI version script names
optional entry points. CI installs `lld` on Linux for this purpose.

When Velox resolves the AWS SDK, Thrift, stemmer or the s2n archive from one
dependency prefix, native builds link them beside the AWS CRT: Linux wraps the
static AWS archives in a linker rescan group and appends the s2n, stemmer and
Thrift archives plus OpenSSL/curl after the group. The Thrift search first uses
Velox's Arrow ExternalProject install and falls back to the AWS dependency
prefix, so builds whose Velox used dependency sources instead of the
ExternalProject layout still link.

The macOS bootstrap installs the version of Snappy pinned by Velox into its
dependency prefix. Velox uses that prefix's CMake package for the Hive/DWIO
codecs, and the JNI link consumes the static archive from the same prefix.

On macOS, Folly's libevent search only covers `/usr/local`, `/opt/local` and
`$HOME`, not Homebrew's `/opt/homebrew` prefix. The bootstrap resolves the
formula's `opt` prefix and adds it to Folly's configure. OpenSSL 4.0 makes
`ASN1_STRING` opaque (OpenSSL 3.6 still exposes the structure); the reviewed
`folly-2026.01.05-openssl-asn1.patch` uses the stable
`ASN1_STRING_get0_data`/`ASN1_STRING_length` accessors instead of direct fields.
Velox and Folly do not use Boost.Log, and its ASIO backend fails to link against
the SDK libc++ with Homebrew Clang 18 on Intel, so the pinned Boost build uses
`--without-log` to omit both `boost_log` and `boost_log_setup` while keeping every
component Velox declares. `log_setup` is a target within the `log` component, not a
valid B2 component selector.

Direct and sanitizer builds use the LZ4 codeload archive for Arrow Flight
when no `ARROW_LZ4_URL` override is provided. Arrow checks the fixed archive
SHA-256. The production Docker builder preseeds the same verified archive.

## Disk and Cache Budget

A cold dependency and Velox tree is roughly 30 GB. A standard Ubuntu or macOS
runner ships about 14 GB free and the repository cache limit is 10 GB, so:

- CI removes preinstalled toolchains the build never uses (Android SDK, .NET,
  CodeQL, iOS simulator runtimes) before downloading anything, and fails fast
  when less than 20 GB is free.
- Only the dependency archives, the `~/.pangrova/native-deps` and
  `~/.pangrova/velox/deps-install` prefixes, the Velox build layer
  (`_build/release/_deps`, `lib`, `CMakeCache.txt`) and the bounded ccache are
  cached. The whole `~/.pangrova/velox` tree is never cached: it cannot fit, and
  the bootstrap verifies every pinned dependency revision so a partial restore
  is rejected rather than trusted.
- Cache keys are layered, and `pangrova-worker-native/dependency-versions.txt`
  is their single input lock. Archives key on that lock only; prefixes add the
  toolchain action and `pangrova-worker-native/patches/*`; the Velox layer adds
  the function-selection contract; ccache is keyed per commit with a restore
  prefix, so a patch edit recompiles only the patched translation units. Bump
  `PANGROVA_DEPENDENCY_BUILD` in the lock whenever dependency build flags or
  install options change. Keys never fall back to another image, compiler
  configuration or pre-patch dependency tree.
- `.github/workflows/native-deps-baseline.yml` warms the layered keys weekly
  and when the dependency lock changes. Each Linux warm job can publish
  its installed `~/.pangrova/native-deps` prefix as an OCI image to GHCR
  when the repository variable
  `PANGROVA_NATIVE_DEPS_IMAGE=ghcr` is set and package write permission is
  granted. Linux CI extracts that prefix when the variable is set.
  The Velox system dependency setup still runs on the consumer. The
  baseline workflow uses the same two macOS SDK build stages as CI and
  publishes the macOS dependency archive for diagnostics and separate
  qualification.
- Hosted macOS CI separates canonical and Velox prerequisite packages,
  Velox/Arrow Flight SDK construction and the Native build/test into three
  jobs per selected architecture using a per-platform reusable workflow.
  Linux platform builds have no macOS-stage dependency. The six-hour
  **per-job** limit must not put cold dependency builds and Native tests in
  the same time budget. Each consumer
  checks the workflow run, source revision, architecture, macOS build,
  compiler, SDK and linked Homebrew versions. It records the runner image
  and verifies the archive checksum and required outputs before using it. The SDK artifact retains the source provenance checkout,
  headers, generated configuration and libraries, but excludes object files
  and download archives. Artifacts are short-lived, not cross-run dependency
  caches; partitioning adds transfer overhead. macOS no longer restores an
  optional last-successful baseline artifact in CI.
- macOS explicitly installs and verifies ccache and exports its compiler
  launchers to every CMake invocation. Stage-specific compiler cache keys
  preserve completed work even when a later stage fails. The Native job
  still runs all of its original build, JNI, Docker provisioning, CTest and
  packaging steps. A successful CTest without a report is an error; if the
  build never reaches tests, the report publisher does not manufacture a
  second, misleading missing-file failure.


## Apache Gluten Reference

Gluten's published [CI guide](https://apache.github.io/gluten/backends/velox/developers/velox-backend-CI.html),
[Docker build guide](https://apache.github.io/gluten/backends/velox/developers/velox-backend-build-in-docker.html)
and [image inventory](https://apache.github.io/gluten/backends/velox/developers/velox-backend-docker-images.html)
describe a useful separation of dependencies, revision-specific binaries and
runtime qualification. These are documented upstream practices, not a claim that
Pangrova has reproduced Gluten's CI results.

| Gluten practice | Pangrova application and current boundary |
|---|---|
| Prebuilt toolchain/dependency images; static builds restore vcpkg binary packages from `/var/cache/vcpkg` | Reuse dependency preparation, not an unrelated prebuilt Velox library. Pangrova retains its pinned protobuf/gRPC/RE2 cohort and SDK patches; Gluten's binary cache is not interchangeable with it. A published Pangrova dependency image is not yet wired into CI. |
| Build environments contain dependency caches, not Gluten or Velox source/binaries | Keep dependencies separate from the revision-specific SDK and JNI build. The ASan job now prepares dependencies without first compiling Release Velox and Flight. |
| One same-revision Native artifact feeds multiple JVM and TPC jobs | Pangrova already shares classifier artifacts with JDK 17/21 jobs. Its static production image and sanitizer variants have different build contracts and must not reuse a development classifier as a substitute. |
| Runtime images contain no compiler, Velox or Arrow installation | Keep production-image acceptance separate from the build environment so installed development libraries cannot mask missing runtime dependencies. |
| Images are refreshed on Docker input changes and a schedule; mutable tags can be reproduced by digest | Preserve digest-pinned production base images and exact source identity. Future dependency-image reuse must also bind compiler, architecture, dependency revisions, patches and sanitizer mode. |

Gluten's static-build guide specifies 64 GB of memory and its image guide notes
that dependency images can take hours to build. Caching amortizes that work; it
does not make a cold build free or prove that a smaller runner is sufficient.
Pangrova's memory-aware job limits are safeguards, not evidence of cold-build
capacity. Four-platform builds, sanitizer runs and same-source production
regression remain the acceptance criteria.

## Diagnosing Failures

| Symptom | Cause and fix |
|---|---|
| `cargo not found` in seconds | Rust toolchain missing; install rustup and let `rust-toolchain.toml` select the channel |
| `OpenSSL` / `zlib` not found during gRPC | Install `libssl-dev` and `zlib1g-dev` before the bootstrap |
| `Could not find CMAKE_ROOT` | A package-managed CMake shim shadows the system one; re-run through the bootstrap, which re-pins the verified binary |
| Build dies with `No space left on device` | Free runner disk or move `PANGROVA_NATIVE_BUILD_DIR` to a larger volume |
| Cache restore succeeds but the build still recompiles | The cache does not include the Velox build tree by design; only the install prefixes are cached |
| Sanitizer job reports findings in passing tests | The runner rejects any recoverable sanitizer diagnostic even when CTest succeeds; read `ctest.log` |
