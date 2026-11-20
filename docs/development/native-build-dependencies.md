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
building dependencies to catch ABI mismatches early. On macOS, CI selects an
installed Homebrew LLVM of version 15 or newer, preferring the existing `llvm`
(or newest installed `llvm@*`) formula instead of installing a possibly
unavailable Intel bottle; only a machine without a suitable LLVM installs
`llvm@17`. The selected prefix is exported as `PANGROVA_LLVM_PREFIX`. Without an
explicit prefix, the bootstrap uses `~/.pangrova/llvm` or Homebrew's default
LLVM. The bootstrap resolves `SDKROOT` with `xcrun` when needed and records it
in `env.sh`: CMake then treats the SDK headers as implicit system headers
instead of placing them ahead of libc++ while building gRPC. CI compiles a small
zlib consumer to verify that header ordering before building the dependencies.
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
- The package-managed CMake can shadow the system one without a Modules
  directory, so both bootstraps validate the CMake found at startup and put its
  directory back in front of `PATH` before configuring Velox.

## Sanitizer Dependency Chain

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

Native test runs build the Java fixture with the project reactor, which
resolves the locally published Iceberg candidate. CI installs that candidate
through the shared Iceberg SDK composite action before the native suite, as the
Java jobs already do.

Velox's own dependency build also compiles Folly in `velox/deps-download`. On
macOS with OpenSSL 3.6 or newer, Folly's `X509_get_subject_name` assignment no
longer compiles, and its example/benchmark targets fail to link on Intel
macOS. The bootstrap applies both reviewed Folly source patches (ASN.1
accessors and the const `X509_get_subject_name` result) before configure,
disables its examples and benchmarks, and sets `PROMPT_ALWAYS_RESPOND=n` so the
pinned dependency sources are reused instead of re-downloaded on non-interactive
runners.
On macOS the Folly build also selects the documented pre-libc++ 19 exception
construction path because Homebrew LLVM headers are newer than the runner
libc++abi; the same define is applied when Pangrova rebuilds Folly itself.

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

## Disk and Cache Budget

A cold dependency and Velox tree is roughly 30 GB. A standard Ubuntu or macOS
runner ships about 14 GB free and the repository cache limit is 10 GB, so:

- CI removes preinstalled toolchains the build never uses (Android SDK, .NET,
  CodeQL, iOS simulator runtimes) before downloading anything, and fails fast
  when less than 20 GB is free.
- Only `~/.pangrova/native-deps`, `~/.pangrova/velox/deps-install` and
  `~/.pangrova/velox/archive-cache` are cached. The whole `~/.pangrova/velox`
  tree is never cached: it cannot fit, and the bootstrap verifies every pinned
  dependency revision so a partial restore is rejected rather than trusted.
  Binary dependency cache keys include the runner image, toolchain action,
  bootstrap script and Folly source patches; they do not fall back to another
  image, compiler configuration or pre-patch dependency tree.

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
