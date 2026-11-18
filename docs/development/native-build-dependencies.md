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
  `libtoolize` on Linux, `glibtoolize` on macOS), plus `pkg-config`.
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
building dependencies to catch ABI mismatches early. On macOS, CI selects
Homebrew LLVM 17 using `PANGROVA_LLVM_PREFIX`; without an explicit prefix, the
bootstrap uses `~/.pangrova/llvm` or Homebrew's default LLVM. The bootstrap
resolves `SDKROOT` with `xcrun` when needed and records it in `env.sh`: CMake
then treats the SDK headers as implicit system headers instead of placing them
ahead of libc++ while building gRPC. CI compiles a small zlib consumer to
verify that header ordering before building the dependencies. macOS CI reuses
preinstalled Homebrew tools without upgrading them and uses ccache only when
already available, avoiding unnecessary source builds on Intel runners.

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

After the regular bootstrap fetches the pinned sources, `setup-native-asan-env.sh`
rebuilds Abseil, protobuf, RE2, gRPC, Folly, Velox and Arrow Flight into separate
ASan prefixes and build directories. Folly uses C++20 and its upstream
`FOLLY_LIBRARY_SANITIZE_ADDRESS` option. The generated `asan-env.sh` records
`PANGROVA_ASAN_FOLLY_PREFIX`, which CI passes to Pangrova's CMake configure.
An explicitly supplied Folly prefix is consumed rather than overwritten, allowing
the separately qualified Darwin build described in the sanitizer guide.

Pangrova checks Folly's headers and archive instrumentation regardless of whether
Velox dependencies came from `deps-install` or a system prefix. Missing or
uninstrumented Folly is an error, not a reason to fall back to Release. The
`Folly::folly` CMake target carries the dependency's sanitizer runtime flags;
linking only its archive would lose the UBSan runtime that Folly also enables.
Release Folly archives found in other dependency directories are excluded from
ASan linkage.

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
  Binary dependency cache keys include the runner image and toolchain action;
  they do not fall back to another image or compiler configuration.

## Industry Alternatives

The current approach builds from pinned sources on standard runners. Larger
organizations with the same dependency stack usually add one or more of:

- A prebuilt dependency image (Docker) for Linux jobs, so CI pulls binaries
  instead of compiling Velox.
- Published per-platform Native artifacts consumed by Java jobs, keeping C++
  compilation out of the test path.
- `ccache` or remote build caching keyed by compiler, flags and source hash.
  The repository enables `ccache` when it is installed and caches the compiler
  results between runs, which keeps repeated CI builds practical without caching
  the multi-gigabyte object tree itself.
- Larger runners with more disk for cold builds.

These reduce cost but add artifact publication and trust requirements. The
repository keeps the source build as the correctness reference and would need a
provenance model before consuming prebuilt binaries.

## Diagnosing Failures

| Symptom | Cause and fix |
|---|---|
| `cargo not found` in seconds | Rust toolchain missing; install rustup and let `rust-toolchain.toml` select the channel |
| `OpenSSL` / `zlib` not found during gRPC | Install `libssl-dev` and `zlib1g-dev` before the bootstrap |
| `Could not find CMAKE_ROOT` | A package-managed CMake shim shadows the system one; re-run through the bootstrap, which re-pins the verified binary |
| Build dies with `No space left on device` | Free runner disk or move `PANGROVA_NATIVE_BUILD_DIR` to a larger volume |
| Cache restore succeeds but the build still recompiles | The cache does not include the Velox build tree by design; only the install prefixes are cached |
| Sanitizer job reports findings in passing tests | The runner rejects any recoverable sanitizer diagnostic even when CTest succeeds; read `ctest.log` |
