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
half an hour of compilation.

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

## Industry Alternatives

The current approach builds from pinned sources on standard runners. Larger
organizations with the same dependency stack usually add one or more of:

- A prebuilt dependency image (Docker) for Linux jobs, so CI pulls binaries
  instead of compiling Velox.
- Published per-platform Native artifacts consumed by Java jobs, keeping C++
  compilation out of the test path.
- `ccache` or remote build caching keyed by compiler, flags and source hash.
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
