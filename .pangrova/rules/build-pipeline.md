# Build Pipeline

This file owns build and test commands. `development.md` decides which ones a change
needs; `.github/workflows/ci.yml` owns the exact CI invocations and image provenance.

## Environment

Set `JAVA_HOME` to JDK 17 with Maven 3.9+ on `PATH`, and verify `java -version` and
`mvn -version` before diagnosing Java failures. Native work needs CMake, Ninja, a
C++20 compiler and the dependencies installed by
`./pangrova-worker-native/bin/setup-native-build-env.sh`. Native tests also need
Python 3.10+ and Docker. Keep personal paths, credentials, fixed host ports and IDE
state out of tracked files.

`setup-native-build-env.sh --check-tools` is the single toolchain contract: it
checks Rust, autotools, libtool, pkg-config, ripgrep, system OpenSSL/zlib headers on Linux,
and (on macOS) Homebrew and a Clang 15+ toolchain before any dependency build. The
bootstrap builds gRPC before Velox's own `setup-ubuntu.sh` runs, so OpenSSL and
zlib development headers must already be installed. That setup script also
installs a package-managed CMake shim; both bootstraps validate the CMake found at
startup and re-prepend its directory before configuring Velox, because a shim
without its Modules directory aborts configuration. CI jobs never install native prerequisites
ad hoc; they call `.github/actions/setup-native-build`, which installs that contract
on Ubuntu (build-essential, autotools, rustup with the channel pinned by
`pangrova-worker-native/lance-ffi/rust-toolchain.toml`) or reuses the macOS
image's tools and an installed Homebrew LLVM 15+ (installing only missing
formulae without upgrading bottles) and then runs `--check-tools`, so a missing
tool fails fast with its name. Velox and Rust pins resolve upstream
(`v2026.04.17.00`, Rust 1.98.1); bump both together with the script.

A cold dependency and Velox tree is about 30 GB, so CI frees preinstalled
toolchains on standard runners before building and caches only
`~/.pangrova/native-deps`, `~/.pangrova/velox/deps-install` and
`~/.pangrova/velox/archive-cache`, plus the bounded compiler cache
(`~/.cache/ccache`). The whole `~/.pangrova/velox` tree must not be
cached: it exceeds the repository cache limit, and a restored cache must never
contain partial dependency checkouts (the setup script verifies each pinned
revision).

On a clean machine, publish the patched Iceberg JDBC catalog candidate before Maven.
It needs JDK 17 and Gradle 8.14.4 (`GRADLE_BIN` if not on `PATH`), verifies archive
and patch checksums, and must not be replaced by the unpatched upstream 1.11.0:

```bash
.pangrova/scripts/build-iceberg-sdk-candidate.sh
```

## Java

`test` and `verify` already compile; a separate compile is only a diagnostic.

```bash
mvn -q -pl <module> -am compile test-compile        # focused compile
mvn -q -pl <module> -am test                        # focused tests
mvn -q -pl <module> -am verify -Pstatic-analysis    # Checkstyle + SpotBugs/FindSecBugs
mvn -q -pl <module> -am verify -Pcoverage           # optional JaCoCo diagnosis
mvn -q -DskipTests install                          # full reactor install
mvn -q test                                         # full reactor, daily selection
mvn -q -Pfull-regression verify                     # full production regression
mvn -q -DskipTests package                          # package
```

## Native

```bash
./pangrova-worker-native/bin/build-lance-ffi.sh    # required before every fresh configure; --all for all platforms
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
python3 pangrova-worker-native/bin/run-native-tests.py                 # all tests
python3 pangrova-worker-native/bin/run-native-tests.py -- -R '^Suite.Test$'
./pangrova-worker-native/bin/build-native.sh       # environment + build + package in one step
```

The test runner prepares the Java fixture, starts and authenticates a fresh
PostgreSQL/Lance authority, owns its cleanup and forwards CTest options. Run CTest
directly only when you already own a live fixture and inject its socket/nonce
environment. Each run keeps full per-test output in the printed log directory's
`ctest.log`; the build-wide `LastTest.log` can be overwritten.

| Preset | Purpose | Build dir |
|---|---|---|
| `dev-release` | Release, tests and OTel on (recommended) | `build/` |
| `dev-debug` | Debug, tests and OTel on | `build/` |
| `dev-asan` | AddressSanitizer | `build-asan/` |
| `dev-msan` | MemorySanitizer | `build-msan/` |
| `dev-ubsan` | UndefinedBehaviorSanitizer | `build-ubsan/` |
| `ci-linux-x86_64`, `ci-osx-aarch64` | CI Release, tests off | `build-ci/` |

### Sanitizers

ASan builds reject uninstrumented dependencies, because mixed instrumentation can mask
or fabricate findings. Supply Velox, protobuf v25.3, Abseil and Folly (with
`FOLLY_LIBRARY_SANITIZE_ADDRESS=ON`) built with the same compiler and ASan:

```bash
export VELOX_BUILD_DIR=/path/to/asan/velox/build
cmake --preset dev-asan -S pangrova-worker-native \
  -DPANGROVA_ASAN_PROTOBUF_LIB=/path/to/asan/libprotobuf.a \
  -Dabsl_DIR=/path/to/asan/absl/lib/cmake/absl \
  -DPANGROVA_ASAN_FOLLY_PREFIX=/path/to/asan/folly/install
cmake --build pangrova-worker-native/build-asan --parallel
python3 pangrova-worker-native/bin/run-native-tests.py --build-dir pangrova-worker-native/build-asan
```

UBSan uses `dev-ubsan` and `--build-dir pangrova-worker-native/build-ubsan`. On macOS,
pass `CMAKE_PROJECT_folly_INCLUDE=<repo>/pangrova-worker-native/cmake/AsanFollyCompat.cmake`
to Folly's configure; on arm64 with Folly v2026.01.05.00 and LLVM 17.0.6 build the
[qualified Darwin dependency](../../docs/development/darwin-function-sanitizer.md#build-the-integer-corrected-dependency).
The macOS gate runs `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1` and does not qualify
LeakSanitizer; Linux ASan/LSan is a separate release gate. The runner rejects a
recoverable sanitizer diagnostic even when CTest succeeds.

### Packaging

```bash
./pangrova-worker-native/bin/package-native-lib.sh
# → pangrova-worker/src/main/resources/META-INF/native/{os}/{arch}/libpangrova_native.*
```

`-Pnative-runtime` consumes an installed platform classifier and does not rebuild it.
Install it from the same staged bytes (macOS arm64 shown; adjust classifier, OS,
architecture and filename elsewhere):

```bash
dir="$PWD/pangrova-worker/src/main/resources/META-INF/native/osx/aarch64"
mvn -q -pl pangrova-native-bundle -Dnative.platform.classifier=osx-aarch_64 \
  -Dnative.resource.os=osx -Dnative.resource.arch=aarch64 -Dnative.resource.dir="$dir" \
  -Dnative.library.path="$dir/libpangrova_native.dylib" clean install
```

The built library, staged resource and installed classifier must have the same
SHA-256. A normal reactor install skips the bundle and can leave an older classifier.

## Cross-Language Checks

```bash
./pangrova-worker-native/bin/package-native-lib.sh
mvn -q -pl pangrova-worker -am test
mvn -q -pl pangrova-testing/pangrova-testing-integration-tests -am test
mvn -q -pl pangrova-testing/pangrova-testing-integration-tests -Dtest=ActivationSmokeE2ETest test
```

## Full Chain

Run in artifact-dependency order. A failed prerequisite blocks its dependents;
independent checks may continue.

1. `build-lance-ffi.sh`, then the `dev-release` build: zero errors and warnings.
2. Native tests: all pass.
3. Native ASan (and UBSan where selected): zero findings.
4. `package-native-lib.sh` and classifier install.
5. `mvn -q -DskipTests install` (independent-process tests resolve reactor artifacts).
6. `mvn -q -Pfull-regression verify`: unit, integration, E2E and `ActivationSmokeE2ETest`
   in one reactor run.
7. Each `native-runtime-owner` selection in its own JVM:

```bash
set -e
for suite in WorkerBootstrapE2ETest NativeExchangeTransportExternalIntegrationTest \
             WorkerShuffleChannelAdmissionIntegrationTest \
             NativeShuffleEgressContractIntegrationTest \
             NativeAiFailureProjectionRegistrationTest \
             WorkerMembershipAdmissionE2ETest \
             'CopySourceReadHttpE2ETest#privateTicketStagesExactVersionAndRestartsFromManagedBytes' \
             LocalGovernedFileSqlE2ETest; do
  file_backend=s3
  if [[ "$suite" == LocalGovernedFileSqlE2ETest ]]; then file_backend=local; fi
  mvn -q -Pfull-regression -pl pangrova-testing/pangrova-testing-integration-tests \
    -Dpangrova.e2e.excludedGroups=none -Dtest="$suite" \
    -Dpangrova.e2e.file.backend="$file_backend" test
done
```

Report test counts for step 6 and each step 7 selection, including FAIL, SKIPPED and
BLOCKED results; step 6 without the eight isolated results is incomplete. Routing
integration is not independent-process distributed evidence. CI additionally builds a
same-source production image and verifies its image ID, source revision, classifier
and runtime manifest; do not copy that argument list into another runner.
