# Build Pipeline

Prerequisite: Set `JAVA_HOME` to a JDK 17 installation, place `$JAVA_HOME/bin` and Maven 3.9+ on `PATH`, and verify both with `java -version` and `mvn -version`.

## Java Side (Maven, JDK 17)

The Iceberg JDBC catalog dependency is a separately versioned Pangrova patch
candidate, built from a pinned Apache source archive. Before Maven on a clean
machine, publish its consumed artifacts to the local Maven repository:

```bash
.pangrova/scripts/build-iceberg-sdk-candidate.sh
```

The script requires JDK 17 and Gradle 8.14.4, verifies the source archive and
patch checksums, applies the tracked patch, and builds through the upstream
Gradle build. Set `GRADLE_BIN` if that exact Gradle executable is not on `PATH`.
Do not validate production artifacts against the unpatched upstream 1.11.0
artifacts.

```bash
# Compile (single module)
mvn -q -pl <module> -am compile test-compile

# Compile (full project)
mvn -q -DskipTests install

# Test (single module)
mvn -q -pl <module> -am test

# Test (full project)
mvn -q -Pfull-regression verify

# Package
mvn -q -DskipTests package
```

## C++ / Native Side (CMake, C++20)

### Environment Setup (first time only)

```bash
./pangrova-worker-native/bin/setup-native-build-env.sh
```

### Compile

```bash
# Lance is a mandatory Native dependency. Build it before every fresh CMake
# configure; the script uses Cargo.lock and the hermetic protobuf toolchain.
./pangrova-worker-native/bin/build-lance-ffi.sh

# Recommended daily build (Release + tests ON)
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel

# Debug build
cmake --preset dev-debug -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel

# One-command full pipeline (env + build + package)
./pangrova-worker-native/bin/build-native.sh
```

### Test

Native SDK tests require a live real PostgreSQL/Lance authority. The runner below
prepares the current Java fixture with skipped tests, starts a fresh authority,
authenticates readiness, and owns its shutdown and PostgreSQL cleanup. It requires
JDK 17, Maven, Python 3.10+, and working Docker/Testcontainers connectivity.
It forwards CTest options and caller sanitizer/Docker environment. Direct CTest
is appropriate only when the caller already owns a live fixture and injects its
protected socket/nonce environment; a leftover socket is not readiness evidence.
Each run retains complete per-test output, including diagnostics from passing
tests, in its printed log directory's `ctest.log`. Use that snapshot for rolling
validation: subsequent CTest discovery can overwrite the build-wide `LastTest.log`.

```bash
# Run all C++ tests (gtest)
python3 pangrova-worker-native/bin/run-native-tests.py

# Run specific test
python3 pangrova-worker-native/bin/run-native-tests.py -- -R '^Suite.Test$'

# ASan build + test
# Point VELOX_BUILD_DIR at a Velox archive compiled with ASan and
# PANGROVA_ASAN_PROTOBUF_LIB at a v25.3 libprotobuf.a built with ASan
# using the same compiler and ASan-instrumented Abseil as Pangrova. The
# matching Abseil package must also be selected for the Native configure.
# Folly's JSON builder must also be ASan-instrumented; point
# PANGROVA_ASAN_FOLLY_PREFIX at an isolated Folly install built with the same
# compiler and FOLLY_LIBRARY_SANITIZE_ADDRESS=ON. The configure rejects a
# missing or unsanitized Folly archive rather than producing mixed-instrumentation findings.
# On macOS pass CMAKE_PROJECT_folly_INCLUDE=<repository>/pangrova-worker-native/
# cmake/AsanFollyCompat.cmake to Folly's configure to disable only the
# unsupported UBSan vptr subcheck while retaining ASan container annotations.
# The default Release archives are
# rejected because mixed instrumentation can mask or fabricate findings.
export VELOX_BUILD_DIR=/path/to/asan/velox/build
cmake --preset dev-asan -S pangrova-worker-native \
  -DPANGROVA_ASAN_PROTOBUF_LIB=/path/to/asan/libprotobuf.a \
  -Dabsl_DIR=/path/to/asan/absl/lib/cmake/absl \
  -DPANGROVA_ASAN_FOLLY_PREFIX=/path/to/asan/folly/install
cmake --build pangrova-worker-native/build-asan --parallel
python3 pangrova-worker-native/bin/run-native-tests.py \
  --build-dir pangrova-worker-native/build-asan
```

On macOS arm64 with Folly v2026.01.05.00 and LLVM 17.0.6, build the pinned
Folly archive using the [qualified Darwin dependency](../../docs/development/darwin-function-sanitizer.md#build-the-integer-corrected-dependency).
Pass its install prefix as `PANGROVA_ASAN_FOLLY_PREFIX` to the Native configure.

The macOS gate qualifies address checks with
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`; it does not qualify LeakSanitizer.
The Native test runner retains each invocation's per-test output and rejects a
recoverable sanitizer diagnostic even when CTest exits successfully. Record
the actual diagnostic, scope and toolchain when investigating a dependency.
Dependency runtime probes use the same platform scope. Linux ASan/LSan is a
separate release gate and must not be inferred from the macOS result.

### Package (native lib → Java classpath)

```bash
./pangrova-worker-native/bin/package-native-lib.sh
# Target: pangrova-worker/src/main/resources/META-INF/native/{os}/{arch}/libpangrova_native.*
```

Before testing with `-Pnative-runtime`, install the platform classifier from these
same staged bytes. That profile consumes the classifier; it does not rebuild it.
Without `native.platform.classifier`, the bundle module skips installation, so a
normal reactor install can leave an older classifier in the local Maven repository.
The loader must keep rejecting conflicting native payloads on the classpath.

For macOS AArch64, from the repository root:

```bash
pangrova_native_resource_dir="$PWD/pangrova-worker/src/main/resources/META-INF/native/osx/aarch64"
mvn -q -pl pangrova-native-bundle \
  -Dnative.platform.classifier=osx-aarch_64 \
  -Dnative.resource.os=osx -Dnative.resource.arch=aarch64 \
  -Dnative.resource.dir="$pangrova_native_resource_dir" \
  -Dnative.library.path="$pangrova_native_resource_dir/libpangrova_native.dylib" \
  clean install
```

Use the matching classifier, resource OS/architecture and library filename on
other platforms. The built library, staged resource and installed classifier entry
must have the same SHA-256 before running Java/native tests.

### Lance FFI (Rust → C static library, required)

```bash
./pangrova-worker-native/bin/build-lance-ffi.sh          # host platform
./pangrova-worker-native/bin/build-lance-ffi.sh --all    # all platforms
```

## CMake Presets

| Preset | Purpose | Build Dir |
|--------|---------|-----------|
| `dev-debug` | Debug, tests ON, OTel ON | `build/` |
| `dev-release` | Release, tests ON, OTel ON **(recommended)** | `build/` |
| `dev-asan` | AddressSanitizer (heap/stack overflow, UAF) | `build-asan/` |
| `dev-msan` | MemorySanitizer (uninitialized reads) | `build-msan/` |
| `dev-ubsan` | UndefinedBehaviorSanitizer | `build-ubsan/` |
| `ci-linux-x86_64` | CI Linux (Release, tests OFF) | `build-ci/` |
| `ci-osx-aarch64` | CI macOS (Release, tests OFF) | `build-ci/` |

## Build and Validation Dependencies

Select local scope using `development.md`. Runtime correctness CI and releases
validate the full chain; docs/rules/tooling-only PRs use lightweight checks.
Ordering follows artifact dependencies, rather than a fixed list of repeated runs:

| Producer | Consumer / required evidence |
|---|---|
| Pinned dependencies + Lance FFI | Native configure/build and CTest |
| Fresh native library | Package before Java tests load JNI |
| Same-source production image | Verified image ID, source revision, classifier and runtime manifest |
| Complete installed Java reactor | Independent-process integration tests that resolve reactor artifacts |
| `native-runtime,full-regression verify` | Unit/integration/E2E/activation smoke in one reactor run |
| Seven isolated `native-runtime-owner` E2E selections | Process-owning Native tests, each in its own JVM; LOCAL FILE selects its own deployment |
| Selected native ASan/UBSan builds + tests | Zero findings before production correctness acceptance |

```
┌──────────────────────────────────────────────────────────────────┐
│ FULL-CHAIN BUILD + VERIFY (run in this exact order)             │
│                                                                  │
│ Step 0: Lance Rust FFI Build (0 errors, 0 warnings)             │
│   ./pangrova-worker-native/bin/build-lance-ffi.sh               │
│                                                                  │
│ Step 1: C++ Build (0 errors, 0 warnings)                        │
│   cmake --preset dev-release -S pangrova-worker-native             │
│   cmake --build pangrova-worker-native/build --parallel            │
│                                                                  │
│ Step 2: C++ Test (all gtests pass)                               │
│   python3 pangrova-worker-native/bin/run-native-tests.py          │
│                                                                  │
│ Step 3: C++ ASan (0 findings)                                    │
│   cmake --preset dev-asan -S pangrova-worker-native              │
│     -DPANGROVA_ASAN_PROTOBUF_LIB=<ASan v25.3 archive>            │
│     -Dabsl_DIR=<ASan Abseil CMake package>                       │
│     -DPANGROVA_ASAN_FOLLY_PREFIX=<ASan Folly install>             │
│   cmake --build pangrova-worker-native/build-asan --parallel       │
│   python3 pangrova-worker-native/bin/run-native-tests.py           │
│     --build-dir pangrova-worker-native/build-asan                  │
│                                                                  │
│ Step 4: Repackage native lib to Java resources                   │
│   ./pangrova-worker-native/bin/package-native-lib.sh               │
│                                                                  │
│ Step 5: Java Full Compile (all 24+ modules)                      │
│   java -version && mvn -version                                 │
│   mvn -q -DskipTests install                                    │
│                                                                  │
│ Step 6: Java Full Test (all modules, shared E2E and smoke)       │
│   mvn -q -Pfull-regression verify                               │
│                                                                  │
│ Step 7: Isolated Native owner E2E (seven separate JVMs)          │
│   see commands below                                             │
│                                                                  │
│ ALL REQUIRED STEPS MUST PASS; report blockers accurately.                           │
└──────────────────────────────────────────────────────────────────┘
```

A failed prerequisite blocks dependent checks; independent validation may continue.
Fix failures caused by the change. Record baseline/environment failures with evidence
and their validation impact according to `development.md`; do not claim a blocked
chain passed or expand the task into unrelated repairs.
`full-regression` removes daily filename exclusions and runs ordinary E2E
groups, including routing integration and performance tests.
Its shared JVM run excludes only `native-runtime-owner`: these selections start and
stop the process-wide Native runtime and would invalidate later SQL tests.
Complete Step 7 by running each owner selection in its own Maven invocation/JVM:

```bash
set -e
for suite in WorkerBootstrapE2ETest NativeExchangeTransportExternalIntegrationTest \
             WorkerShuffleChannelAdmissionIntegrationTest \
             NativeShuffleEgressContractIntegrationTest \
             NativeAiFailureProjectionRegistrationTest \
             'CopySourceReadHttpE2ETest#privateTicketStagesExactVersionAndRestartsFromManagedBytes' \
             LocalGovernedFileSqlE2ETest; do
  file_backend=s3
  if [[ "$suite" == LocalGovernedFileSqlE2ETest ]]; then file_backend=local; fi
  mvn -q -Pfull-regression -pl pangrova-testing/pangrova-testing-integration-tests \
    -Dpangrova.e2e.excludedGroups=none -Dtest="$suite" \
    -Dpangrova.e2e.file.backend="$file_backend" test
done
```

The Step 6 shared run, including `ActivationSmokeE2ETest`, and all seven isolated
owner selections must pass; report their test counts
separately. A single shared JVM run that excludes these selections without the
seven isolated results is incomplete.
It does not turn routing integration into independent-process distributed evidence.
Record discovered test counts and FAIL/SKIPPED/BLOCKED results; a successful Maven
command that selects no applicable tests is not validation. Use the optional
`-Pcoverage` profile to inspect JaCoCo reports when it helps identify untested
behavior; a percentage is not a release gate.
The `pangrova-testing-integration-tests` smoke-harness self-test only proves test discovery; the
`ActivationSmokeE2ETest` in Step 6 is the real full-chain activation test.

The exact production image staging, provenance properties and commands live in
`.github/workflows/ci.yml`; avoid copying its long argument list into task logs
or maintaining another full-pipeline runner.
