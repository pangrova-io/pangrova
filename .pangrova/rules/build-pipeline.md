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

```bash
# Run all C++ tests (gtest)
cd pangrova-worker-native/build && ctest --output-on-failure

# Run specific test
cd pangrova-worker-native/build && ./pangrova_native_tests --gtest_filter="Suite.Test"

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
cd pangrova-worker-native/build-asan && ctest --output-on-failure
```

macOS does not provide LeakSanitizer for this toolchain; run its ASan CTest with
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`. Linux ASan/LSan is a separate
release gate and must not be inferred from the macOS result.

### Package (native lib → Java classpath)

```bash
./pangrova-worker-native/bin/package-native-lib.sh
# Target: pangrova-worker/src/main/resources/META-INF/native/{os}/{arch}/libpangrova_native.*
```

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

## Full-Chain Build Order

This is the reference chain for production regression and broad execution changes.
Select local validation using `development.md`: isolated Java changes run module
checks; docs/tooling run their own checks; runtime JNI/protocol/native/execution
changes validate the affected links below. ASan is required for memory, ownership,
JNI, or concurrency changes. Full production CI/release validation runs all steps.

Build artifacts must come from the revision under test. Dependency setup and Lance
FFI builds may reuse verified outputs when their inputs/toolchains are unchanged.
After native runtime changes, rebuild and package before Java tests load the library.
Independent Java and native compilation may run separately; preserve artifact
ordering for tests that consume the native bundle.

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
│   ctest --test-dir pangrova-worker-native/build --output-on-failure│
│                                                                  │
│ Step 3: C++ ASan (0 findings)                                    │
│   cmake --preset dev-asan -S pangrova-worker-native              │
│     -DPANGROVA_ASAN_PROTOBUF_LIB=<ASan v25.3 archive>            │
│     -Dabsl_DIR=<ASan Abseil CMake package>                       │
│     -DPANGROVA_ASAN_FOLLY_PREFIX=<ASan Folly install>             │
│   cmake --build pangrova-worker-native/build-asan --parallel       │
│   ctest --test-dir pangrova-worker-native/build-asan               │
│     --output-on-failure                                          │
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
│ Step 7: Isolated Native owner E2E (three separate JVMs)          │
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
Its shared JVM run excludes only `native-runtime-owner`: these classes start and
stop the process-wide Native runtime and would invalidate later SQL tests.
Complete Step 7 by running each owner class in its own Maven invocation/JVM:

```bash
for suite in WorkerBootstrapE2ETest NativeExchangeTransportExternalIntegrationTest \
             WorkerShuffleChannelAdmissionIntegrationTest; do
  mvn -q -Pfull-regression -pl pangrova-testing/pangrova-test-e2e \
    -Dpangrova.e2e.excludedGroups=none -Dtest="$suite" test
done
```

The Step 6 shared run, including `ActivationSmokeE2ETest`, and all three isolated
owner runs must pass; report their test counts
separately. A single shared JVM run that excludes these classes without the
three isolated results is incomplete.
It does not turn routing integration into independent-process distributed evidence.
Record discovered test counts and FAIL/SKIPPED/BLOCKED results; a successful Maven
command that selects no applicable tests is not validation. Use the optional
`-Pcoverage` profile to inspect JaCoCo reports when it helps identify untested
behavior; a percentage is not a release gate.
The `pangrova-test-smoke` harness self-test only proves test discovery; the
`ActivationSmokeE2ETest` in Step 6 is the real full-chain activation test.
