# Build Pipeline

Prerequisite: Set `JAVA_HOME` to a JDK 17 installation, place `$JAVA_HOME/bin` and Maven 3.9+ on `PATH`, and verify both with `java -version` and `mvn -version`.

## Java Side (Maven, JDK 17)

```bash
# Compile (single module)
mvn -q -pl <module> -am compile test-compile

# Compile (full project)
mvn -q -DskipTests install

# Test (single module)
mvn -q -pl <module> -am test

# Test (full project)
mvn -q -Pfull-regression,coverage verify

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
# The default Release archives are
# rejected because mixed instrumentation can mask or fabricate findings.
export VELOX_BUILD_DIR=/path/to/asan/velox/build
cmake --preset dev-asan -S pangrova-worker-native \
  -DPANGROVA_ASAN_PROTOBUF_LIB=/path/to/asan/libprotobuf.a \
  -Dabsl_DIR=/path/to/asan/absl/lib/cmake/absl
cmake --build pangrova-worker-native/build-asan --parallel
cd pangrova-worker-native/build-asan && ctest --output-on-failure
```

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

## Full-Chain Build Order (MANDATORY for completion)

Every modification session must execute this full chain before declaring done.
Steps are sequential — each depends on the previous.

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
│ Step 6: Java Full Test (all modules, 0 failures)                 │
│   mvn -q -Pfull-regression,coverage verify                      │
│                                                                  │
│ Step 7: E2E (full pipeline: SQL → Native → Result)               │
│   mvn -q -Pfull-regression                                      │
│     -pl pangrova-testing/pangrova-test-e2e -am test              │
│                                                                  │
│ Step 8: Smoke (real SQL → Worker → packaged JNI → result)        │
│   mvn -q -pl pangrova-testing/pangrova-test-e2e                  │
│     -Dtest=ActivationSmokeE2ETest test                           │
│                                                                  │
│ ALL 8 STEPS MUST PASS. No exceptions.                           │
└──────────────────────────────────────────────────────────────────┘
```

If ANY step fails → fix it. Do not proceed to the next step until the current one passes.
Do not skip steps. Do not claim "not my code". The full chain is your responsibility.
`full-regression` removes daily filename/tag exclusions, including untagged
`*IntegrationTest`, native bootstrap, routing integration, and performance tests.
It does not turn routing integration into independent-process distributed evidence.
Record discovered test counts and FAIL/SKIPPED/BLOCKED results; a successful Maven
command that selects no applicable tests is not validation. The coverage profile
produces each module's real JaCoCo report; run the changed-line coverage gate
against the actual review base after verification, including uncommitted files.
The `pangrova-test-smoke` harness self-test only proves test discovery; it does
not qualify as Step 8 until it contains a real full-chain activation test.
