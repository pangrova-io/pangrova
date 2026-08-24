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
mvn -q test

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
cmake --preset dev-asan -S pangrova-worker-native
cmake --build pangrova-worker-native/build-asan --parallel
cd pangrova-worker-native/build-asan && ctest --output-on-failure
```

### Package (native lib → Java classpath)

```bash
./pangrova-worker-native/bin/package-native-lib.sh
# Target: pangrova-worker/src/main/resources/META-INF/native/{os}/{arch}/libpangrova_native.*
```

### Lance FFI (Rust → C static library)

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
│ Step 1: C++ Build (0 errors, 0 warnings)                        │
│   cmake --preset dev-release -S pangrova-worker-native             │
│   cmake --build pangrova-worker-native/build --parallel            │
│                                                                  │
│ Step 2: C++ Test (all gtests pass)                               │
│   ctest --test-dir pangrova-worker-native/build --output-on-failure│
│                                                                  │
│ Step 3: C++ ASan (0 findings)                                    │
│   cmake --preset dev-asan -S pangrova-worker-native                │
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
│   mvn -q test                                                    │
│                                                                  │
│ Step 7: E2E (full pipeline: SQL → Native → Result)               │
│   mvn -q -pl pangrova-testing/pangrova-test-e2e -am test            │
│                                                                  │
│ Step 8: Smoke (full-chain connectivity)                          │
│   mvn -q -pl pangrova-testing/pangrova-test-smoke -am test          │
│                                                                  │
│ ALL 8 STEPS MUST PASS. No exceptions.                           │
└──────────────────────────────────────────────────────────────────┘
```

If ANY step fails → fix it. Do not proceed to the next step until the current one passes.
Do not skip steps. Do not claim "not my code". The full chain is your responsibility.
