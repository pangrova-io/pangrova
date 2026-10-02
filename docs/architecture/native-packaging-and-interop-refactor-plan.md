---
title: "Native Packaging and Java/C++ Interop Refactor Plan"
sidebar_label: "Native Packaging and Interop Plan"
---

# Native Packaging and Java/C++ Interop Refactor Plan

## 1. Status and decision summary

Status: Target architecture and delivery plan; proposed identifiers in this
document are not current production types until their implementation gate passes.

Audit baseline:

- Pangrova working tree inspected on 2026-09-12. The tree contained unrelated
  uncommitted work; this plan does not treat those changes as part of the
  implementation.
- Velox4J reference: `boostscale/velox4j` commit
  `b55cceddcc2e64b065a61dc12b62f98af5327cd1` (2026-09-11).

The target design is:

1. Build the Native engine once per supported target in a pinned environment.
2. Install the JNI library and its complete non-system runtime dependency
   closure into a clean staging directory.
3. Normalize loader paths to `$ORIGIN` on Linux and `@loader_path` on macOS.
4. Generate a signed/checksummed Native manifest and package the staging
   directory as one platform-specific classifier JAR.
5. Test the classifier JAR itself in a clean runtime; tests must not use the
   raw CMake build directory or host library paths.
6. Keep Protobuf plus Substrait as the control-plane contract and the Arrow C
   Data/C Stream interfaces as the in-process batch contract.
7. Reduce JNI to a small, dynamically registered ABI. All feature operations
   use a typed request/response envelope or the standard columnar stream ABI;
   no feature owns an independent JNI protocol.
8. Use typed, generation-safe handles for every Native resource and one error
   model for all expected failures.

This is a replacement, not a compatibility layer. Pangrova is a new project and
the repository explicitly forbids permanent legacy forwarding paths.

## 2. What to adopt from Velox4J

Velox4J has several useful build and interop patterns:

- Maven invokes the C++ build during `generate-resources`, then packages the
  installed Native output from `src/main/cpp/build/dist/lib`.
- CMake uses `file(GET_RUNTIME_DEPENDENCIES)` to collect the main JNI library's
  non-system shared-library closure.
- The build rewrites every packaged ELF runpath to `$ORIGIN`, allowing the
  loader to extract the complete closure into one directory.
- The loader copies all libraries in the platform resource directory before
  loading the main JNI library.
- The Velox revision is pinned by an immutable reference and source checksum,
  with an automated dependency update workflow.
- `JNI_OnLoad` dynamically registers Native methods, avoiding a public symbol
  for every Java method name.
- Java objects hold opaque `long` handles while C++ owns `shared_ptr` values in
  an object store.
- Arrow C Data Interface addresses carry local columnar values without a
  Protobuf encoding of batch contents.

The following Velox4J choices must not be copied directly:

- A single shaded Java/Native artifact is unsuitable for Pangrova's modular
  server and connector ecosystem.
- Velox JSON serde must not become Pangrova's public plan protocol. Pangrova's
  planner and protocol must remain independent of the execution backend.
- Raw `os.name` and `os.arch` strings are not a stable artifact coordinate or
  resource path.
- A loader that rejects a second load attempt is incompatible with Pangrova's
  idempotent bootstrap contract.
- Packaging only one Linux/glibc target is insufficient for Pangrova's declared
  four-platform release matrix.

## 3. Current-state findings

### 3.1 Packaging and release blockers

| ID | Severity | Finding | Consequence |
|---|---|---|---|
| PKG-01 | P0 | The checked-in macOS JNI library links `libarrow`, `libarrow_flight`, `libglog`, and `libgflags`, but the classifier workflow copies only `libpangrova_native` | A clean host cannot load the published JAR |
| PKG-02 | P0 | The checked-in dylib contains absolute RPATH entries under a developer home directory | The artifact is machine-specific and non-reproducible |
| PKG-03 | P0 | `package-native-lib.sh` stages into `pangrova-worker/src/main/resources`, while CI/release stages into `pangrova-native-bundle` | Development and release exercise different artifacts |
| PKG-04 | P0 | `release.yml` references `docker/Dockerfile.native-build`, but the file is under `deploy/docker/` | Linux release builds fail before compilation |
| PKG-05 | P0 | CI installs the classifier JAR but Java tests load the raw build directory through `java.library.path` and `LD_LIBRARY_PATH` | CI can pass while the shipped JAR is unloadable |
| PKG-06 | P1 | The Worker test-resource copy includes only `.dylib`, and `${os.detected.arch}` can produce `aarch_64` while the loader expects `aarch64` | Local Linux and ARM classpath loading are inconsistent |
| PKG-07 | P1 | The bundle enforcer verifies only that `META-INF/native` exists, not the exact target, main library, dependency closure, or manifest | A wrong or empty platform payload can be released |
| PKG-08 | P1 | A 191 MB developer-built dylib lives in `pangrova-worker` resources | Java artifacts can accidentally contain stale, host-specific binaries |
| PKG-09 | P1 | The `native-runtime` Maven profile is not a reliable transitive consumer contract | Downstream users can receive `pangrova-worker` without a usable Native artifact |
| PKG-10 | P1 | The release path has no clean-runtime load test, dependency allowlist, build manifest, or checksum verification | ABI, libc, architecture, and supply-chain drift are detected late |

### 3.2 Protocol and JNI fragmentation

The canonical v2 proto tree is already shared by Maven and CMake, which is the
correct foundation. The remaining fragmentation is semantic and ABI-level:

| ID | Severity | Finding | Target correction |
|---|---|---|---|
| INT-01 | P0 | `RuntimeInitConfig` has no protocol/build handshake | Bootstrap with `InteropClientHello`/`InteropServerHello` before accepting configuration or tasks |
| INT-02 | P1 | protocol major, minor, descriptor hash, and rollout epoch are assembled or hard-coded in several Java and C++ files | Generate one protocol identity on each side from the canonical descriptor and build metadata |
| INT-03 | P1 | Native task parsing requires an exact minor and rollout epoch | Negotiate a compatible range and required features; reserve major for breaking changes |
| INT-04 | P1 | `RuntimeCommandType` plus `Any` permits mismatched command and payload values | Replace the pair with a `oneof` request body; retain `Any` only at intentionally extensible capability boundaries |
| INT-05 | P1 | expected failures are split among JNI exceptions, null/empty results, raw status values, and `RuntimeCommandResponse.error` | Return one typed `ExecutionError` for expected failures; reserve JNI exceptions for ABI/JVM faults |
| INT-06 | P1 | task state is interpreted by hard-coded numeric comparisons in Java | Switch exhaustively on the generated enum and reject `UNRECOGNIZED` |
| INT-07 | P1 | DiskANN and BM25 define feature-specific Native methods and binary layouts | Route execution through typed capability/task operations; remove the independent ABI |
| INT-08 | P1 | memory reclaim, metrics, and server lifecycle mix primitive JNI calls with Protobuf commands | Route process/task control through one typed command envelope |
| INT-09 | P1 | JVM function callbacks exchange `Object[]` and type-name strings | Use a vectorized typed batch contract and a generated function descriptor |
| INT-10 | P1 | the write path mixes Arrow C pointers, IPC byte arrays, and `DirectByteBuffer` conventions | Select one local columnar ownership contract and make IPC a remote/persistence format only |
| INT-11 | P1 | shuffle includes a custom four-byte partition header beside Protobuf tickets and Arrow/Native serdes | Put partition identity in a typed envelope/Flight metadata, not an undocumented byte prefix |
| INT-12 | P1 | `NativePage` says zero-copy but clones a heap `byte[]`; its format enum exposes an implementation format in SPI | Remove it from the Native hot path and move batch encoding details out of engine-neutral SPI |
| INT-13 | P1 | Unconnected Java pointer-stream, ownership-mode and unlimited-capability designs were backed only by fake pointers/enums | Removed; task-local Native owners and the canonical transport remain authoritative. Actual lifecycle/value tests still require packaged JNI execution |
| INT-14 | P1 | symbol scripts export wildcard `Java_*`; declarations are spread across three Java classes | Dynamically register a small ABI from one binding class and export only `JNI_OnLoad`/`JNI_OnUnload` |
| INT-15 | P2 | current JNI comments and architecture docs describe removed methods, v1 schemas, or an executor handle field absent from the current command proto | Regenerate or rewrite boundary documentation from the actual contract inventory |

### 3.3 Build and module boundary drift

- The root `pangrova-bom` is simultaneously the reactor parent, plugin policy,
  and consumer dependency BOM. Those are distinct responsibilities.
- `pangrova-worker` directly contains JNI declarations, loader behavior,
  feature bindings, Arrow dependencies, task orchestration, HTTP, and server
  logic. This prevents a narrow Java/Native boundary.
- Documentation says production dependencies are statically linked, while the
  actual target links shared Arrow/Parquet/Flight libraries and emits dev RPATHs.
- The deployment Dockerfile contains stale module paths and builds Java without
  establishing the platform Native bundle as an input.
- Default Worker tests exclude `native-required`; this cannot validate the
  shipped integrated system.

## 4. Target module and package organization

### 4.1 Maven and distribution modules

```text
pangrova-parent/                    private build parent and plugin policy
pangrova-bom/                       published dependencyManagement only
pangrova-protocol/                  canonical IDL + generated Java + codecs
pangrova-native-runtime/            Java loader, JNI ABI, handles, callbacks
pangrova-worker/                    worker orchestration; no native declarations
pangrova-worker-native/             C++20 engine and JNI implementation
pangrova-native-bundle/             packaging-only classifier JAR producer
pangrova-distribution/              platform server tar/zip/container assembly
```

The repository root remains the reactor aggregator. `pangrova-parent` owns
plugin management and Java build defaults; `pangrova-bom` owns only versions
that consumers import. The split prevents consumer dependency management from
implicitly carrying build extensions and reactor policy.

`pangrova-native-runtime` depends on `pangrova-protocol` and the minimum Arrow
C Data Java artifact required by the selected local data ABI. It is the only
Java module allowed to declare Native methods or import Arrow C Data classes.
`pangrova-worker` depends on its Java interfaces and never calls `System.load`
or declares `native` methods.

### 4.2 Java packages in `pangrova-native-runtime`

```text
org.pangrova.interop.api            Java-facing runtime/session/stream interfaces
org.pangrova.interop.bootstrap      handshake and runtime initialization
org.pangrova.interop.binding        sole package containing native declarations
org.pangrova.interop.handle         typed AutoCloseable handle owners
org.pangrova.interop.columnar       Arrow C Data/C Stream import and ownership
org.pangrova.interop.callback       Native-to-Java callback entry points
org.pangrova.interop.loader         platform detection, extraction, verification
org.pangrova.interop.internal       implementation details, not exported
```

Only `org.pangrova.interop.binding.NativeBindings` declares Native methods.
Every public handle is final, idempotently closeable, scope-bound, and backed by
a `Cleaner` only as a leak detector; correctness must rely on explicit close.

### 4.3 C++ boundary packages

```text
pangrova-worker-native/src/jni/entry/       JNI_OnLoad and registration table
pangrova-worker-native/src/jni/protocol/    envelope parse/validate/dispatch
pangrova-worker-native/src/jni/handle/      typed generation-safe registries
pangrova-worker-native/src/jni/columnar/    C Data/C Stream exports and releases
pangrova-worker-native/src/jni/callback/    cached JavaVM/classes/method IDs
pangrova-worker-native/src/runtime/         process/session lifecycle
pangrova-worker-native/src/executor/        task execution, no JNI includes
```

JNI types must not escape `src/jni`. Velox types must not appear in JNI headers.
The ABI layer translates immediately into protocol DTOs, typed handles, or
engine-neutral columnar interfaces.

### 4.4 Published artifacts

```text
org.pangrova:pangrova-native-runtime:<version>
org.pangrova:pangrova-native-bundle:<version>:linux-x86_64
org.pangrova:pangrova-native-bundle:<version>:linux-aarch_64
org.pangrova:pangrova-native-bundle:<version>:osx-x86_64
org.pangrova:pangrova-native-bundle:<version>:osx-aarch_64
org.pangrova:pangrova-distribution:<version>:<platform>@tar.gz
```

The server distribution selects one platform explicitly. Library consumers may
select a classifier in their dependency declaration; do not rely on an active
profile in a transitive POM. An optional all-platform convenience artifact may
be added later, but it must not be the default server dependency.

## 5. Native packaging specification

### 5.1 Canonical staging layout

```text
stage/<classifier>/
  META-INF/native/<os>/<arch>/
    native-manifest.json
    libpangrova_native.so|dylib
    <every non-system runtime dependency, if any>
    LICENSES/<dependency>.txt
```

No build script writes generated binaries into `src/main/resources`. Maven
packages `${native.stage.dir}` directly. `clean` deletes staging and target
outputs without touching source files.

### 5.2 Native manifest

`native-manifest.json` is generated, never hand-edited, and contains:

- Pangrova version, git commit, dirty flag, build timestamp policy;
- JNI ABI major/minor and protocol descriptor SHA-256;
- target triple, OS, architecture, libc family/minimum, C++ runtime;
- compiler/CMake versions and build type;
- pinned Velox revision and source checksum;
- Arrow/Protobuf versions and enabled feature set;
- main library filename and every packaged dependency in load order;
- SHA-256, size, and SONAME/install-name for every binary;
- an allowlist of expected system libraries.

Release builds must be from a clean source tree and use reproducible timestamps.
The loader verifies platform, ABI, and every checksum before loading any binary.

### 5.3 Linkage policy

Use a static-first policy:

1. Link Velox and dependencies statically when their build supports safe symbol
   isolation.
2. Discover residual shared dependencies with CMake
   `file(GET_RUNTIME_DEPENDENCIES)`.
3. Fail if a discovered dependency is neither on the system allowlist nor in
   the staged bundle.
4. Rewrite Linux runpaths to `$ORIGIN` and macOS install names/runpaths to
   `@loader_path`.
5. Fail on absolute build paths, package-manager prefixes, unresolved symbols,
   duplicate SONAMEs, or unexpected exported symbols.

This intentionally reconciles the current static-only policy with the actual
Arrow/Flight linkage. An ADR must record the residual dynamic-closure rule and
the allowlisted platform libraries before implementation merges.

### 5.4 Loader behavior

The loader must:

- normalize only the four supported platform identifiers;
- reject unknown OS/architecture combinations with a diagnostic list;
- use a content-addressed extraction directory keyed by manifest hash;
- coordinate concurrent processes with an atomic directory move/file lock;
- verify hashes before reuse and after extraction;
- set owner-only permissions and never extract through a caller-controlled path;
- load dependencies in manifest order when required, then load the main library;
- call the handshake probe and mark `loaded` only after ABI validation succeeds;
- be idempotent and retain the original failure for subsequent callers;
- expose build/manifest diagnostics without silently degrading execution.

Production must have one loading path. `java.library.path`, `LD_LIBRARY_PATH`,
and `DYLD_LIBRARY_PATH` are permitted only in an explicitly separate developer
profile and never in release or packaged integration tests.

### 5.5 Build orchestration

Provide one command for a host build and one for a release target:

```text
./pangrova-worker-native/bin/build-native.sh --preset dev-release
./pangrova-worker-native/bin/build-platform-bundle.sh <classifier>
```

Both commands execute CMake configure, build, test, install, dependency audit,
manifest generation, JAR packaging, and packaged load verification. Maven may
invoke the host command at `generate-resources`, as Velox4J does, but the
execution must be controlled by an explicit `native-build` profile so ordinary
Java-only reactor work does not unexpectedly download or compile Velox.

## 6. Unified Java/C++ protocol

### 6.1 Boundary layering

```text
Java domain/API
  -> generated Protobuf control messages + Substrait plan
  -> tiny JNI ABI
  -> generated C++ Protobuf validation/dispatch
  -> engine-neutral Native runtime interfaces
  -> Velox implementation

Local batch data:
  Native-owned typed handle -> Arrow C Data/C Stream view -> Java consumer

Remote batch data:
  Arrow Flight stream + authenticated ticket + typed application metadata
```

Protobuf is not used to wrap each local batch. Raw pointers are never durable
identities, never cross processes, and never appear in Protobuf.

### 6.2 Bootstrap handshake

Add these canonical messages:

```proto
message InteropClientHello {
  uint32 jni_abi_major = 1;
  uint32 min_jni_abi_minor = 2;
  uint32 max_jni_abi_minor = 3;
  ProtocolVersion protocol = 4;
  repeated string required_features = 5;
  string java_runtime_version = 6;
}

message InteropServerHello {
  uint32 jni_abi_major = 1;
  uint32 negotiated_jni_abi_minor = 2;
  ProtocolVersion protocol = 3;
  repeated string supported_features = 4;
  NativeBuildInfo build = 5;
  ExecutionError error = 6;
}
```

Handshake occurs immediately after `System.load`, before runtime config. The
descriptor hash identifies the exact build and supports diagnostics; it is not
used by itself to reject an otherwise compatible additive minor version.
Required feature negotiation determines compatibility.

`rollout_epoch` is deployment policy, not a wire compatibility version. Move it
to worker registration/deployment compatibility metadata and remove it from
every request after the homogeneous rollout gate is implemented.

### 6.3 Stable JNI ABI

The target Native declaration set is deliberately small:

```text
bootstrap(byte[] clientHello) -> byte[] serverHello
initialize(byte[] runtimeRequest) -> byte[] response
invoke(long scopeHandle, byte[] requestEnvelope) -> byte[] responseEnvelope
openStream(long scopeHandle, byte[] streamRequest) -> long streamHandle
getSchema(long streamHandle, long arrowSchemaAddress) -> int
nextBatch(long streamHandle, long arrowArrayAddress) -> int
release(long typedHandle) -> void
shutdown() -> byte[] response
```

Exact signatures may be refined by benchmark results, but feature-specific
methods must not be added. Register the table in `JNI_OnLoad`; export only
`JNI_OnLoad` and `JNI_OnUnload` from the shared library.

The integer return from columnar methods is a closed enum such as `DATA`,
`NOT_READY`, `END`, and `ERROR`. Error details are fetched from the stream's
typed status response; a status byte must never be prefixed to batch payload.

### 6.4 Typed request/response envelopes

Replace `RuntimeCommandType + Any` with an exhaustive `oneof`:

```proto
message InteropRequest {
  uint64 request_id = 1;
  TaskAttemptId attempt = 2;
  TraceContext trace = 3;
  uint64 deadline_epoch_ms = 4;
  oneof body {
    CreateTaskRequest create_task = 20;
    StartTaskRequest start_task = 21;
    CancelTaskRequest cancel_task = 22;
    GetTaskReportRequest get_task_report = 23;
    UpdateResourceGrantRequest update_resource_grant = 24;
    InjectRuntimeFilterRequest inject_runtime_filter = 25;
    PrefetchArtifactsRequest prefetch_artifacts = 26;
    EvictCacheRequest evict_cache = 27;
    StartResultServerRequest start_result_server = 28;
    StartShuffleServerRequest start_shuffle_server = 29;
  }
}

message InteropResponse {
  uint64 request_id = 1;
  ExecutionError error = 2;
  oneof body { /* typed responses */ }
}
```

Global operations use a runtime handle; task operations use a typed task handle.
The request body and routing scope cannot disagree. Unknown requests fail with
`ERROR_CLASS_PROTOCOL_MISMATCH`.

### 6.5 Error contract

Expected Native failures always return `ExecutionError` with:

- stable Pangrova error code;
- error class and retryability;
- failed component and task/partition context;
- sanitized message;
- optional Native diagnostic ID, not an unbounded stack trace in normal RPCs.

JNI throws only for JVM allocation failure, invalid JNI usage, corrupt ABI
state, or inability to serialize the response. Java maps `ExecutionError` once
in `pangrova-native-runtime`; Worker code does not interpret numeric Native
states or catch `UnsatisfiedLinkError` for feature fallback.

### 6.6 Handle and ownership contract

Every handle encodes or validates:

- resource type;
- registry generation/runtime incarnation;
- monotonically increasing resource ID;
- owning scope and close state.

Acquisition returns a strong C++ reference for the duration of the operation.
Release removes the registry entry before scheduling destruction. Wrong-type,
stale, and double-release handles return deterministic errors.

For columnar batches, Native retains ownership. Java imports a non-owning Arrow
view and explicitly releases the batch handle. Any release callback reachable
from a Java thread may enqueue release to a Native executor but must not directly
destroy Velox-owned state. ASan tests must cover close races and leaked handles.

### 6.7 Callback contract

Native-to-Java callbacks are split into:

- control callbacks: one generated Protobuf request/response entry point;
- columnar callbacks: typed writer/function handle plus Arrow C Data addresses;
- lifecycle callbacks: explicit release/abort with idempotency keys.

All classes, method IDs, and the `JavaVM*` are cached during bootstrap. Native
threads attach through an RAII guard. Every call checks `ExceptionCheck`; Java
exceptions are converted once into `ExecutionError` and never left pending.

Scalar and aggregate JVM functions become vectorized batch callbacks. `Object[]`
and string-based runtime type matching are removed from the execution hot path.

### 6.8 Data-format policy

| Boundary | Canonical format | Ownership |
|---|---|---|
| Planner/Worker to Native control | Protobuf + Substrait | copied immutable bytes |
| Native to Java in-process batch | Arrow C Data/C Stream | Native-owned typed handle |
| Java to Native in-process batch | Arrow C Data/C Stream | explicit transfer or borrowed scope |
| Worker-to-worker/result network | Arrow Flight/IPC | transport-owned buffers |
| Durable shuffle/spill | versioned serializer selected by `BatchSerde` | file/object-store ownership |
| Errors/status/metrics | Protobuf | copied immutable bytes |

`CUSTOM_BINARY`, feature-local little-endian result blobs, status-prefixed
payloads, and the shuffle partition header are removed after their consumers
migrate. Engine-specific serializer names stay inside the Native implementation.

## 7. Delivery plan

Each work package should be a reviewable PR. A package does not begin until its
listed dependency is merged and green.

### WP0 — Freeze the contract inventory and write ADRs

Deliverables:

- machine-readable inventory of all Java Native declarations, registered C++
  methods, callbacks, handles, Protobuf messages, and data formats;
- ADR for static-first plus residual runtime-closure packaging;
- ADR for Protobuf control plus Arrow C Data/C Stream data;
- explicit supported platform/libc/toolchain matrix;
- delete or mark as implementation-blocking every unimplemented interop SPI.

Acceptance:

- every current boundary is assigned a target or deletion;
- no code change to a boundary can merge without updating the inventory.

### WP1 — Repair the release path before redesigning the ABI

Dependencies: WP0.

Deliverables:

- fix the Native Dockerfile path and stale Docker module paths;
- remove the checked-in binary from Worker resources;
- make staging output-only and platform-normalized;
- make local and CI packaging call the same script;
- verify exact bundle contents and fail on wrong platform/missing file.

Acceptance:

- all four classifier JARs are created from clean staging directories;
- no generated Native binary appears under a source resource directory.

### WP2 — Package the runtime dependency closure

Dependencies: WP1.

Deliverables:

- CMake install component for the main library and runtime closure;
- `$ORIGIN`/`@loader_path` normalization;
- system-library allowlists and unresolved-symbol checks;
- generated Native manifest, licenses, and checksums;
- strip/debug-symbol policy with separate debug artifacts.

Acceptance:

- `ldd`/`readelf` and `otool` show no developer paths;
- every non-system dependency resolves from the extracted bundle;
- symbol export set contains only the approved JNI entry points.

### WP3 — Make the packaged JAR the only tested runtime

Dependencies: WP2.

Deliverables:

- loader extraction and verification from the classifier JAR;
- clean-container/macOS load smoke test;
- remove release-test use of `java.library.path` and host library variables;
- archive exact tested classifier JARs for the release job.

Acceptance:

- deleting the CMake build directory before Java tests does not change results;
- a missing dependency, modified hash, wrong architecture, or wrong ABI fails
  before Worker readiness.

### WP4 — Split build parent, consumer BOM, runtime, and distribution

Dependencies: WP1; may run in parallel with WP2 after the staging contract is fixed.

Deliverables:

- `pangrova-parent`, consumer-only `pangrova-bom`,
  `pangrova-native-runtime`, and `pangrova-distribution`;
- move loader/JNI Java code out of Worker;
- move Arrow C Data dependency out of Worker;
- explicit platform dependency in each server distribution.

Acceptance:

- `pangrova-worker` contains no `native` declaration and no `System.load` call;
- a consumer can import the BOM without inheriting build extensions;
- each distribution contains exactly one platform bundle.

### WP5 — Introduce bootstrap handshake and generated protocol identity

Dependencies: WP3 and WP4.

Deliverables:

- hello/build-info messages and loader handshake;
- generated Java/C++ protocol identity helpers;
- feature-range negotiation and diagnostics;
- move rollout policy out of per-request compatibility checks.

Acceptance:

- compatible additive minor versions negotiate successfully;
- major, required-feature, descriptor-corruption, and build/platform mismatches
  fail before runtime initialization.

### WP6 — Introduce the small dynamically registered JNI ABI

Dependencies: WP5.

Deliverables:

- `NativeBindings` as the sole declaration class;
- C++ registration table in `JNI_OnLoad`;
- typed runtime/task/stream handles;
- generated JNI signature manifest and Java/C++ symmetry gate.

Acceptance:

- only `JNI_OnLoad`/`JNI_OnUnload` are exported;
- packaged ABI tests invoke every registered method;
- wrong-type, stale, and double-release handles fail deterministically.

### WP7 — Unify control requests, responses, status, and errors

Dependencies: WP6.

Deliverables:

- `InteropRequest`/`InteropResponse` oneof envelopes;
- task lifecycle, memory, cache, filter, result-server, and shuffle-server
  operations moved to the envelope;
- exhaustive enum use and one Java error mapper;
- remove null/empty-success and numeric-state conventions.

Acceptance:

- there is one control dispatcher on each side;
- malformed, unknown, expired, cancelled, and duplicate requests have golden
  typed errors and idempotency behavior.

### WP8 — Use the sole Native columnar ownership boundary

Dependencies: WP6.

Deliverables:

- Native task-local vectors and explicit shared pool/buffer owners; no parallel Java pointer-stream SPI;
- Arrow C Data schema/batch interoperability with explicit release, and standard Arrow streams at remote/persistence boundaries;
- unified write callback batch contract;
- backpressure, cancellation, EOF, and error semantics.

Acceptance:

- no heap `byte[]` copy on the steady-state local batch path;
- ASan passes release, cancellation, close-race, and JVM callback tests;
- allocation accounting returns to baseline after every test.

### WP9 — Remove feature JNI and custom binary paths

Dependencies: WP7 and WP8.

Deliverables:

- DiskANN build/search and BM25 execution routed through capability/task plans;
- vectorized typed JVM function callbacks;
- remove custom result blobs, `CUSTOM_BINARY`, status prefixes, and shuffle
  byte headers;
- delete unused legacy SPI, implementations, tests, and config.

Acceptance:

- the boundary inventory contains no feature-specific Native method;
- dead-stub and dependency-boundary gates pass with no baseline additions.

### WP10 — Release hardening and documentation convergence

Dependencies: WP2 through WP9.

Deliverables:

- reproducible build metadata, SBOM/provenance, source/Javadoc artifacts;
- glibc baseline and clean-host compatibility tests;
- updated architecture, build, deployment, JNI, and troubleshooting docs;
- remove all v1/removed-method descriptions and compatibility aliases.

Acceptance:

- one release candidate passes the complete matrix using only downloaded
  release artifacts;
- all docs and executable gates describe the same path.

## 8. CI and verification matrix

### 8.1 Per-PR gates

- canonical proto descriptor diff and reserved-field check;
- generated JNI registration/signature symmetry;
- Java 17 compile/test and Java 21 compatibility test;
- C++ release build, GTest, and warning-as-error gate;
- Linux x86_64 platform bundle and clean-container load test;
- JAR content, checksum, RPATH, dependency, and symbol audit;
- dead stubs, module boundaries, Checkstyle, SpotBugs, and diff coverage.

### 8.2 Nightly gates

- ASan and UBSan Native suites;
- repeated shutdown/re-init, callback attachment, and close-race stress tests;
- protocol parser fuzzing and malformed-envelope corpus;
- packaged E2E and smoke tests;
- leak/peak-memory and batch throughput regression thresholds;
- Linux aarch64 and both macOS targets when runner capacity is limited on PRs.

### 8.3 Release gates

For every target classifier:

1. Build from a pinned clean environment.
2. Run Native tests.
3. Install and audit the runtime closure.
4. Generate manifest, checksums, SBOM, and provenance.
5. Package the classifier JAR.
6. Start a clean runtime with only Java artifacts and that classifier JAR.
7. Run handshake, JNI ABI coverage, SQL E2E, result, shuffle, write, cancel,
   memory-pressure, and shutdown tests.
8. Publish exactly the artifact tested in step 7.

The release job must never rebuild Java or Native bits after the test job has
approved them.

## 9. Compatibility and rollout

The migration is performed vertically, one end-to-end path at a time. Temporary
dual paths are allowed only inside a single PR/branch and are removed before the
work package is complete; no permanent runtime feature flag selects a legacy
protocol.

Recommended rollout order:

1. packaging/load path;
2. bootstrap and ABI registration;
3. task create/start/status/cancel;
4. result stream;
5. write callback;
6. shuffle/exchange;
7. memory/cache/runtime control;
8. JVM functions and AI/index capabilities;
9. deletion of all legacy contracts.

Coordinator scheduling compares the negotiated protocol/capability identity in
Worker registration. A Coordinator must not dispatch a task whose required
features are absent. Rolling upgrades permit only negotiated additive minors;
major upgrades require a homogeneous Worker pool or explicit pool partitioning.

## 10. Definition of done

The refactor is complete only when all statements below are true:

- one canonical source tree generates every Pangrova-owned cross-runtime proto;
- one Java class declares a small dynamically registered JNI ABI;
- no feature-specific JNI entry point remains;
- every Native resource crossing JNI has a typed, generation-safe owner;
- local batches use one Arrow C Data/C Stream ownership model;
- remote batches use one authenticated Flight/IPC contract;
- all expected failures use `ExecutionError` and generated enums;
- all platform JARs contain a verified, relocatable runtime closure;
- packaged tests use the exact release JAR with no host library fallback;
- Worker Java code contains no Native loader/declaration or Arrow implementation
  dependency;
- no generated binary is tracked under source resources;
- protocol, build, CI, deployment, and architecture documentation agree;
- the repository's full eight-step Native/Java/E2E/smoke validation chain passes.

## 11. Reference implementation links

- [Velox4J README and release/build model](https://github.com/boostscale/velox4j)
- [Velox4J Maven native-resource and shading configuration](https://github.com/boostscale/velox4j/blob/main/pom.xml)
- [Velox4J runtime dependency installation](https://github.com/boostscale/velox4j/blob/main/src/main/cpp/main/CMakeLists.txt)
- [Velox4J `$ORIGIN` runpath normalization](https://github.com/boostscale/velox4j/blob/main/src/main/cpp/build.sh)
- [Velox4J classpath Native loader](https://github.com/boostscale/velox4j/blob/main/src/main/java/org/boostscale/velox4j/jni/JniLibLoader.java)
- [Velox4J dynamic JNI registration](https://github.com/boostscale/velox4j/blob/main/src/main/cpp/main/velox4j/jni/JniLoader.cc)
- [Velox4J Arrow C Data bridge](https://github.com/boostscale/velox4j/blob/main/src/main/java/org/boostscale/velox4j/arrow/Arrow.java)
- [Velox4J Native object store](https://github.com/boostscale/velox4j/blob/main/src/main/cpp/main/velox4j/lifecycle/ObjectStore.h)
