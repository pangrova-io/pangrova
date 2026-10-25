# Multimodal file-plane implementation and remaining production gates

This records the file-plane work against the Databricks/Snowflake alignment design
as of 2026-10-25. It is an implementation checkpoint, not a production acceptance
or a claim that the whole design is complete. Changes in other engine areas need
their own current-source qualification.

## Implemented and wired

- Volume and Stage definitions, immutable identity, DDL, metadata persistence,
  grants, authorization and audit; SQL `LIST` and `REMOVE`; managed roots retain
  identity across rename and are not reused by a new object with the same name.
- Coordinator-issued short-lived, credential-free file operation descriptors;
  Worker LOCAL/S3 streaming access, metadata, listing, conditional deletion,
  ranged reads, HTTP HEAD, version conditions and provider presigned URLs.
- Stable Stage URLs resolve the immutable Stage identity and recheck current
  authorization on every GET/HEAD before redirecting to a short-lived Worker URL.
  The previously issued Worker URL remains usable until its own expiry.
- Manual Stage refresh publishes a durable directory index atomically only after
  a complete listing. Refresh attempts are fenced. A Worker-side PostgreSQL page
  reader supports bounded pages, a consistent snapshot, cancellation and deadlines.
  This page reader is not wired to a Native SQL Directory operator.
- Multipart sessions now have a signed 24-hour absolute expiry independent of
  short-lived request grants. Tokens bind the complete storage scope and path;
  renewal requires a fresh authorized grant and can continue on another Worker.
  Expired cleanup retains ownership on failure for a subsequent retry.
- LOCAL multipart parts use content-based SHA-256 ETags and atomic replacement.
  Truncated/oversized/interrupted writes preserve the previously accepted part.
  Completion checks actual bytes against the client's part ETags and sizes before
  publication; rejected validation preserves the object and retryable parts.

The behavior and API details are in
[Volumes, Stages and file access](../getting-started/catalogs/volumes-stages-files.md).

## Validation evidence

The focused Maven reactor selection completed with **91 tests, zero failures,
errors or skips**, covering the following classes against real PostgreSQL,
production HTTP resources and the packaged Native library where applicable:

- `JdbcFileSpaceRegistryTest` (12), `JdbcFileDirectoryIndexTest` (8),
  `JournalSqlOperationTest` (1), `JdbcQueryLineageOutboxIntegrationTest` (9),
  `JdbcCopyFrozenInputLineageTest` (5).
- `FileSpaceAccessServiceTest` (10), `FileUrlServiceTest` (4),
  `WorkerFileClientTest` (6), `StageDirectoryRefreshServiceTest` (3),
  `FileSpaceObjectStoreTest` (5).
- `StageFileUrlHttpIntegrationTest` (6), `FileDirectorySnapshotIntegrationTest` (8),
  `FileSpaceDdlE2ETest` (5), `FileSpaceControlPlaneContractE2ETest` (3),
  `ActivationSmokeE2ETest` (3), `SpiWiringVerificationTest` (3).

The subsequent multipart changes passed isolated JDK 17/JUnit checks using the
reactor dependency classpath, with zero failures or skips:

- `FileSpaceFileResourceTest`: 19; `S3ObjectStoreAccessorMinioTest`: 8 against a
  real MinIO container; `LocalObjectStoreAccessorTest`: 16;
  `FileSpaceHttpReadTest`: 20; `FileSpaceMultipartHttpTest`: 1.
- The new two-Worker renewal test fails against the pre-fix resource because the
  origin Worker deleted staged parts after request-grant expiry. It passes after
  the session lifetime repair.
- Three LOCAL retry/content regression cases fail against the pre-fix accessor
  and pass after the integrity repair.
- Scoped Checkstyle for both changed production classes passes. Earlier scoped
  file-plane Checkstyle, dead-stub/connectivity, SPI annotation and E2E
  truthfulness checks passed. These are not a passing repository-wide static gate.

The two HTTP Worker endpoints above run in one JVM. This establishes routing and
shared-storage behavior, not independent-process recovery or distributed qualification.

The broader default-profile metadata/coordinator/worker owner reactor was
interrupted during quota wrap-up with exit 130. Metadata completed 514 tests with
zero failures, errors or skips; this profile excludes integration-tagged metadata
tests. Earlier dependency modules also completed successfully. The run had not
reached the Coordinator and Worker owner suites. This is an incomplete reactor,
not a passing broader gate; rerun the affected owners and full release profile.
The earlier owner run stopped with two metadata errors; their root causes
(journal double cleanup and an empty migration update advancing the authorization
revision) were fixed, and the affected focused tests subsequently passed.

## Remaining work against the design

| Work package | Remaining implementation / acceptance |
|---|---|
| M0: definitions and governance | Finish the full cross-tenant, external-location privilege, audit and migration acceptance matrix; retain real database and authenticated HTTP evidence. Existing passing cases do not cover every combination. |
| M1: transfers and URLs | SQL URL functions and SQL/client PUT/GET surfaces; scoped URLs and S3 AssumeRole/STS credentials; durable multipart ownership/recovery and autonomous reclamation; S3 large-object and 100 GiB multipart acceptance. The LOCAL 5 GiB HTTP gate is recorded separately below and does not qualify these other backends or multipart sizes. |
| M2: queryable files and ingestion | Wire `DIRECTORY(@stage)` through planner → typed v2 → Worker → JNI → Native with the exact six columns; implement URI `COPY INTO`, `read_files` with the eight requested formats, external-table DDL/readers, write-through index updates and actual `FILE MANAGED` copy/publication. FILE modifiers and index/page-reader foundations are not those completed execution paths. |
| M3: Iceberg v3 and AI | Verify every requested Iceberg v3 feature and AI function against the latest implementation and real providers, including FILE authorization, modality/size limits and exact SQL semantics. Changes and tests in other workstreams are not automatically qualification of the design's full matrix. |
| M4: lifecycle and operations | Managed DROP physical-file semantics, external-file preservation, automatic refresh, events/Inventory, durable restart/checkpoint recovery, per-tenant/space byte and concurrency quotas, backlog/coverage and latency metrics, documented operational limits. Manual refresh and opportunistic multipart cleanup do not satisfy this package. |
| M5: production scale and clients | OSS/GCS/ADLS implementations and consistency tests; 100-million-entry directory qualification; real client REST/JDBC/CLI E2E; fault/restart/multi-process tests and deployment/runbook verification. |
| Release acceptance | Full affected Java/Native/JNI chain on the final source, required sanitizer checks, independent-process/multi-node failures and release full regression. Native smoke tests do not replace these gates. |

## Next implementation boundary

The next main vertical slice is the Native Directory execution path. Resolve and
authorize the immutable Stage reference in the control plane, propagate a typed
binding and publish capability only when the Worker/Native path is actually wired.
Construct the six columns in Native and verify SELECT, predicates and joins with
real directory snapshots and authorization changes. Do not return relational
query rows from Coordinator Java or register a placeholder table function.

Then connect ingestion and FILE managed writes to the existing fenced publication
mechanisms before claiming the design's first complete upload → DIRECTORY → COPY
→ FILE/AI query loop. Keep the remaining size, scale, backend and recovery gates
explicit until executed successfully.

## Reproducing the large-object gate

`FileSpaceLargeObjectIntegrationTest` transfers a real 5 GiB LOCAL object through
HTTP, compares full upload/download SHA-256, checks HEAD length, verifies bytes
across the 4 GiB boundary and rejects a stale ETag. Allow at least 6 GiB of free
scratch space and several minutes; temporary files are removed by JUnit. This is
not a sparse-file or mocked byte-count check and does not qualify S3 large objects
or 100 GiB multipart.

```bash
mvn -pl pangrova-worker -am -Pfull-regression \
  -Dtest=FileSpaceLargeObjectIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The initial standalone harness passed on 2026-10-25 with a 256 MiB Java heap,
transferring and downloading all 5,368,709,120 bytes. Upload and download SHA-256
were `7535f0d34d4af8a46292dbb5958215503907cd0bd269ada090dfc59ff3896d6f`.
HEAD, the range crossing 4 GiB, and the stale-ETag 412 check passed; its temporary
object was removed. The final JUnit integration test also passed: one test,
zero failures/errors/skips, 267 seconds with a 256 MiB heap. Its position-dependent
pattern includes high offset bits, so a 32-bit range-offset wrap cannot return
matching bytes. JUnit removed the temporary 5 GiB object after the test.

The implementation goal was paused at the user's requested 10% remaining-account
quota boundary. All file-plane changes in this checkpoint were committed and
pushed on `codex/ai-native-semantic-closure`; the implementation and release gaps
above remain open.
