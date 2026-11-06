# Query lineage publication

A logical query emits START from `QueryManager.submitQuery`, after its local state and
connector authority have been registered, within the submitting trace scope, before
admission or execution can terminate it. `QueryTerminalCleanup` owns the single logical
terminal effect: FINISHED maps to COMPLETE, FAILED to FAIL, CANCELLED to ABORT. Required
write commit finishes before the task completion owner can signal logical success.
Task completion and query dispatch do not publish another logical query event.

`pangrova-query-api` owns immutable query/split events (`org.pangrova.query.api.event`)
and `org.pangrova.query.spi.event.QueryEventListener`. Query Core owns
`org.pangrova.query.core.event.QueryEventDispatcher`; it invokes observers synchronously
on the publisher thread and isolates `java.lang.RuntimeException` from each listener. It does
not provide durable delivery, replay, deduplication, or exactly-once guarantees.

The actual `QueryAuditDispatcher` publishes admission/terminal observations and records
lineage facts directly through `LineageEventEmitter`. It does not install a second
`LineageQueryEventListener`. The independently tested listener adapter remains available
for explicit integrations; the borrowed-parent behavior of `withListener` is tested but
is not the production lineage publication path. A parent never retains a child's listener.
The JDBC event listener and split-completion publication are not wired by production
assembly. Their class presence must not be interpreted as an enabled audit pipeline.

Terminal inputs/outputs supplement admission metadata, as permitted by the OpenLineage
object model. Dataset and column identifiers remain structured even when they contain
dots. Existing direct, transformed and aggregate derivation categories are preserved;
the HTTP adapter does not label every expression IDENTITY. SQL is a job SQL facet and
query failure detail is a run errorMessage facet.

For correlated `UNNEST`, Calcite may type the correlation variable with the
left relation followed by the expanded output columns. Column origins and
derivation classification accept only an exact name/type match for the left
relation's field prefix and only references into that prefix. A missing owner,
reordered field or changed type still rejects lineage capture rather than
attributing the array to another input.

A zero-input table function such as `GENERATE_SERIES` produces derived output
columns with no physical source table columns. Calcite's column-origin metadata
records the empty source set for this leaf; lineage classification marks its
outputs as transformed. Table functions with inputs still require a known
column mapping and cannot be treated as source-free.

Governance bootstrap selects `pangrova.lineage.backend` explicitly (`LOG` by default to
preserve the existing log deployment; `HTTP` for an external backend). HTTP requires
`OPENLINEAGE_URL`; `OPENLINEAGE_API_KEY` is optional bearer authentication. The namespace
is `pangrova.lineage.namespace` (default `pangrova`). An invalid or absent configured
HTTP endpoint fails initialization and never changes to log delivery. The programmatic
HTTP constructor accepts the existing OpenLineage SDK HTTP configuration object. No YAML loading is
claimed by the environment constructor.

The emitter consumer owns its HTTP transport. Actual queued sends and shutdown drain
finish before the consumer closes the transport. A logical terminal event, a timeout,
or a cancel request is not evidence of physical query or transport drain.

Journal SQL cleanup may be attempted both by its caller and the background
reaper. The cleanup owner rechecks completion after acquiring exclusive cleanup
ownership, so a stale competing attempt cannot return the same SQL owner twice.
A failed physical connection close remains owned and can be retried.

Focused integration tests deploy the real Marquez 0.50.0 HTTP API with real PostgreSQL,
send through the production OpenLineage SDK, then query the reference backend's actual
`lineage_events` journal. They retain UUID and success/failure assertions and cover
START/terminal cardinality, cancellation, structured dataset names, SQL/error facets,
derivation categories, and independent listener owners.

On ARM64 Docker hosts, the upstream Marquez 0.50.0 AMD64 image runs under
emulation. Its request latency can exceed the integration tests' HTTP budgets.
Prepare a native Java 17 image before running these tests:

```bash
python3.12 .pangrova/scripts/prepare-lineage-test-image.py \
  --runtime-image eclipse-temurin:17-jre
mvn -pl :pangrova-metadata-core -am \
  -Dtest=QueryLineageHttpRecoveryIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dpangrova.test.marquez.image=<image-tag-from-preparation> test
```

Use a digest-qualified Java 17 runtime image for repeatable preparation. The
script copies the unmodified application and entrypoint from an immutable
upstream image, verifies the application's SHA-256, and emits the resulting
image ID, native architecture, source digest and runtime image ID. Preparation
containers remain stopped and are removed after use. The resulting local image
is used by the existing reference-backend fixture; PostgreSQL, real HTTP
ingestion, test deadlines and assertions are unchanged. Retain the receipt and
test reports with validation evidence. This removes emulation overhead; it does
not establish production backend throughput or cold-start latency.

This unit does not establish lossless delivery: the existing bounded in-memory queue
can drop events and its shutdown drain can report failed sends. The existing lineage
capture TTL/capacity and unsupported relational node approximation also require a
separate durable/correctness closure. Real full query coordinator/JNI execution and
owning module regression are mandatory; source completion or leaf ingestion tests
alone do not qualify the whole governance chain.
