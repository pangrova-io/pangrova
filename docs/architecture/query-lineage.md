# Query lineage publication

A logical query emits START from `QueryManager.submitQuery`, after its local state and
connector authority have been registered, within the submitting trace scope, before
admission or execution can terminate it. `QueryTerminalCleanup` owns the single logical
terminal effect: FINISHED maps to COMPLETE, FAILED to FAIL, CANCELLED to ABORT. Required
write commit finishes before the task completion owner can signal logical success.
Task completion and query dispatch do not publish another logical query event.

The `QueryEventDispatcher` SPI carries immutable dataset and column metadata captured
from the authorized plan. A manager attaches its listener through `withListener`; a
borrowed parent retains its dynamic registrations and never retains a child's listener.
A lifecycle callback error remains observable and fault isolated. Other SPI listeners
receive the same query events regardless of whether the optional audit writer exists.

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

Focused integration tests deploy the real Marquez 0.50.0 HTTP API with real PostgreSQL,
send through the production OpenLineage SDK, then query the reference backend's actual
`lineage_events` journal. They retain UUID and success/failure assertions and cover
START/terminal cardinality, cancellation, structured dataset names, SQL/error facets,
derivation categories, and independent listener owners.

This unit does not establish lossless delivery: the existing bounded in-memory queue
can drop events and its shutdown drain can report failed sends. The existing lineage
capture TTL/capacity and unsupported relational node approximation also require a
separate durable/correctness closure. Real full query coordinator/JNI execution and
owning module regression are mandatory; source completion or leaf ingestion tests
alone do not qualify the whole governance chain.
