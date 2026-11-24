---
title: "REST API"
description: "Query submission, lifecycle, and error endpoints"
---

# REST API

Submit and manage queries over the Coordinator HTTP API.


## REST API

### Base URL

```
http://<coordinator-host>:<port>/v1
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/query/submit` | Submit a SQL query |
| `GET` | `/v1/query/{queryId}/state` | Get query execution state |
| `POST` | `/v1/query/{queryId}/cancel` | Cancel a running query |
| `GET` | `/v1/query/profile/{queryId}` | Get query flame graph (SVG) |
| `GET` | `/v1/query/profile/{queryId}/json` | Get audit totals and retained Native task-attempt counters |
| `GET` | `/v1/health` | Health check (`503` when not ready) |
| `POST` | `/v1/workers/register` | Worker registration (internal) |
| `POST` | `/v1/worker/heartbeat` | Worker heartbeat (internal) |
| `POST` | `/v1/streaming/cdc/register` | Register a CDC stream consumer (internal) |
| `POST` | `/v1/streaming/cdc/heartbeat` | CDC stream heartbeat (internal) |

### Query Profile

Query profiles are scoped to the authenticated submitting principal. The Web UI lists
that principal’s active queries. An available SVG profile requires a live local query.
The JSON profile returns query audit timing and memory totals. Each element of `attempts`
identifies an issued stage, partition, plan epoch, task attempt, and Worker. `reportState`
and `reportSequence` identify the measured snapshot. The attempt includes measured output
backpressure and per-operator counters in nanoseconds, bytes, and rows. Attempts for a
retry and its predecessor appear separately. If the Coordinator retains no attempt
counters, `attempts` is empty and `telemetryAvailable` is false.

Each operator reports `blockedNanos` and the selected reasons
`waitForProducerNanos`, `waitForConsumerNanos`, `waitForSplitNanos`, and
`waitForConnectorNanos`. Velox accumulates these intervals across that
operator's drivers. Drivers and operators can overlap, so the counters are
distinct from query wall time. An Exchange can report split wait while it
can still receive either an input announcement or a data page. The connector
counter measures time awaiting a connector future. Object request latency
and decoding time use their own measurements.

`executionGraph` describes the physical Stage and Exchange topology registered by the
owning Coordinator. It includes planned parallelism, Exchange distribution and read
mode, and observed stage transitions with retry generation. Transition `elapsedNanos`
uses one Coordinator monotonic clock and starts at graph registration. The graph is
retained with the in-process telemetry and can be absent from historical profiles.
Planned widths and observed attempt counts are separate measurements.
`selectedAttempts` identifies physical successes accepted by a logical task slot
and stage generation. A speculative copy can settle its original logical slot.
The record carries both task IDs and the physical attempt’s plan epoch. It can
include earlier stage generations from a retried query.

`runtimeFilterWait` records the single cross-stage build decision before the probe
begins enumerating splits. It identifies the build and probe stages, the probe
generation, and the range count. The wait runs from `startedNanos` to
`finishedNanos` on the graph's Coordinator clock. Outcomes are `APPLIED`,
`EMPTY_BUILD`, `MISSING_SUMMARY`, `TIMED_OUT`, and `QUERY_TERMINATED`. An absent
entry means the graph recorded no completed build wait. The interval can overlap
Native execution and the acknowledgement of already-issued tasks.

`downstreamPreparations` describes work submitted to the live read preparation
executor. Each entry covers one executor submission, which can handle multiple
coalesced wakeups. The `submittedNanos`, `startedNanos` and `finishedNanos`
offsets share the graph's monotonic origin. A queued entry has only a submission
offset. A rejected entry also has a finish offset. The other outcomes are
`RUNNING`, `COMPLETED`, `SKIPPED` (inactive when drained), and `FAILED`.
Queue time is the start minus submission, and execution time is the finish
minus start. Parallel work can overlap these intervals.

`stageDispatchWork` contains measured intervals for each live stage and
generation. The phases are `SERIALIZE_PLAN`, `ENUMERATE_SPLITS`,
`VALIDATE_AND_BIND_PLAN`, `SUBMIT_TASKS`, and `OBSERVE_COHORT_ACK`.
The item count identifies enumerated splits or submitted task partitions as
applicable. The ACK interval starts after submission and measures how long the
Coordinator still waits for the cohort; it uses the same clock as the stage
transitions. The interval reports whether the cohort acknowledged successfully.

### Submit a Query

Request:

```bash
curl -X POST http://localhost:8080/v1/query/submit \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "query": "SELECT * FROM catalog.schema.table LIMIT 10",
    "session_properties": {
      "catalog": "iceberg_prod",
      "schema": "analytics"
    }
  }'
```

Response:

```json
{
  "query_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "QUEUED",
  "result_endpoint": null,
  "column_names": []
}
```

The initial receipt can have a null `result_endpoint` and no column names while
planning is pending. Poll the state endpoint until the result endpoint is available.
It identifies the Worker serving Arrow Flight results (`ticket` is opaque and
`protocol` identifies the transport). Consume a readable `RUNNING` or `FINISHING`
stream before waiting for `FINISHED`; bounded output can require an active reader
to finish. The `session_properties` field is optional and defaults to an
empty map. When an authenticated principal submits an `Idempotency-Key`, the
Coordinator retains the original submission receipt for ten minutes after completion.
A retry with the same SQL and session properties returns that receipt, including its
original state and result endpoint. Poll the state endpoint for later transitions.
Changing the SQL or session properties with the same key returns HTTP 409. Each
principal has an independent key space. Concurrent matching requests share the first
submission. Receipt expiry is processed in bounded batches during subsequent submissions.

A rejection before query registration, such as a draining Coordinator, releases the
key. If the submission throws after registration or registration cannot be confirmed,
the Coordinator preserves the error for the same retention window and returns
`X-Pangrova-Query-Id` so the client can look up the execution. These guarantees cover
one running Coordinator. Restart and failover discard its in-memory submission keys.

### Query Lifecycle

```
Submit → QUEUED → PLANNING → SCHEDULING → RUNNING → FINISHING → FINISHED
                                                             ↘ FAILED
                              CANCELLED ← (any non-terminal state)
```

States `FAILED` and `CANCELLED` can be reached from any non-terminal state.

### Get Query State

```bash
curl http://localhost:8080/v1/query/a1b2c3d4-e5f6-7890-abcd-ef1234567890/state
```

Response:

```json
{
  "query_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "FINISHED",
  "stats": { "elapsed_time_ms": 1234, "queued_time_ms": 12, "rows_returned": 100 },
  "result_endpoint": { "host": "worker-1", "port": 9090, "ticket": "...", "protocol": "arrow_flight" }
}
```

Absent optional `error`, `stats`, and `result_endpoint` fields are omitted from
state responses rather than serialized as null. Field availability depends on
the current query state and retained execution evidence.

### Cancel a Query

```bash
curl -X POST http://localhost:8080/v1/query/a1b2c3d4-e5f6-7890-abcd-ef1234567890/cancel
```

Response (HTTP 202):

```json
{
  "query_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "cancelled": true,
  "outcome": "CANCEL_ACCEPTED"
}
```

### Error Responses

All error responses use a consistent format:

```json
{
  "error_code": "NOT_FOUND",
  "error_type": "USER_ERROR",
  "message": "Query not found: invalid-id"
}
```

| HTTP Status | Error Code | Meaning |
|-------------|-----------|---------|
| 400 | `BAD_REQUEST` | Malformed request (missing query field) |
| 404 | `NOT_FOUND` | Query ID not found |
| 409 | `CONFLICT` | Unregistered worker (registration conflict) |
| 422 | `API_VERSION_UNSUPPORTED` | Invalid or unsupported API version on a version-checked endpoint |
| 500 | `INTERNAL_ERROR` | Server-side failure |

### Authentication Headers

| Header | Description |
|--------|-------------|
| `Authorization: Bearer <jwt-token>` | JWT authentication |
| `Authorization: Basic <base64(user:password)>` | Base64-encoded UTF-8 credentials for password authentication; use TLS |
| `X-Pangrova-Api-Version` | Clients send their API version. The response advertises the server’s current version. |
| `X-Pangrova-Request-Id` | Client-generated request ID for tracing and idempotent retries |
| `Idempotency-Key` | Deduplicates retried `POST /v1/query/submit` requests |

### Health Check

```bash
curl http://localhost:8080/v1/health
```

Returns `200` when the node is ready and `503` otherwise:

```json
{
  "ready": true,
  "nodeType": "COORDINATOR",
  "checks": { "metadata": true, "storage": true },
  "lastProcessedInvalidationId": 0,
  "resultServerPort": 9090,
  "resultServerRunning": true
}
```

---

### REST API — Pagination and Large Results

For queries returning large result sets, the REST API returns a `result_endpoint` pointing to the Worker serving results via Arrow Flight transport. The client fetches results in batches:

```
1. POST /v1/query/submit and retain query_id.
2. Use the submit response if it has a readable result endpoint. Otherwise wait
   on the accepting Coordinator for that endpoint or the terminal outcome.
3. Consume Arrow Flight batches through EOF while the query is RUNNING or FINISHING.
4. Confirm FINISHED on the owner. FAILED or CANCELLED can arrive after partial output.
5. Close the result reader and client.
```

The Java SDK uses `beginQuery(sql, sessionProperties)` for a query execution
handle. Its status and cancellation requests stay on the endpoint that accepted
the submission. `awaitStatus(target, deadlineEpochMs)` takes a
`org.pangrova.common.enums.QueryWaitTarget` and requests that owner event. `drained` becomes true after the Coordinator records physical resource
release.

HTTP callers can use `GET /v1/query/{queryId}/state?afterVersion=0&waitMs=0`
to read the live owner's current version. Supply a positive `waitMs` up to 5000
and a stable UUID in `X-Pangrova-Query-Observer` to suspend the request. With
`until=NEXT_VERSION`, supply the last observed version in `afterVersion`.
`RESULT_OR_TERMINAL`, `TERMINAL`, and `DRAINED` wait for a readable result or
terminal state, the logical terminal state, or physical release, respectively.
Use `afterVersion=0` when starting a targeted wait without a prior status read.
Timeout returns the current snapshot. The Coordinator
allows one pending wait per observer, principal and query, with additional
principal, tenant and process limits. A request on a different Coordinator
receives `QUERY_OWNER_UNAVAILABLE`. Use the plain `GET /v1/query/{queryId}/state`
for diagnostic reads. State, wait, and cancellation requests are scoped to the
submitting principal. Historical diagnostics verify the original journal admission
and return the recorded state with version and drain evidence omitted.

Java SDK applications use `client.openResult(endpoint, deadlineEpochMs)` in
try-with-resources. Read the schema with `columns()`, advance with `next()`,
and read values with zero-based `value(column)`. The cursor decodes values
from the active Flight batch and returned values can outlive the batch.
After closing the cursor at EOF, use the query ID to confirm the terminal
outcome before reporting success.

For the Java REST client, `useTls(true)` with `trustStorePath` loads the configured
JKS/PKCS12 trust store into the HTTPS context. A store requires HTTPS; an orphan
store password, missing file, or invalid password is rejected. A supplied shared
`HttpClient` cannot be combined with a configured store because its immutable trust
policy cannot be overwritten. Result transport uses independent PEM credentials.
`httpExecutorThreads(n)` sets a positive owned HTTP executor size independent of
failover endpoint count. Combining it with a supplied shared `HttpClient` is
rejected instead of ignoring the executor configuration.
Closing seals HTTP admission and refuses to retire an executor while a real request
is active; retry after the request exits. JDK 17 exposes no HTTP socket close/drain
API, so executor termination must not be presented as a physical network receipt.
Query cancellation rejects HTTP authentication/authorization/server errors and
malformed or conflicting replies. Logical outcomes require the exact requested
`query_id`, boolean `cancelled` and outcome to match the service's 200/202/404
contract. `CANCEL_ACCEPTED`, `ALREADY_TERMINAL` and `NOT_FOUND` are not physical
execution or network-drain evidence; even a 404 from an unrelated proxy is rejected.

### Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `Connection refused` | Coordinator not running or wrong host/port | Verify with `curl http://host:port/v1/health` |
| `SQLException: query field is required` | Empty query string | Ensure query is not null or blank |
| `SSL handshake failed` | Wrong trust roots, missing client certificate, or hostname mismatch | REST HTTPS uses `sslTrustStore`/`sslTrustStorePassword`; result transport uses independent PEM `ResultTransportSecurity` trust roots and a paired client certificate/key for mutual TLS |
| Slow first query | Possible JVM warmup, cold caches, Native initialization, or storage latency | Inspect the query profile and logs; do not assume subsequent queries will be faster |
| `Query not found` after submit | Query was removed or results are no longer available | Inspect query status and result authorization deadline; Worker terminal retention does not extend either |
