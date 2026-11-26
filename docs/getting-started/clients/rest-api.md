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

This contract is REST API version 2. `GET /v1/server/info` advertises the
current and minimum supported versions in `api_version` and `min_api_version`.
Version 1 is rejected on version-checked endpoints because live query responses
require `result_disposition`.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/server/info` | Discover current and minimum supported API versions |
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
The JSON profile uses the owning Coordinator's retained terminal audit record.
It returns HTTP 404 until that record is available or after it leaves local retention.
Use `?history=true` on either JSON profile endpoint to read durable audit history.
The history lookup still requires the submitting principal.

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
  -H "X-Pangrova-Api-Version: 2" \
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
  "result_disposition": "PENDING",
  "result_endpoint": null,
  "column_names": []
}
```

`result_disposition` is required and has three values:

- `PENDING`: the query has not resolved whether it will expose a result stream.
- `STREAM`: the response carries an Arrow Flight `result_endpoint` for Worker
  output or Coordinator control-plane rows.
- `NONE`: the query intentionally returns no client result set, as with resultless
  DDL and DML completion.

`STREAM` always carries an endpoint, `NONE` never carries one, and `PENDING` cannot
survive `FINISHED`, `FAILED`, or `CANCELLED`. An empty `SELECT` is still `STREAM`.
The initial receipt can have no column names while planning is pending. Poll the
state endpoint until the disposition is `STREAM` or the query reaches a terminal
state. The endpoint identifies the host serving Arrow Flight results (`ticket`
is opaque and `protocol` identifies the transport); this can be a Worker or the
Coordinator for bounded control-plane rows. The protocol is canonical and must be
`grpc` or `grpc+tls`. Consume a readable `RUNNING`
or `FINISHING` stream before waiting for `FINISHED`; bounded output can require an
active reader to finish. The `session_properties` field is optional and defaults
to an empty map. When an authenticated principal submits an `Idempotency-Key`, the
Coordinator atomically records the principal, key digest, canonical request
fingerprint, lineage, and a deterministic `QUEUED`/`PENDING` acceptance receipt in
the initial journal row. A normal new acceptance performs one journal INSERT and no
readback. During the acceptance window, a matching retry returns the same query ID
and acceptance receipt without executing the SQL again; changing SQL or session
properties returns HTTP 409. Each principal has an independent key space.

Acceptance facts remain until ten minutes after the later of acceptance or terminal
completion. Once that window elapses, a new candidate may reuse the key with either
an identical or a different request, even before the periodic sweep removes its old
terminal acceptance. Active queries never expire. Conflict and uncertain-commit paths read the authoritative row and
compare its fingerprint; restart and failover do not discard idempotency state.
Poll the state endpoint for later transitions. A rejection before durable acceptance
releases the key, while a failure after acceptance remains attached to the accepted
query ID.

### Query Lifecycle

```
Submit → QUEUED → PLANNING → SCHEDULING → RUNNING → FINISHING → FINISHED
                                                             ↘ FAILED
                              CANCELLED ← (any non-terminal state)
```

States `FAILED` and `CANCELLED` can be reached from any non-terminal state.

### Get Query State

```bash
curl http://localhost:8080/v1/query/a1b2c3d4-e5f6-7890-abcd-ef1234567890/state \
  -H "X-Pangrova-Api-Version: 2"
```

Response:

```json
{
  "query_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "FINISHED",
  "result_disposition": "STREAM",
  "version": 7,
  "drained": false,
  "stats": { "elapsed_time_ms": 1234, "queued_time_ms": 12, "rows_returned": 100 },
  "result_endpoint": { "host": "worker-1", "port": 9090, "ticket": "...", "protocol": "grpc" }
}
```

Absent optional `error`, `stats`, and `result_endpoint` fields are omitted from
live state responses rather than serialized as null. `result_disposition`,
`version`, and `drained` are mandatory for live state responses. Field availability
otherwise depends on the current query state and retained execution evidence.

### Cancel a Query

```bash
curl -X POST http://localhost:8080/v1/query/a1b2c3d4-e5f6-7890-abcd-ef1234567890/cancel \
  -H "X-Pangrova-Api-Version: 2"
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
2. Use the submit response only when its disposition is STREAM. Otherwise wait
   on the accepting Coordinator for STREAM or the terminal outcome. NONE means
   there is no result reader to open.
3. Consume Arrow Flight batches through EOF while the query is RUNNING or FINISHING.
4. Confirm FINISHED on the owner. FAILED or CANCELLED can arrive after partial output.
5. Close the result reader and client.
```

The Java SDK uses `beginQuery(sql, sessionProperties)` for a query execution
handle. The submit receipt contains `owner_endpoint` with `coordinator_id`,
`incarnation`, `host`, and the bound HTTP `port`. An idempotent replay returns
the original receipt and owner. The SDK sends status and cancellation to that
origin when it matches an endpoint configured for the client. It uses the
configured HTTP scheme for the match. When the Coordinator cluster enables
query peer routing, the receipt also contains `owner_endpoint.route_ticket`.
For a shared ingress, pass it as `X-Pangrova-Query-Route` on status, cancellation,
and profile requests. The Java SDK carries the ticket on waits and cancellation
when it addresses the ingress. Use the submitting principal's credentials on
each request.
To reconnect from a fresh Java REST client, pass the saved receipt to
`resumeQuery(receipt)` using the original submitting principal's credentials.
`awaitStatus(target, deadlineEpochMs)` takes a
`org.pangrova.common.enums.QueryWaitTarget` and requests that owner event. Live
status responses include `version` and `drained`. The `drained` flag becomes
true after the Coordinator records physical resource release.

HTTP callers can use `GET /v1/query/{queryId}/state?afterVersion=0&waitMs=0`
to read the live owner's current version. Supply a positive `waitMs` up to 5000
and a stable UUID in `X-Pangrova-Query-Observer` to suspend the request. With
`until=NEXT_VERSION`, supply the last observed version in `afterVersion`.
`RESULT_OR_TERMINAL`, `TERMINAL`, and `DRAINED` wait for a readable result or
terminal state, the logical terminal state, or physical release, respectively.
Use `afterVersion=0` when starting a targeted wait without a prior status read.
Timeout returns the current snapshot. The Coordinator
allows one pending wait per observer, principal and query, with additional
principal, tenant and process limits. A configured ingress routes a ticketed
live request to the Coordinator incarnation that issued the receipt.
Live waits return `QUERY_OWNER_UNAVAILABLE` when the owner cannot serve the
request. Use `GET /v1/query/{queryId}/state?history=true`
for a journal lookup. Ordinary state reads use the local query projection.
State, wait, and cancellation requests are scoped to the
submitting principal. Historical diagnostics verify the original journal admission
and return only `query_id` and the recorded `state`. The journal does not retain
result disposition or endpoint authority, so historical responses omit
`result_disposition`, `result_endpoint`, `version`, and `drained` and must not be
used to open a result stream.

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
