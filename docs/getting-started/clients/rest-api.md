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
| `GET` | `/v1/query/profile/{queryId}/json` | Get structured query profile (JSON) |
| `GET` | `/v1/health` | Health check (`503` when not ready) |
| `POST` | `/v1/workers/register` | Worker registration (internal) |
| `POST` | `/v1/worker/heartbeat` | Worker heartbeat (internal) |
| `POST` | `/v1/streaming/cdc/register` | Register a CDC stream consumer (internal) |
| `POST` | `/v1/streaming/cdc/heartbeat` | CDC stream heartbeat (internal) |

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
  "result_endpoint": {
    "host": "worker-1",
    "port": 9090,
    "ticket": "exchange-ticket-xyz",
    "protocol": "arrow_flight"
  },
  "column_names": ["id", "name", "amount"]
}
```

The `result_endpoint` points to the Worker serving results via Arrow Flight
transport (`ticket` identifies the Flight ticket; `protocol` identifies the
result transport). The `session_properties` field is optional and defaults to an
empty map. Retried submissions can be deduplicated by sending an
`Idempotency-Key` header; a retry with the same key resolves to the same query.

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
  "error": null,
  "stats": { "elapsed_time_ms": 1234, "queued_time_ms": 12, "rows_returned": 100 },
  "result_endpoint": { "host": "worker-1", "port": 9090, "ticket": "...", "protocol": "arrow_flight" }
}
```

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
  "error_type": null,
  "message": "Query not found: invalid-id"
}
```

| HTTP Status | Error Code | Meaning |
|-------------|-----------|---------|
| 400 | `INVALID_REQUEST` | Malformed request (missing query field) |
| 404 | `NOT_FOUND` | Query ID not found |
| 409 | `CONFLICT` | Unregistered worker (registration conflict) |
| 500 | `INTERNAL_ERROR` | Server-side failure |

### Authentication Headers

| Header | Description |
|--------|-------------|
| `Authorization: Bearer <jwt-token>` | JWT authentication |
| `Authorization: Basic <user:password>` | Password authentication |
| `X-Pangrova-Api-Version` | Requested API version; responses always echo the negotiated version |
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
1. POST /v1/query/submit → get query_id + result_endpoint
2. Connect to result_endpoint via Arrow Flight
3. Stream result batches (Arrow RecordBatch)
4. Close Flight connection when done
```

This architecture avoids loading entire result sets into Coordinator memory.

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
| Slow first query | JIT warmup on Coordinator | Expected; subsequent queries will be faster |
| `Query not found` after submit | Query was removed or results are no longer available | Inspect query status and result authorization deadline; Worker terminal retention does not extend either |
