---
title: Clients
sidebar_position: 3
description: JDBC, REST API, and CLI client usage
---

# Client Usage

Pangrova supports three client interfaces: JDBC, REST API, and CLI.

---

## JDBC Client

### Maven Dependency

```xml
<dependency>
  <groupId>org.pangrova</groupId>
  <artifactId>pangrova-client-jdbc</artifactId>
  <version>${pangrova.version}</version>
</dependency>
```

### Driver Class

```
org.pangrova.client.jdbc.PangrovaDriver
```

### Connection String

```
jdbc:pangrova://<host>:<port>[/<catalog>][?key=value&key2=value2]
```

Examples:

```
jdbc:pangrova://localhost:8080/iceberg_catalog
jdbc:pangrova://localhost:8080/iceberg_catalog?connectTimeout=5000
jdbc:pangrova://coordinator.prod:8080/prod_iceberg?ssl=true
```

The URL parser validates host (required) and port (1-65535, required). Catalog is optional — if omitted, queries must use fully-qualified table names (`catalog.schema.table`).

### Java Example

```java
String url = "jdbc:pangrova://localhost:8080/iceberg_catalog";
Properties props = new Properties();
// props.put("user", "admin");
// props.put("password", "secret");

try (Connection conn = DriverManager.getConnection(url, props);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT * FROM my_table LIMIT 10")) {
    while (rs.next()) {
        System.out.println(rs.getString(1));
    }
}
```

### Connection Properties

| Property | Default | Description |
|----------|---------|-------------|
| `user` | — | Username for authentication |
| `password` | — | Password for authentication |
| `ssl` | `false` | Enable SSL/TLS connection |
| `sslTrustStore` | — | Path to SSL trust store |
| `sslTrustStorePassword` | — | Trust store password |
| `connectTimeout` | `30000` | Connection timeout in milliseconds |
| `socketTimeout` | `0` | Socket timeout (0 = infinite) |

### SSL Configuration

```java
Properties props = new Properties();
props.put("ssl", "true");
props.put("sslTrustStore", "/path/to/truststore.jks");
props.put("sslTrustStorePassword", "changeit");

Connection conn = DriverManager.getConnection(
    "jdbc:pangrova://host:8080/catalog", props);
```

---

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
| `GET` | `/v1/health` | Health check |
| `POST` | `/v1/workers/register` | Worker registration (internal) |
| `POST` | `/v1/worker/heartbeat` | Worker heartbeat (internal) |
| `POST` | `/v1/streaming/cdc/start` | Start CDC pipeline (🔮 Planned) |

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
  "state": "RUNNING",
  "result_endpoint": {
    "host": "worker-1",
    "port": 9090,
    "token": "exchange-token-xyz"
  },
  "column_names": ["id", "name", "amount"]
}
```

The `result_endpoint` points to the Worker serving results via Arrow Flight transport. The `session_properties` field is optional and defaults to an empty map.

### Query Lifecycle

```
Submit → QUEUED → PLANNING → RUNNING → FINISHED
                                    ↘ FAILED
                         CANCELLED ←↙
```

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
  "result_endpoint": { "host": "worker-1", "port": 9090, "token": "..." }
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
| 409 | `CONFLICT` | Worker protocol version mismatch |
| 500 | `INTERNAL_ERROR` | Server-side failure |
```

### Authentication Headers

| Header | Description |
|--------|-------------|
| `Authorization: Bearer <jwt-token>` | JWT authentication |
| `X-Pangrova-User` | Username (internal auth mode) |
| `X-Pangrova-Catalog` | Default catalog for the session |
| `X-Pangrova-Schema` | Default schema for the session |

### Health Check

```bash
curl http://localhost:8080/v1/health
```

Response:

```json
{"status": "healthy", "version": "1.0.0"}
```

---

## CLI

The Pangrova CLI provides an interactive SQL shell.

### Launch

```bash
bin/pangrova-cli.sh --host localhost --port 8080
```

### Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host <host>` | `localhost` | Coordinator host |
| `--port <port>` | `8080` | Coordinator port |
| `--catalog <name>` | — | Default catalog |

### Interactive Mode

```
pangrova> SELECT 1 + 1 AS result;
+--------+
| result |
+--------+
|      2 |
+--------+
1 row in set (0.05 sec)

pangrova> SHOW CATALOGS;
```

### Script Mode

Pipe SQL from a file:

```bash
bin/pangrova-cli.sh --host localhost --port 8080 < queries.sql
```

Or use shell redirection:

```bash
echo "SELECT count(*) FROM my_table;" | bin/pangrova-cli.sh --host localhost --port 8080
```


---

## Connection Pooling (JDBC)

The Pangrova JDBC driver shares a single `HttpClient` across all connections created by the same driver instance. This avoids per-connection thread pool overhead.

```java
// All connections share the same HTTP client internally
DataSource ds = new PangrovaDataSource("jdbc:pangrova://coordinator:8080/prod");
try (Connection c1 = ds.getConnection();
     Connection c2 = ds.getConnection()) {
    // c1 and c2 share the same underlying HTTP thread pool
}
```

### Connection Lifecycle

```
DriverManager.getConnection(url, props)
  → PangrovaDriver.connect()
    → JdbcUrlParser.parse(url)  // validate host, port, catalog
    → PangrovaRestClient.builder().build()
    → PangrovaConnection(restClient, catalog)
```

The connection is lightweight — it holds a reference to the shared `PangrovaRestClient` and the catalog name. Closing a connection does not shut down the shared HTTP client.

### Best Practices

| Practice | Recommendation |
|----------|---------------|
| Connection pooling | Use HikariCP or similar; Pangrova connections are lightweight |
| Timeout configuration | Set `connectTimeout=5000` for production |
| Catalog selection | Specify catalog in URL to avoid fully-qualified table names |
| Error handling | Catch `SQLException` and check `getErrorCode()` for retry logic |
| SSL in production | Always enable `ssl=true` when security is enabled |

### REST API — Pagination and Large Results

For queries returning large result sets, the REST API returns a `result_endpoint` pointing to the Worker serving results via Arrow Flight transport. The client fetches results in batches:

```
1. POST /v1/query/submit → get query_id + result_endpoint
2. Connect to result_endpoint via Arrow Flight
3. Stream result batches (Arrow RecordBatch)
4. Close Flight connection when done
```

This architecture avoids loading entire result sets into Coordinator memory.

### Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `Connection refused` | Coordinator not running or wrong host/port | Verify with `curl http://host:port/v1/health` |
| `SQLException: query field is required` | Empty query string | Ensure query is not null or blank |
| `SSL handshake failed` | Wrong trust store or self-signed cert | Configure `sslTrustStore` and `sslTrustStorePassword` |
| Slow first query | JIT warmup on Coordinator | Expected; subsequent queries will be faster |
| `Query not found` after submit | Query completed and result expired | Increase `worker.task.result-retention-seconds` |
