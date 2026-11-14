---
title: "JDBC"
description: "JDBC driver, connection strings, and pooling"
---

# JDBC

Connect to Pangrova from Java applications through the JDBC driver.


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
jdbc:pangrova://localhost:8080/iceberg_catalog?connectTimeoutMs=5000
jdbc:pangrova://coordinator.prod:8080/prod_iceberg?ssl=true
```

The URL parser validates host (required) and port (1-65535, required). Catalog is optional — if omitted, queries must use fully-qualified table names (`catalog.schema.table`).

### Java Example

```java
String url = "jdbc:pangrova://localhost:8080/iceberg_catalog";
Properties props = new Properties();
// props.put("token", "<bearer-token>");

try (Connection conn = DriverManager.getConnection(url, props);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT * FROM my_table LIMIT 10")) {
    while (rs.next()) {
        System.out.println(rs.getString(1));
    }
}
```

### Prepared Parameter Admission

Supported prepared-statement setters accept only the actual one-based parameter
markers in the SQL text. Question marks in quoted strings, quoted identifiers,
and comments are not markers. An invalid index fails immediately with SQLState
`07009` without changing existing bindings. Setters and `clearParameters()` reject
a closed statement, as required by the
[JDK 17 PreparedStatement contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/PreparedStatement.html).
These checks do not add server-side prepared statements or implement unsupported
typed conversion, batching, or calendar-specific temporal behavior.

### UUID Result Values

Results declared with the canonical Arrow `arrow.uuid` extension are exposed by
`getObject()` as `java.util.UUID`, and by `getString()` as the canonical UUID
string. JDBC metadata reports `Types.OTHER`, type name `UUID`, display width 36,
and precision and scale 0. Untagged 16-byte binary values remain binary values.
This describes client result decoding; SQL and connector UUID support requires
separate end-to-end validation.

### Connection Properties

| Property | Default | Description |
|----------|---------|-------------|
| `token` | — | Bearer token for authentication |
| `user` / `password` | — | UTF-8 Basic authentication; both required and mutually exclusive with `token` |
| `ssl` | `false` | Use REST HTTPS and require the actual result endpoint to advertise TLS |
| `sslTrustStore` | — | REST HTTPS JKS/PKCS12 trust store |
| `sslTrustStorePassword` | — | REST trust store password |
| `resultTlsRootCa` | — | PEM result-channel trust roots; requires TLS on that channel |
| `resultTlsCertificateChain` / `resultTlsPrivateKey` | — | Paired PEM result client certificate and key; explicit `resultTlsRootCa` is also required |
| `connectTimeoutMs` | `5000` | TCP connect timeout in milliseconds |
| `requestTimeoutMs` | `30000` | HTTP request timeout in milliseconds |
| `retries` | — | Request retry count |
| `failover` | — | Failover coordinator endpoints |
| `poolSize` | `4` | Positive HTTP executor thread count, independent of failover endpoints |

Both HTTP timeout properties must be positive and at most `Long.MAX_VALUE / 2`
milliseconds (`4611686018427387903`). The REST configuration validates this
supported JDK timer window before allocating its HTTP owner. The bound leaves
signed-long millisecond arithmetic headroom; it does not guarantee correctness
under arbitrary wall-clock jumps. Java REST timeout durations need not fit
signed-long nanoseconds. Retry delay durations must fit signed-long milliseconds,
the actual jitter/sleep unit, and a retry count of `Integer.MAX_VALUE` still sends
the initial request without overflowing the attempt count.

Failover retries retain the endpoint selection captured when their HTTP request
was built and visit the configured endpoints in order. Concurrent failures only
advance the matching current selection; revisiting an endpoint cannot let an old
failure rewind routing. SQL submissions remain non-idempotent single-send requests,
including when an endpoint returns HTTP 503 and healthy alternatives are configured.

Standard `DriverManager.getConnection(url, user, password)` now uses the same Basic
authentication contract. Use TLS for credentials. Prefer `Properties` to embedding
secrets in a URL; URL property components use UTF-8 form decoding, so encode reserved
characters such as `&`, `=`, `+`, and `:` when they occur in property values.

Result PEM credentials are separate from the REST Java trust store. They retain
certificate and endpoint hostname verification and never rewrite the advertised
endpoint. `Connection.close()` seals new statement admission, cancels active
queries, closes its statements and their fetched results, and then closes the
transport. If cancellation fails or an execution is still running, the connection
keeps its HTTP owner available to retry cancellation. Retry close after the
statement exits. A closed-for-admission connection is not itself a physical drain receipt.

### SSL Configuration

An explicit `sslTrustStore` requires `ssl=true` and is loaded into that connection's
HTTPS context. A missing store or incorrect password fails connection construction;
it never falls back to the driver's shared system-trust client. Connections without
an explicit store retain the shared system-trust client. REST stores are not result
channel PEM credentials.

Explicit trust-store connections own an executor of exactly `poolSize` threads
(including the default of 4). System-trust connections share the driver's first
executor: a later explicit conflicting `poolSize` is rejected; omitting it reuses
the established size. Invalid sizes fail before client allocation instead of
falling back. This is executor ownership, not a TCP connection-limit or socket
drain guarantee: JDK 17 `HttpClient` has no public close/termination API.
Shared-executor shutdown first seals admission and waits outside its ownership
monitor. Timeout or interruption reports failure while retaining the original
client and executor for retry; interruption is preserved. New shared connections
remain rejected until that actual executor terminates successfully.
Driver deregistration is not yet wired to connection-aware shared-owner retirement.
It must not blindly close the global executor: already opened connections and
other registered driver instances can still borrow that same owner. Automatic
deregistration/shutdown needs connection leases; the explicit termination barrier
does not establish that separate lifecycle contract.

When `Statement.cancel()` races with a pending submission, it waits for that
invocation's submit response and addresses the returned query ID. A failed
submission with no confirmed query ID raises `SQLException` to the waiting
caller. A cancellation on an idle statement returns immediately.

`Statement.cancel()` retains the currently active query identity if its HTTP request
fails or its acknowledgement is malformed, so an explicit retry still addresses
the same query. Only a validated, matching `CANCEL_ACCEPTED` or `ALREADY_TERMINAL`
response retires that identity; a delayed response cannot clear a newer query.
`NOT_FOUND` raises `SQLException` and leaves an active identity available for retry.
It proves only that the contacted coordinator did not find the query, not that an
older coordinator in an HA deployment cancelled it. These logical acknowledgements
are not evidence of Worker resource drain, result-channel exit, or socket closure.

The client's catalog discovery consumes the REST namespace endpoints. Schemas are listed
directly, including empty schemas; tables and columns retain their full catalog/schema
identity. JDBC search patterns use `%`, `_`, and the reported `\` escape, not regular
expressions. Filtered columns keep their original one-based ordinal and declared
type parameters. HTTP authorization, missing-table, transport, and malformed metadata
responses raise `SQLException` with the original cause; only valid empty payloads
produce empty metadata. Unknown type information is not guessed as VARCHAR.
These are client protocol semantics, not a production-readiness claim for the catalog
server. The current Coordinator discovery implementation still requires real table
enumeration, complete connector schema declarations, and authenticated tenant/RBAC
session wiring; HTTP fixture tests do not establish that server-side chain.

```java
Properties props = new Properties();
props.put("ssl", "true");
props.put("sslTrustStore", "/path/to/truststore.jks");
props.put("sslTrustStorePassword", "changeit");

Connection conn = DriverManager.getConnection(
    "jdbc:pangrova://host:8080/catalog", props);
```

Close refuses to release a configured HTTP executor while a real request remains
active; new HTTP admission is sealed and close can be retried after the request exits.
Successful executor termination is not proof that all JDK 17 HTTP sockets have
physically closed: JDK 17 `HttpClient` has no public close/drain API.

---

## Connection Pooling (JDBC)

The Pangrova JDBC driver shares a single system-trust `HttpClient` across connections
without an explicit trust store. Configured trust-store connections own their HTTPS context.
Sharing the system-trust client avoids per-connection thread pool overhead:

```java
// Both connections share the driver-level HTTP client internally
try (Connection c1 = DriverManager.getConnection("jdbc:pangrova://coordinator:8080/prod");
     Connection c2 = DriverManager.getConnection("jdbc:pangrova://coordinator:8080/prod")) {
    // c1 and c2 share the same underlying HTTP thread pool
}
```

### Connection Lifecycle

`Connection.isValid(timeout)` checks the actual `/v1/health` response and accepts
only HTTP 200. Unready 503, authorization errors, missing endpoints, redirects,
and other success codes are not a valid connection. The probe sends configured
Basic/Bearer credentials through the same header builder as other REST requests.
The Coordinator itself exempts health from authentication; this preserves client
credential consistency for authenticating proxies, not a backend-login guarantee.
This health check is not proof of query drain or physical socket closure.

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
| Timeout configuration | Tune `connectTimeoutMs`/`requestTimeoutMs` for production |
| Catalog selection | Specify catalog in URL to avoid fully-qualified table names |
| Error handling | Catch `SQLException` and check `getErrorCode()` for retry logic |
| SSL in production | Always enable `ssl=true` when security is enabled |
