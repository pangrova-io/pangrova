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

### Connection Properties

| Property | Default | Description |
|----------|---------|-------------|
| `token` | — | Bearer token for authentication |
| `ssl` | `false` | Enable SSL/TLS connection |
| `sslTrustStore` | — | Path to SSL trust store |
| `sslTrustStorePassword` | — | Trust store password |
| `connectTimeoutMs` | `5000` | TCP connect timeout in milliseconds |
| `requestTimeoutMs` | `30000` | HTTP request timeout in milliseconds |
| `retries` | — | Request retry count |
| `failover` | — | Failover coordinator endpoints |
| `poolSize` | `4` | Shared HTTP client thread pool size |

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

## Connection Pooling (JDBC)

The Pangrova JDBC driver shares a single `HttpClient` across all connections
created by the same driver instance. This avoids per-connection thread pool
overhead:

```java
// Both connections share the driver-level HTTP client internally
try (Connection c1 = DriverManager.getConnection("jdbc:pangrova://coordinator:8080/prod");
     Connection c2 = DriverManager.getConnection("jdbc:pangrova://coordinator:8080/prod")) {
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
| Timeout configuration | Tune `connectTimeoutMs`/`requestTimeoutMs` for production |
| Catalog selection | Specify catalog in URL to avoid fully-qualified table names |
| Error handling | Catch `SQLException` and check `getErrorCode()` for retry logic |
| SSL in production | Always enable `ssl=true` when security is enabled |
