package org.pangrova.client.jdbc;

import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.client.rest.ConnectionPoolConfig;
import org.pangrova.client.rest.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JDBC Driver for Pangrova distributed query engine.
 * Auto-registered via {@code META-INF/services/java.sql.Driver}.
 *
 * <p>URL format: {@code jdbc:pangrova://<host>:<port>[/<catalog>][?property=value]}</p>
 *
 * <p>R10.5: A single {@link HttpClient} is shared across all JDBC connections
 * created by this driver, avoiding per-connection thread pool overhead.
 * The shared client is created lazily on first connection and shut down
 * when the driver is deregistered.</p>
 */
public final class PangrovaDriver implements Driver {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaDriver.class);
    private static final int DEFAULT_HTTP_CLIENT_POOL_SIZE = 4;
    private static final Set<String> KNOWN_PROPERTIES = Set.of(
            "token", "ssl", "sslTrustStore", "sslTrustStorePassword",
            "failover", "retries", "connectTimeoutMs", "requestTimeoutMs",
            "poolSize");
    private static final Set<String> SECURITY_PREFIXES = Set.of("ssl", "auth");

    /**
     * Shared HttpClient instance across all JDBC connections.
     * Lazily initialized on first {@link #connect} call.
     * Thread-safe: HttpClient is immutable after construction.
     */
    private static volatile HttpClient sharedHttpClient;
    private static volatile ExecutorService sharedExecutor;
    private static final Object CLIENT_LOCK = new Object();

    static {
        try {
            DriverManager.registerDriver(new PangrovaDriver());
            LOG.debug("event=pangrova_driver_registered");
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            JdbcConnectionParams params = JdbcUrlParser.parse(url);
            Map<String, String> mergedProps = mergeProperties(params.properties(), info);
            validateProperties(mergedProps);

            int poolSize = parsePoolSize(mergedProps);
            HttpClient client = getOrCreateSharedHttpClient(poolSize);
            PangrovaRestClient.Builder builder = PangrovaRestClient.builder()
                    .coordinatorHost(params.host())
                    .coordinatorPort(params.port())
                    .httpClient(client);
            applyProperties(builder, params.host(), params.port(), mergedProps);

            PangrovaRestClient restClient = builder.build();
            try {
                return new PangrovaConnection(restClient, params.catalog());
            } catch (Exception e) {
                restClient.close();
                throw e;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to connect to Pangrova: " + e.getMessage(), e);
        }
    }

    private static Map<String, String> mergeProperties(Map<String, String> urlProps, Properties info) {
        Map<String, String> merged = new LinkedHashMap<>(urlProps);
        if (info != null) {
            info.forEach((k, v) -> merged.put(String.valueOf(k), String.valueOf(v)));
        }
        return merged;
    }

    private static void validateProperties(Map<String, String> props) throws SQLException {
        for (String key : props.keySet()) {
            if (KNOWN_PROPERTIES.contains(key)) {
                continue;
            }
            for (String prefix : SECURITY_PREFIXES) {
                if (key.startsWith(prefix)) {
                    throw new SQLException("Unknown security property: " + key);
                }
            }
        }
        if (props.containsKey("ssl")) {
            String sslVal = props.get("ssl");
            if (!"true".equalsIgnoreCase(sslVal) && !"false".equalsIgnoreCase(sslVal)) {
                throw new SQLException("Invalid value for 'ssl': " + sslVal
                        + " (expected 'true' or 'false')");
            }
        }
    }

    private static void applyProperties(PangrovaRestClient.Builder builder,
                                         String host, int port,
                                         Map<String, String> props) {
        applyAuthAndTlsProperties(builder, props);
        applyTimeoutProperties(builder, props);
        applyRetryPolicy(builder, props);
        applyFailoverConfig(builder, host, port, props);
    }

    private static void applyAuthAndTlsProperties(PangrovaRestClient.Builder builder,
                                                   Map<String, String> props) {
        if (props.containsKey("token")) {
            builder.bearerToken(props.get("token"));
        }
        if ("true".equalsIgnoreCase(props.get("ssl"))) {
            builder.useTls(true);
        }
        if (props.containsKey("sslTrustStore")) {
            builder.trustStorePath(props.get("sslTrustStore"));
        }
        if (props.containsKey("sslTrustStorePassword")) {
            builder.trustStorePassword(props.get("sslTrustStorePassword"));
        }
    }

    private static void applyTimeoutProperties(PangrovaRestClient.Builder builder,
                                               Map<String, String> props) {
        if (props.containsKey("connectTimeoutMs")) {
            builder.connectTimeout(Duration.ofMillis(
                    Long.parseLong(props.get("connectTimeoutMs"))));
        }
        if (props.containsKey("requestTimeoutMs")) {
            builder.requestTimeout(Duration.ofMillis(
                    Long.parseLong(props.get("requestTimeoutMs"))));
        }
    }

    private static void applyRetryPolicy(PangrovaRestClient.Builder builder,
                                         Map<String, String> props) {
        if (props.containsKey("retries")) {
            int maxRetries = Integer.parseInt(props.get("retries"));
            builder.retryPolicy(new RetryPolicy(maxRetries,
                    RetryPolicy.DEFAULT.initialDelay(),
                    RetryPolicy.DEFAULT.maxDelay(),
                    RetryPolicy.DEFAULT.backoffMultiplier()));
        }
    }

    private static void applyFailoverConfig(PangrovaRestClient.Builder builder,
                                            String host, int port,
                                            Map<String, String> props) {
        if (props.containsKey("failover")) {
            String[] backups = props.get("failover").split(",");
            List<String> endpoints = new ArrayList<>();
            endpoints.add(host + ":" + port);
            for (String backup : backups) {
                endpoints.add(backup.trim());
            }
            builder.connectionPoolConfig(ConnectionPoolConfig.withFailover(endpoints));
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return JdbcUrlParser.accepts(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        Map<String, String> current = new LinkedHashMap<>();
        if (url != null && acceptsURL(url)) {
            try {
                JdbcConnectionParams params = JdbcUrlParser.parse(url);
                current.putAll(params.properties());
            } catch (Exception ignored) {
            }
        }
        if (info != null) {
            info.forEach((k, v) -> current.put(String.valueOf(k), String.valueOf(v)));
        }
        return new DriverPropertyInfo[]{
                propInfo("token", current.get("token"), "Bearer token for authentication"),
                propInfo("ssl", current.get("ssl"), "Enable TLS (true/false)"),
                propInfo("sslTrustStore", current.get("sslTrustStore"), "Path to trust store"),
                propInfo("sslTrustStorePassword", current.get("sslTrustStorePassword"), "Trust store password"),
                propInfo("failover", current.get("failover"), "Comma-separated backup host:port endpoints"),
                propInfo("retries", current.get("retries"), "Max retry attempts"),
                propInfo("connectTimeoutMs", current.get("connectTimeoutMs"), "Connect timeout in milliseconds"),
                propInfo("requestTimeoutMs", current.get("requestTimeoutMs"), "Request timeout in milliseconds"),
                propInfo("poolSize", current.get("poolSize"), "HTTP client thread pool size (default: 4)"),
        };
    }

    private static DriverPropertyInfo propInfo(String name, String value, String description) {
        DriverPropertyInfo pi = new DriverPropertyInfo(name, value);
        pi.required = false;
        pi.description = description;
        return pi;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging not supported");
    }

    /**
     * Returns the shared HttpClient, creating it lazily if needed.
     * Thread-safe via double-checked locking on {@link #CLIENT_LOCK}.
     */
    private static HttpClient getOrCreateSharedHttpClient(int poolSize) {
        HttpClient client = sharedHttpClient;
        if (client != null) {
            return client;
        }
        synchronized (CLIENT_LOCK) {
            if (sharedHttpClient == null) {
                int effectivePoolSize = poolSize > 0 ? poolSize : DEFAULT_HTTP_CLIENT_POOL_SIZE;
                AtomicInteger counter = new AtomicInteger();
                sharedExecutor = Executors.newFixedThreadPool(effectivePoolSize, r -> {
                    Thread t = new Thread(r, "pangrova-jdbc-http-" + counter.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                });
                sharedHttpClient = HttpClient.newBuilder()
                        .executor(sharedExecutor)
                        .build();
                LOG.info("event=shared_http_client_created poolSize={}", effectivePoolSize);
            }
            return sharedHttpClient;
        }
    }

    /**
     * Shuts down the shared HttpClient executor.
     * Called during driver deregistration or JVM shutdown.
     */
    static void shutdownSharedClient() {
        synchronized (CLIENT_LOCK) {
            ExecutorService exec = sharedExecutor;
            if (exec != null) {
                shutdownExecutor(exec);
                sharedHttpClient = null;
                sharedExecutor = null;
                LOG.info("event=shared_http_client_shutdown");
            }
        }
    }

    private static void shutdownExecutor(ExecutorService exec) {
        exec.shutdown();
        try {
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
            }
        } catch (InterruptedException e) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static int parsePoolSize(Map<String, String> props) {
        String value = props.get("poolSize");
        if (value == null || value.isEmpty()) {
            return DEFAULT_HTTP_CLIENT_POOL_SIZE;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid poolSize '{}', using default {}", value, DEFAULT_HTTP_CLIENT_POOL_SIZE);
            return DEFAULT_HTTP_CLIENT_POOL_SIZE;
        }
    }
}
