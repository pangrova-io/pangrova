package org.pangrova.client.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.ResultPage;
import org.pangrova.protocol.rest.query.SubmitQueryRequest;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pangrova REST API client SDK.
 * Thread-safe, immutable config, AutoCloseable for resource cleanup.
 *
 * <p>Supports configurable retry with exponential backoff via {@link RetryPolicy}
 * and multi-endpoint failover via {@link ConnectionPoolConfig}.
 *
 * <p>Usage:
 * <pre>{@code
 * try (PangrovaRestClient client = PangrovaRestClient.builder()
 *         .coordinatorHost("localhost")
 *         .coordinatorPort(8080)
 *         .retryPolicy(RetryPolicy.DEFAULT)
 *         .connectionPoolConfig(ConnectionPoolConfig.singleEndpoint("localhost", 8080))
 *         .build()) {
 *     SubmitQueryResponse resp = client.submitQuery("SELECT 1", Map.of());
 * }
 * }</pre>
 */
public final class PangrovaRestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaRestClient.class);

    private final PangrovaRestClientConfig config;
    private final RetryPolicy retryPolicy;
    private final ConnectionPoolConfig connectionPoolConfig;
    private final HttpClient httpClient;
    private final boolean ownsHttpClient;
    private final ObjectMapper objectMapper;
    private final ResultStreamFetcher resultFetcher;
    private final List<String> endpoints;
    private final AtomicInteger endpointIndex;
    private final RestResponseHandler responseHandler;
    private final RetryableHttpExecutor executor;

    private PangrovaRestClient(PangrovaRestClientConfig config) {
        this(config, null);
    }

    /**
     * @param config         client configuration
     * @param sharedClient   optional shared HttpClient; if null, a new one is created (and owned)
     */
    private PangrovaRestClient(PangrovaRestClientConfig config, HttpClient sharedClient) {
        this.config = config;
        this.retryPolicy = config.retryPolicy();
        this.connectionPoolConfig = config.connectionPoolConfig();
        this.objectMapper = JsonMapperFactory.create();
        this.responseHandler = new RestResponseHandler(objectMapper);

        if (sharedClient != null) {
            this.httpClient = sharedClient;
            this.ownsHttpClient = false;
            LOG.debug("event=rest_client_created mode=shared_http_client host={} port={}",
                    config.coordinatorHost(), config.coordinatorPort());
        } else {
            HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                    // A-CLIENT-02: never transparently follow 3xx redirects — a redirect Location could
                    // point at an internal/private address (SSRF surface). Any 3xx is surfaced via
                    // checkResponse instead of being blindly followed; leader re-routing is handled
                    // explicitly, not via Location. (Java's default is NEVER; made explicit here so a
                    // future refactor cannot silently enable transparent redirects.)
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .connectTimeout(config.connectTimeout());
            if (config.useTls() && config.trustStorePath() != null) {
                clientBuilder.sslContext(TlsContextFactory.build(config));
            }
            if (connectionPoolConfig != null) {
                int poolSize = connectionPoolConfig.maxConnectionsPerEndpoint()
                        * connectionPoolConfig.endpoints().size();
                clientBuilder.executor(java.util.concurrent.Executors.newFixedThreadPool(
                        poolSize, r -> {
                            Thread t = new Thread(r, "pangrova-rest-pool");
                            t.setDaemon(true);
                            return t;
                        }));
                LOG.debug("event=rest_client_pool_configured poolSize={} idleTimeout={}",
                        poolSize, connectionPoolConfig.connectionIdleTimeout());
            }
            this.httpClient = clientBuilder.build();
            this.ownsHttpClient = true;
        }
        // F-REST-05: Guard against partial construction failure leaking resources.
        try {
            this.resultFetcher = new ResultStreamFetcher(config.useTls(), config.trustStorePath());
        } catch (RuntimeException ex) {
            if (this.ownsHttpClient) {
                // HttpClient doesn't implement AutoCloseable in JDK 11-17;
                // executor is daemon-threaded and will be GC'd.
                LOG.warn("event=rest_client_constructor_failure closing_resources", ex);
            }
            throw ex;
        }
        this.endpoints = resolveEndpoints();
        this.endpointIndex = new AtomicInteger(0);
        this.executor = new RetryableHttpExecutor(httpClient, retryPolicy, connectionPoolConfig,
                endpoints, endpointIndex, config.scheme(), config.requestTimeout());
    }

    private List<String> resolveEndpoints() {
        if (connectionPoolConfig != null && connectionPoolConfig.failoverEnabled()) {
            return connectionPoolConfig.endpoints();
        }
        return List.of(config.coordinatorHost() + ":" + config.coordinatorPort());
    }

    private String currentBaseUrl() {
        String endpoint = endpoints.get(endpointIndex.get() % endpoints.size());
        return config.scheme() + "://" + endpoint;
    }

    /**
     * Submits a query to the Coordinator.
     * Non-idempotent POST: does not retry after the request may have been sent.
     */
    public SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties) {
        SubmitQueryRequest request = new SubmitQueryRequest(sql, sessionProperties);
        return postNonIdempotent("/v1/query/submit", request, SubmitQueryResponse.class);
    }

    /**
     * Queries the status of a submitted query.
     */
    public QueryStatusResponse getQueryStatus(String queryId) {
        return get("/v1/query/" + queryId + "/state", QueryStatusResponse.class);
    }

    /**
     * Cancels a running query.
     */
    public CancelOutcome cancelQuery(String queryId) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(currentBaseUrl() + "/v1/query/" + queryId + "/cancel"))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.noBody())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(builder.build());
        return parseCancelOutcome(response);
    }

    private CancelOutcome parseCancelOutcome(HttpResponse<String> response) {
        int status = response.statusCode();
        if (status == HttpStatusCodes.NOT_FOUND) {
            return CancelOutcome.NOT_FOUND;
        }
        try {
            var node = objectMapper.readTree(response.body());
            String outcome = node.has("outcome") ? node.get("outcome").asText() : "";
            return switch (outcome) {
                case "CANCEL_ACCEPTED" -> CancelOutcome.CANCEL_ACCEPTED;
                case "ALREADY_TERMINAL" -> CancelOutcome.ALREADY_TERMINAL;
                default -> CancelOutcome.NOT_FOUND;
            };
        } catch (Exception e) {
            return (status == HttpStatusCodes.ACCEPTED)
                    ? CancelOutcome.CANCEL_ACCEPTED : CancelOutcome.ALREADY_TERMINAL;
        }
    }

    /** Outcome of a cancel request. */
    public enum CancelOutcome {
        CANCEL_ACCEPTED,
        ALREADY_TERMINAL,
        NOT_FOUND
    }

    /**
     * Lists all catalogs (top-level namespaces) from the coordinator.
     */
    public List<String> listCatalogs() {
        var node = get("/v1/namespaces", JsonNode.class);
        return CatalogMetadataParser.parseCatalogs(node);
    }

    /**
     * Lists tables in a given namespace (catalog).
     */
    public List<MetadataTableIdentifier> listTables(String namespace) {
        try {
            var node = get("/v1/namespaces/" + namespace + "/tables", JsonNode.class);
            return CatalogMetadataParser.parseTables(node);
        } catch (PangrovaClientException e) {
            LOG.warn("Failed to list tables: {}", e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * Loads table metadata including column information.
     */
    public MetadataTableInfo loadTableMetadata(String namespace, String table) {
        try {
            var node = get("/v1/namespaces/" + namespace + "/tables/" + table, JsonNode.class);
            return new MetadataTableInfo(namespace, table, CatalogMetadataParser.parseColumns(node));
        } catch (PangrovaClientException e) {
            return null;
        }
    }

    /** Table identifier returned by the catalog REST API. */
    public record MetadataTableIdentifier(List<String> namespace, String name) {}

    /** Column metadata from the catalog REST API. */
    public record MetadataColumnInfo(int id, String name, String type, boolean required) {}

    /** Table metadata including columns from the catalog REST API. */
    public record MetadataTableInfo(String namespace, String table, List<MetadataColumnInfo> columns) {}

    /**
     * Pings the coordinator's health endpoint to verify connectivity.
     * Used by JDBC Connection.isValid() to implement a real server health check.
     *
     * @param timeoutSeconds maximum time to wait for a response
     * @throws PangrovaConnectionException if the server is unreachable
     */
    public void ping(int timeoutSeconds) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(currentBaseUrl() + "/v1/health"))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .GET()
                .build();
        HttpResponse<String> response = executor.execute(request);
        if (response.statusCode() >= HttpStatusCodes.INTERNAL_SERVER_ERROR) {
            throw new PangrovaConnectionException(
                    "HEALTH_CHECK_FAILED",
                    "Server returned " + response.statusCode(),
                    null);
        }
    }

    /**
     * Fetches query result via transport, returning all batches as a single Arrow IPC byte array.
     */
    public byte[] fetchResult(ResultEndpoint endpoint) {
        return resultFetcher.fetch(endpoint);
    }

    /**
     * Streams query result via transport, invoking the consumer for each batch.
     */
    public void fetchResultStream(ResultEndpoint endpoint, ResultBatchConsumer consumer) {
        resultFetcher.fetchStream(endpoint, consumer);
    }

    /**
     * Fetches a single page of query results with cursor-based pagination.
     */
    public ResultPage fetchResultPage(String queryId, int maxRows, String nextToken) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive, got " + maxRows);
        }
        StringBuilder path = new StringBuilder("/v1/query/")
                .append(queryId)
                .append("/result?max_rows=")
                .append(maxRows);
        if (nextToken != null && !nextToken.isEmpty()) {
            path.append("&next_token=").append(
                    java.net.URLEncoder.encode(nextToken, java.nio.charset.StandardCharsets.UTF_8));
        }
        return get(path.toString(), ResultPage.class);
    }

    @Override
    public void close() {
        resultFetcher.close();
        if (ownsHttpClient) {
            shutdownHttpClientExecutor();
        }
        LOG.debug("event=rest_client_closed ownsHttpClient={}", ownsHttpClient);
    }

    private void shutdownHttpClientExecutor() {
        httpClient.executor()
                .filter(java.util.concurrent.ExecutorService.class::isInstance)
                .map(java.util.concurrent.ExecutorService.class::cast)
                .ifPresent(this::shutdownExecutorService);
    }

    private void shutdownExecutorService(java.util.concurrent.ExecutorService es) {
        es.shutdown();
        try {
            if (!es.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                es.shutdownNow();
            }
        } catch (InterruptedException e) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.debug("event=http_client_executor_shutdown");
    }

    public PangrovaRestClientConfig config() {
        return config;
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---- internal HTTP helpers ----

    private <T> T post(String path, Object body, Class<T> responseType) {
        byte[] bodyBytes = serializeBody(body, path);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(currentBaseUrl() + path))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(builder.build());
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "POST " + path);
    }

    private <T> T postNonIdempotent(String path, Object body, Class<T> responseType) {
        byte[] bodyBytes = serializeBody(body, path);
        String requestId = java.util.UUID.randomUUID().toString();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(currentBaseUrl() + path))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Idempotency-Key", requestId)
                .header("X-Pangrova-Request-Id", requestId);
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.execute(builder.build());
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "POST " + path);
    }

    private byte[] serializeBody(Object body, String path) {
        try {
            return objectMapper.writeValueAsBytes(body);
        } catch (Exception e) {
            throw new PangrovaProtocolException(
                    "SERIALIZATION_FAILED", "Failed to serialize request body for POST " + path, -1, e);
        }
    }

    private <T> T get(String path, Class<T> responseType) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(currentBaseUrl() + path))
                .timeout(config.requestTimeout())
                .GET()
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(builder.build());
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "GET " + path);
    }

    private void injectAuthHeader(HttpRequest.Builder builder) {
        if (config.bearerToken() != null && !config.bearerToken().isBlank()) {
            builder.header("Authorization", "Bearer " + config.bearerToken());
        }
    }

    // ---- Builder ----

    public static final class Builder {

        private String coordinatorHost = "localhost";
        private int coordinatorPort = 8080;
        private Duration connectTimeout = PangrovaRestClientConfig.DEFAULT_CONNECT_TIMEOUT;
        private Duration requestTimeout = PangrovaRestClientConfig.DEFAULT_REQUEST_TIMEOUT;
        private boolean useTls;
        private String trustStorePath;
        private String trustStorePassword;
        private String bearerToken;
        private HttpClient sharedHttpClient;
        private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;
        private ConnectionPoolConfig connectionPoolConfig;

        private Builder() {}

        public Builder coordinatorHost(String host) {
            this.coordinatorHost = host;
            return this;
        }
        public Builder coordinatorPort(int port) {
            this.coordinatorPort = port;
            return this;
        }
        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = timeout;
            return this;
        }
        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }
        public Builder useTls(boolean enabled) {
            this.useTls = enabled;
            return this;
        }
        public Builder trustStorePath(String path) {
            this.trustStorePath = path;
            return this;
        }
        public Builder trustStorePassword(String password) {
            this.trustStorePassword = password;
            return this;
        }
        public Builder bearerToken(String token) {
            this.bearerToken = token;
            return this;
        }

        /**
         * Injects a shared HttpClient instance instead of creating a new one.
         * When set, the client is NOT owned by this PangrovaRestClient — the caller
         * is responsible for its lifecycle.
         */
        public Builder httpClient(HttpClient client) {
            this.sharedHttpClient = client;
            return this;
        }

        /**
         * Sets the retry policy for transient failure handling.
         * Defaults to {@link RetryPolicy#DEFAULT} (3 retries, exponential backoff).
         */
        public Builder retryPolicy(RetryPolicy policy) {
            this.retryPolicy = policy;
            return this;
        }

        /**
         * Sets the connection pool and failover configuration.
         * When configured with multiple endpoints and failover enabled,
         * the client rotates to alternate endpoints on transient failures.
         */
        public Builder connectionPoolConfig(ConnectionPoolConfig poolConfig) {
            this.connectionPoolConfig = poolConfig;
            return this;
        }

        /**
         * Builds the client from the configured values.
         */
        public PangrovaRestClient build() {
            PangrovaRestClientConfig config = new PangrovaRestClientConfig(
                    coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                    useTls, trustStorePath, trustStorePassword, bearerToken,
                    retryPolicy, connectionPoolConfig);
            return new PangrovaRestClient(config, sharedHttpClient);
        }
    }
}
