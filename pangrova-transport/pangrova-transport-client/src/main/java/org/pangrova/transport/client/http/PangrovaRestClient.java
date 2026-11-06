package org.pangrova.transport.client.http;

import org.pangrova.client.api.MetadataTableIdentifier;
import org.pangrova.client.api.MetadataTableInfo;
import org.pangrova.transport.client.authentication.ClientAuthentication;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.client.protocol.QueryStatusResponse;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.client.protocol.SubmitQueryRequest;
import org.pangrova.client.protocol.SubmitQueryResponse;
import org.pangrova.client.api.ResultBatchConsumer;
import org.pangrova.client.error.PangrovaConnectionException;
import org.pangrova.client.error.PangrovaProtocolException;
import org.pangrova.client.session.ConnectionPoolConfig;
import org.pangrova.client.session.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

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
    private final RestHttpClientOwner httpOwner;
    private final ObjectMapper objectMapper;
    private final ResultStreamFetcher resultFetcher;
    private final EndpointRouting routing;
    private final RestResponseHandler responseHandler;
    private final RetryableHttpExecutor executor;

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
        this.routing = new EndpointRouting(resolveEndpoints(), config.scheme());

        this.httpOwner = RestHttpClientOwner.create(config, sharedClient);
        try {
            this.resultFetcher = new ResultStreamFetcher(config.resultTransportSecurity());
        } catch (RuntimeException | Error failure) {
            try {
                httpOwner.close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        this.executor = new RetryableHttpExecutor(httpOwner, retryPolicy, routing, config.requestTimeout());
    }

    private List<String> resolveEndpoints() {
        if (connectionPoolConfig != null && connectionPoolConfig.failoverEnabled()) {
            return connectionPoolConfig.endpoints();
        }
        return List.of(config.coordinatorHost() + ":" + config.coordinatorPort());
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
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("Query cancellation requires its query identity");
        }
        var selection = routing.current();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + "/v1/query/" + queryId + "/cancel"))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.noBody())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(routing.bind(selection, builder.build()));
        return parseCancelOutcome(response, queryId);
    }

    private CancelOutcome parseCancelOutcome(HttpResponse<String> response, String queryId) {
        int status = response.statusCode();
        if (status != HttpStatusCodes.NOT_FOUND) responseHandler.check(response);
        CancelOutcome expected = switch (status) {
            case HttpStatusCodes.OK -> CancelOutcome.ALREADY_TERMINAL;
            case HttpStatusCodes.ACCEPTED -> CancelOutcome.CANCEL_ACCEPTED;
            case HttpStatusCodes.NOT_FOUND -> CancelOutcome.NOT_FOUND;
            default -> throw new PangrovaProtocolException("CANCEL_RESPONSE_INVALID",
                    "Unexpected query cancellation response status", status);
        };
        JsonNode node;
        try {
            node = objectMapper.readerFor(JsonNode.class)
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                    .readValue(response.body());
        } catch (com.fasterxml.jackson.core.JsonProcessingException invalidBody) {
            throw new PangrovaProtocolException("CANCEL_RESPONSE_INVALID",
                    "Query cancellation response is not valid JSON", status, invalidBody);
        }
        if (node == null || !node.isObject() || !java.util.Objects.equals(queryId, node.path("query_id").textValue())
                || !expected.name().equals(node.path("outcome").textValue())
                || !node.path("cancelled").isBoolean()
                || node.path("cancelled").booleanValue() != (expected == CancelOutcome.CANCEL_ACCEPTED)) {
            throw new PangrovaProtocolException("CANCEL_RESPONSE_INVALID",
                    "Query cancellation response does not match its requested identity and status", status);
        }
        return expected;
    }

    /** Logical cancellation outcome only; none of these values establishes physical execution or socket exit. */
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

    /** Lists actual schemas, including schemas with no tables, under one catalog. */
    public List<String> listSchemas(String catalog) {
        var node = get("/v1/namespaces?parent=" + metadataPathComponent(catalog), JsonNode.class);
        return CatalogMetadataParser.parseSchemas(node, catalog);
    }

    /**
     * Lists tables in a given namespace (catalog).
     */
    public List<MetadataTableIdentifier> listTables(String namespace) {
        var node = get("/v1/namespaces/" + metadataPathComponent(namespace) + "/tables", JsonNode.class);
        return CatalogMetadataParser.parseTables(node);
    }

    /**
     * Loads table metadata including column information.
     */
    public MetadataTableInfo loadTableMetadata(List<String> namespace, String table) {
        if (namespace == null || namespace.isEmpty() || namespace.stream().anyMatch(
                part -> part == null || part.isEmpty() || part.contains("\u001f"))) {
            throw new IllegalArgumentException("Namespace requires nonempty components without the reserved separator");
        }
        String encodedNamespace = metadataPathComponent(String.join("\u001f", namespace));
        var node = get("/v1/namespaces/" + encodedNamespace + "/tables/" + metadataPathComponent(table),
                JsonNode.class);
        return new MetadataTableInfo(namespace, table, CatalogMetadataParser.parseColumns(node));
    }

    private static String metadataPathComponent(String component) {
        return java.net.URLEncoder.encode(java.util.Objects.requireNonNull(component, "component"),
                java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Pings the coordinator's health endpoint to verify connectivity.
     * Used by JDBC Connection.isValid() to implement a real server health check.
     *
     * @param timeoutSeconds maximum time to wait for a response
     * @throws PangrovaConnectionException if the server is unreachable
     */
    public void ping(int timeoutSeconds) {
        var selection = routing.current();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + "/v1/health"))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .GET();
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.execute(routing.bind(selection, builder.build()));
        if (response.statusCode() != HttpStatusCodes.OK) {
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

    /** Fetches results while honoring the caller's absolute query deadline. */
    public byte[] fetchResult(ResultEndpoint endpoint, long deadlineEpochMs) {
        return resultFetcher.fetch(endpoint, deadlineEpochMs);
    }

    /**
     * Streams query result via transport, invoking the consumer for each batch.
     */
    public void fetchResultStream(ResultEndpoint endpoint, ResultBatchConsumer consumer) {
        resultFetcher.fetchStream(endpoint, consumer);
    }

    @Override
    public void close() {
        httpOwner.seal();
        resultFetcher.close();
        httpOwner.close();
        LOG.debug("event=rest_client_closed");
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
        var selection = routing.current();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + path))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(routing.bind(selection, builder.build()));
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "POST " + path);
    }

    private <T> T postNonIdempotent(String path, Object body, Class<T> responseType) {
        byte[] bodyBytes = serializeBody(body, path);
        String requestId = java.util.UUID.randomUUID().toString();
        var selection = routing.current();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + path))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Idempotency-Key", requestId)
                .header("X-Pangrova-Request-Id", requestId);
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.execute(routing.bind(selection, builder.build()));
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
        var selection = routing.current();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + path))
                .timeout(config.requestTimeout())
                .GET()
                .header("Accept", "application/json");
        injectAuthHeader(builder);
        HttpResponse<String> response = executor.executeWithRetry(routing.bind(selection, builder.build()));
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "GET " + path);
    }

    private void injectAuthHeader(HttpRequest.Builder builder) {
        if (config.authentication() != null) {
            builder.header("Authorization", config.authentication().authorizationHeader());
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
        private ResultTransportSecurity resultTransportSecurity = ResultTransportSecurity.systemTrust(false);
        private ClientAuthentication authentication;
        private HttpClient sharedHttpClient;
        private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;
        private ConnectionPoolConfig connectionPoolConfig;
        private Integer httpExecutorThreads;

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

        /**
         * Sets the result-channel PEM policy independently of REST HTTPS trust stores.
         *
         * @param security immutable result policy and optional PEM material
         * @return this builder
         */
        public Builder resultTransportSecurity(ResultTransportSecurity security) {
            this.resultTransportSecurity = java.util.Objects.requireNonNull(security, "security");
            return this;
        }
        public Builder bearerToken(String token) {
            return authentication(ClientAuthentication.bearer(token));
        }

        /**
         * Sets the sole REST authentication policy without replacing previously configured credentials.
         *
         * @param value immutable authentication policy
         * @return this builder
         */
        public Builder authentication(ClientAuthentication value) {
            java.util.Objects.requireNonNull(value, "authentication");
            if (authentication != null) {
                throw new IllegalArgumentException("Authentication has already been configured");
            }
            this.authentication = value;
            return this;
        }

        /**
         * Injects a shared HttpClient instance instead of creating a new one.
         * When set, the client is NOT owned by this PangrovaRestClient — the caller
         * is responsible for its lifecycle. An explicit HTTPS trust store cannot
         * be combined with a shared client; build rejects ambiguous trust ownership.
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

        /** Sets the owned HTTP executor thread count without multiplying it by failover endpoints. */
        public Builder httpExecutorThreads(int threads) {
            this.httpExecutorThreads = threads;
            return this;
        }

        /**
         * Builds the client from the configured values.
         */
        public PangrovaRestClient build() {
            PangrovaRestClientConfig config = new PangrovaRestClientConfig(
                    coordinatorHost, coordinatorPort, connectTimeout, requestTimeout,
                    useTls, trustStorePath, trustStorePassword, authentication,
                    retryPolicy, connectionPoolConfig, httpExecutorThreads, resultTransportSecurity);
            return new PangrovaRestClient(config, sharedHttpClient);
        }
    }
}
