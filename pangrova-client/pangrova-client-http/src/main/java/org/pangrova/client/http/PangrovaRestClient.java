package org.pangrova.client.http;

import org.pangrova.common.net.UriComponents;
import org.pangrova.common.concurrency.ThreadSleeper;
import org.pangrova.common.client.metadata.MetadataTableIdentifier;
import org.pangrova.common.client.metadata.MetadataTableInfo;
import org.pangrova.client.http.authentication.ClientAuthentication;
import org.pangrova.transport.client.FlightResultFetcher;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.exception.PangrovaConnectionException;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.exception.PangrovaProtocolException;
import org.pangrova.common.client.session.ConnectionPoolConfig;
import org.pangrova.common.client.session.RetryPolicy;
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
public final class PangrovaRestClient implements QueryClient, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaRestClient.class);
    private static final long RECOVERY_MAX_WAIT_MS = 60_000;
    private static final long RECOVERY_INTERVAL_MS = 1_000;
    private static final String OWNER_UNAVAILABLE = "QUERY_OWNER_UNAVAILABLE";
    private static final String RECOVERY_PENDING = "QUERY_RECOVERY_PENDING";

    private final PangrovaRestClientConfig config;
    private final java.util.concurrent.ConcurrentMap<String,
            String> branchRoutes = new java.util.concurrent.ConcurrentHashMap<>();
    private final RetryPolicy retryPolicy;
    private final ConnectionPoolConfig connectionPoolConfig;
    private final RestHttpClientOwner httpOwner;
    private final ObjectMapper objectMapper;
    private final FlightResultFetcher resultFetcher;
    private final EndpointSelector routing;
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
        this.routing = new EndpointSelector(resolveEndpoints(), config.scheme());

        this.httpOwner = RestHttpClientOwner.create(config, sharedClient);
        try {
            this.resultFetcher = new FlightResultFetcher(config.resultTransportSecurity());
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
        return submitQuery(sql, sessionProperties, routing.current());
    }

    @Override
    public QueryExecution beginQuery(String sql, Map<String, String> sessionProperties) {
        var selection = routing.current();
        var receipt = submitQuery(sql, sessionProperties, selection);
        return new OwnerQuery(receipt, routing.owner(selection, receipt.ownerEndpoint()));
    }

    /** Reconnects to an existing query from its submit receipt without issuing SQL again. */
    public QueryExecution resumeQuery(SubmitQueryResponse receipt) {
        java.util.Objects.requireNonNull(receipt, "receipt");
        return new OwnerQuery(receipt, routing.owner(routing.current(), receipt.ownerEndpoint()));
    }

    private SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties,
            EndpointSelector.Selection selection) {
        var properties = new java.util.HashMap<>(branchRoutes);
        if (sessionProperties != null) properties.putAll(sessionProperties);
        var request = new SubmitQueryRequest(sql, properties);
        return postNonIdempotent("/v1/query/submit", request, SubmitQueryResponse.class, selection);
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
        return cancelQuery(queryId, routing.current(), false, null);
    }

    private CancelOutcome cancelQuery(String queryId, EndpointSelector.Selection selection, boolean ownerBound,
            String routeTicket) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("Query cancellation requires its query identity");
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + "/v1/query/" + queryId + "/cancel"))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.noBody())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        addRequestHeaders(builder);
        addRouteTicket(builder, routeTicket);
        var request = routing.bind(selection, builder.build());
        HttpResponse<String> response = ownerBound ? executor.execute(request) : executor.executeWithRetry(request);
        return parseCancelOutcome(response, queryId);
    }

    private static void addRouteTicket(HttpRequest.Builder builder, String routeTicket) {
        if (routeTicket != null) {
            builder.header(org.pangrova.common.client.query.QueryOwnerEndpoint.ROUTE_HEADER, routeTicket);
        }
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



    /**
     * Lists all catalogs (top-level namespaces) from the coordinator.
     */
    public List<String> listCatalogs() {
        var node = get("/v1/namespaces", JsonNode.class);
        return CatalogMetadataParser.parseCatalogs(node);
    }

    /** Lists actual schemas, including schemas with no tables, under one catalog. */
    public List<String> listSchemas(String catalog) {
        var node = get("/v1/namespaces?parent=" + UriComponents.encode(catalog), JsonNode.class);
        return CatalogMetadataParser.parseSchemas(node, catalog);
    }

    /**
     * Lists tables in a given namespace (catalog).
     */
    public List<MetadataTableIdentifier> listTables(String namespace) {
        var node = get("/v1/namespaces/" + UriComponents.encode(namespace) + "/tables", JsonNode.class);
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
        String encodedNamespace = UriComponents.encode(String.join("\u001f", namespace));
        var node = get("/v1/namespaces/" + encodedNamespace + "/tables/" + UriComponents.encode(table),
                JsonNode.class);
        return new MetadataTableInfo(namespace, table, CatalogMetadataParser.parseColumns(node));
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
        addRequestHeaders(builder);
        HttpResponse<String> response = executor.execute(routing.bind(selection, builder.build()));
        if (response.statusCode() != HttpStatusCodes.OK) {
            throw new PangrovaConnectionException(
                    "HEALTH_CHECK_FAILED",
                    "Server returned " + response.statusCode(),
                    null);
        }
    }

    @Override
    public ResultCursor openResult(ResultEndpoint endpoint, long deadlineEpochMs) {
        return resultFetcher.open(endpoint, deadlineEpochMs);
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

    private record QueryControlRoute(EndpointSelector.Selection selection, String ticket) {}

    private final class OwnerQuery implements QueryExecution {
        private final SubmitQueryResponse receipt;
        private final String observer = java.util.UUID.randomUUID().toString();
        private volatile QueryControlRoute route;
        private volatile QueryStatusResponse recovered;
        private volatile boolean branchApplied;
        private Long version;

        OwnerQuery(SubmitQueryResponse receipt, EndpointSelector.Selection selection) {
            this.receipt = receipt;
            this.route = routeFor(selection);
        }

        @Override
        public SubmitQueryResponse submission() {
            return receipt;
        }

        @Override
        public QueryStatusResponse awaitStatus(QueryWaitTarget target, long deadlineEpochMillis) {
            java.util.Objects.requireNonNull(target, "target");
            var terminal = recovered;
            if (terminal != null) return terminal;
            var current = route;
            try {
                return readLive(current, target, deadlineEpochMillis, true);
            } catch (PangrovaClientException failure) {
                if (signedReceipt() == null) throw failure;
                if (OWNER_UNAVAILABLE.equals(failure.errorCode())) {
                    return awaitRetirement(current.selection(), deadlineEpochMillis);
                }
                if (!canFailOver(failure)) throw failure;
                for (var alternate : routing.alternatives(current.selection())) {
                    try {
                        var candidate = routeFor(alternate);
                        var status = readLive(candidate, target, deadlineEpochMillis, false);
                        route = candidate;
                        return status;
                    } catch (PangrovaClientException alternateFailure) {
                        if (OWNER_UNAVAILABLE.equals(alternateFailure.errorCode())) {
                            return awaitRetirement(alternate, deadlineEpochMillis);
                        }
                        if (!canFailOver(alternateFailure)) throw alternateFailure;
                    }
                }
                throw failure;
            }
        }

        private QueryStatusResponse readLive(QueryControlRoute selected, QueryWaitTarget target,
                long deadlineEpochMillis, boolean wait) {
            long now = System.currentTimeMillis();
            if (deadlineEpochMillis <= now) throw statusDeadline(null);
            long remaining = deadlineEpochMillis == Long.MAX_VALUE ? Long.MAX_VALUE : deadlineEpochMillis - now;
            long configuredMillis = config.requestTimeout().toMillis();
            long requestMillis = Math.min(configuredMillis, remaining);
            boolean followVersion = target == QueryWaitTarget.NEXT_VERSION;
            long waitMillis = !wait || (followVersion && version == null) ? 0
                    : Math.min(QueryStatusResponse.MAX_WAIT_MILLIS, requestMillis / 2);
            String path = "/v1/query/" + receipt.queryId() + "/state?afterVersion="
                    + (version == null ? 0 : version) + "&waitMs=" + waitMillis;
            if (!followVersion) path += "&until=" + target.name();
            var builder = HttpRequest.newBuilder(URI.create(selected.selection().baseUrl() + path))
                    .timeout(Duration.ofMillis(Math.max(1, requestMillis))).GET()
                    .header("Accept", "application/json")
                    .header("X-Pangrova-Query-Observer", observer);
            addRequestHeaders(builder);
            addRouteTicket(builder, selected.ticket());
            var response = executeObservation(selected.selection(), builder, remaining);
            responseHandler.check(response);
            var status = responseHandler.deserialize(response.body(), QueryStatusResponse.class, "GET " + path);
            if (!hasCoherentLiveStatus(status)) {
                throw new PangrovaProtocolException("QUERY_STATUS_INVALID",
                        "Owner status identity, version, drain evidence or disposition mismatched",
                        response.statusCode());
            }
            version = status.version();
            applyBranchSelection(status);
            return status;
        }

        private QueryStatusResponse awaitRetirement(EndpointSelector.Selection selected, long deadlineEpochMillis) {
            long recoveryDeadline = Math.min(deadlineEpochMillis, System.currentTimeMillis() + RECOVERY_MAX_WAIT_MS);
            while (true) {
                try {
                    return readRecovered(selected, recoveryDeadline);
                } catch (PangrovaClientException failure) {
                    if (!RECOVERY_PENDING.equals(failure.errorCode())) throw failure;
                    long remaining = recoveryDeadline - System.currentTimeMillis();
                    if (remaining <= RECOVERY_INTERVAL_MS) {
                        if (recoveryDeadline == deadlineEpochMillis) throw statusDeadline(failure);
                        throw failure;
                    }
                    ThreadSleeper.sleepQuietly(RECOVERY_INTERVAL_MS);
                    if (Thread.currentThread().isInterrupted()) {
                        throw new PangrovaConnectionException("CONNECTION_INTERRUPTED",
                                "Interrupted waiting for query retirement queryId=" + receipt.queryId(), failure);
                    }
                }
            }
        }

        private QueryStatusResponse readRecovered(EndpointSelector.Selection selected, long deadlineEpochMillis) {
            long remaining = deadlineEpochMillis - System.currentTimeMillis();
            if (remaining <= 0) throw statusDeadline(null);
            String path = "/v1/query/" + receipt.queryId() + "/receipt-recovery";
            var builder = HttpRequest.newBuilder(URI.create(selected.baseUrl() + path))
                    .timeout(Duration.ofMillis(Math.max(1, Math.min(config.requestTimeout().toMillis(), remaining))))
                    .GET().header("Accept", "application/json");
            addRequestHeaders(builder);
            addRouteTicket(builder, signedReceipt());
            var response = executeObservation(selected, builder, remaining);
            responseHandler.check(response);
            var status = responseHandler.deserialize(response.body(), QueryStatusResponse.class, "GET " + path);
            if (!receipt.queryId().equals(status.queryId()) || !status.recovered()) {
                throw new PangrovaProtocolException("QUERY_RECOVERY_INVALID",
                        "Receipt recovery requires a matching, physically drained terminal outcome",
                        response.statusCode());
            }
            applyBranchSelection(status);
            recovered = status;
            return status;
        }

        private void applyBranchSelection(QueryStatusResponse status) {
            var branch = status.branchSelection();
            if (branch == null || branchApplied) return;
            synchronized (this) {
                if (branchApplied) return;
                if (branch.branch() == null) branchRoutes.remove(branch.propertyKey());
                else branchRoutes.put(branch.propertyKey(), branch.branch());
                branchApplied = true;
            }
        }

        private HttpResponse<String> executeObservation(EndpointSelector.Selection selected,
                HttpRequest.Builder builder, long remaining) {
            try {
                return executor.execute(routing.bind(selected, builder.build()));
            } catch (PangrovaConnectionException failure) {
                if ("REQUEST_TIMEOUT".equals(failure.errorCode()) && remaining <= config.requestTimeout().toMillis()) {
                    throw statusDeadline(failure);
                }
                throw failure;
            }
        }

        private QueryControlRoute routeFor(EndpointSelector.Selection selected) {
            return new QueryControlRoute(selected, routing.routeTicket(selected, receipt.ownerEndpoint()));
        }

        private String signedReceipt() {
            return receipt.ownerEndpoint() == null ? null : receipt.ownerEndpoint().routeTicket();
        }

        private boolean hasCoherentLiveStatus(QueryStatusResponse status) {
            return receipt.queryId().equals(status.queryId())
                    && status.version() != null
                    && status.version() >= 0
                    && status.drained() != null
                    && (version == null || status.version() >= version);
        }

        private PangrovaConnectionException statusDeadline(Throwable cause) {
            return new PangrovaConnectionException("QUERY_STATUS_DEADLINE_EXCEEDED",
                    "Query deadline exceeded waiting for state queryId=" + receipt.queryId(), cause);
        }

        @Override
        public CancelOutcome cancel() {
            if (recovered != null) return CancelOutcome.ALREADY_TERMINAL;
            var current = route;
            CancelOutcome outcome;
            try {
                outcome = cancelQuery(receipt.queryId(), current.selection(), true, current.ticket());
            } catch (PangrovaClientException failure) {
                if (signedReceipt() == null) throw failure;
                if (OWNER_UNAVAILABLE.equals(failure.errorCode())) {
                    return observeCancellation(current.selection(), failure);
                }
                if (!canFailOver(failure)) throw failure;
                for (var alternate : routing.alternatives(current.selection())) {
                    try {
                        return observeCancellation(alternate, failure);
                    } catch (PangrovaClientException alternateFailure) {
                        if (!canFailOver(alternateFailure)) throw alternateFailure;
                    }
                }
                throw failure;
            }
            return outcome == CancelOutcome.NOT_FOUND && signedReceipt() != null
                    ? observeCancellation(current.selection(), null) : outcome;
        }

        private CancelOutcome observeCancellation(EndpointSelector.Selection selected,
                PangrovaClientException original) {
            try {
                readRecovered(selected, Long.MAX_VALUE);
                return CancelOutcome.ALREADY_TERMINAL;
            } catch (PangrovaClientException failure) {
                if (!"NOT_FOUND".equals(failure.errorCode())) throw failure;
                if (original != null) throw original;
                return CancelOutcome.NOT_FOUND;
            }
        }
    }

    private static boolean canFailOver(PangrovaClientException failure) {
        return failure instanceof PangrovaConnectionException && switch (failure.errorCode()) {
            case "CONNECTION_TIMEOUT", "REQUEST_TIMEOUT", "CONNECTION_REFUSED",
                    "DNS_RESOLUTION_FAILED", "CONNECTION_FAILED" -> true;
            default -> false;
        };
    }

    // ---- internal HTTP helpers ----

    private <T> T postNonIdempotent(String path, Object body, Class<T> responseType,
                                   EndpointSelector.Selection selection) {
        byte[] bodyBytes = serializeBody(body, path);
        String requestId = java.util.UUID.randomUUID().toString();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(selection.baseUrl() + path))
                .timeout(config.requestTimeout())
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Idempotency-Key", requestId)
                .header("X-Pangrova-Request-Id", requestId);
        addRequestHeaders(builder);
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
        addRequestHeaders(builder);
        HttpResponse<String> response = executor.executeWithRetry(routing.bind(selection, builder.build()));
        responseHandler.check(response);
        return responseHandler.deserialize(response.body(), responseType, "GET " + path);
    }

    private void addRequestHeaders(HttpRequest.Builder builder) {
        ApiVersionHeaders.addVersionHeader(builder);
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
