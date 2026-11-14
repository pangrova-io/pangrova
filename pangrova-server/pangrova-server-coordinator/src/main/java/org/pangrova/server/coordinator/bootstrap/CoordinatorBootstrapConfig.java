package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.observability.sdk.config.ObservabilityConfig;

import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;

import java.nio.file.Path;
import java.util.Locale;
import java.util.List;
import java.util.Optional;

import org.pangrova.security.api.authentication.AuthenticatorFactory;
import org.pangrova.config.enums.ShuffleMode;
import org.pangrova.config.constants.CoordinatorOptions;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;

/**
 * Configuration for Coordinator bootstrap sequence.
 * Grouped into semantic sub-configs to keep constructor params manageable.
 */
public record CoordinatorBootstrapConfig(
        CoreConfig core,
        FeatureFlags featureFlags,
        QueryConfig query,
        TransportConfig transport,
        WorkerHealthConfig workerHealth,
        MetadataJdbcConfig metadataJdbcConfig,
        SecurityConfig securityConfig,
        ObservabilityConfig observabilityBootstrapConfig) {

    public CoordinatorBootstrapConfig {
        if (core == null) {
            throw new IllegalArgumentException("core must not be null");
        }
        if (featureFlags == null) {
            throw new IllegalArgumentException("featureFlags must not be null");
        }
        if (query == null) {
            throw new IllegalArgumentException("query must not be null");
        }
        if (transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        if (workerHealth == null) {
            throw new IllegalArgumentException("workerHealth must not be null");
        }
        // metadataJdbcConfig, securityConfig, observabilityBootstrapConfig are
        // nullable — they are only required when the corresponding feature flag
        // is enabled. Callers must check featureFlags before accessing them.
    }

    /** Creates a standalone coordinator configuration with production defaults. */
    public static CoordinatorBootstrapConfig withDefaults(String coordinatorId, int httpPort) {
        return new CoordinatorBootstrapConfig(
                new CoreConfig(coordinatorId, httpPort, CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
                FeatureFlags.allDisabled(),
                QueryConfig.defaults(),
                TransportConfig.defaults(),
                WorkerHealthConfig.defaults(),
                null, null, null);
    }

    // -- Convenience accessors delegating to sub-configs --

    public String coordinatorId() { return core.coordinatorId(); }
    public int httpPort() { return core.httpPort(); }
    public String httpHost() { return core.httpHost(); }
    public long shutdownTimeoutMs() { return core.shutdownTimeoutMs(); }
    public CoordinatorDeploymentRole role() { return core.role(); }

    public boolean observabilityEnabled() { return featureFlags.observability(); }
    public boolean securityEnabled() { return featureFlags.security(); }
    public boolean metadataEnabled() { return featureFlags.metadata(); }
    public boolean streamingEnabled() { return featureFlags.streaming(); }
    public boolean governanceEnabled() { return featureFlags.governance(); }
    public boolean resilienceEnabled() { return featureFlags.resilience(); }
    public boolean tenantEnabled() { return featureFlags.tenant(); }

    void requireAuthenticatedTenancy() {
        if (tenantEnabled()
                && (!securityEnabled() || securityConfig == null
                    || !securityConfig.authenticatesClients())) {
            throw new IllegalStateException(
                    "Tenant isolation requires an enabled, non-NOOP authenticator");
        }
    }
    public boolean operationsEnabled() { return featureFlags.operations(); }
    public boolean speculationEnabled() { return featureFlags.speculation(); }

    /** Optional threshold for speculation in a stage with one running task. */
    public long speculationSingleTaskDurationThresholdMs() {
        return Long.getLong("pangrova.speculation.single-task-duration-threshold-ms", 0L);
    }
    public boolean pullModeSplitAssignmentEnabled() { return featureFlags.pullModeSplitAssignment(); }
    public boolean cacheWritebackEnabled() { return featureFlags.cacheWriteback(); }
    public boolean indexingEnabled() { return featureFlags.indexing(); }
    public boolean embeddingEnabled() {
        return featureFlags.embedding();
    }

    /**
     * Returns true if DDL leader routing is enabled. When enabled, DDL statements
     * on non-leader coordinators are rejected with HTTP 307 redirect to the leader.
     * Default is false (DDL can execute on any coordinator).
     */
    public boolean ddlLeaderRoutingEnabled() {
        return featureFlags.ddlLeaderRouting();
    }
    public ShuffleMode shuffleMode() { return featureFlags.shuffleMode(); }
    public List<String> shuffleEndpoints() { return featureFlags.shuffleEndpoints(); }

    /**
     * Returns the base index path for index file storage.
     * Reads from the {@code pangrova.index.basePath} system property.
     * Fails fast if not configured when indexing is enabled.
     */
    public String baseIndexPath() {
        String path = System.getProperty("pangrova.index.basePath");
        if (indexingEnabled() && (path == null || path.isBlank())) {
            throw new IllegalStateException(
                    "baseIndexPath is required when indexing is enabled. "
                            + "Set system property 'pangrova.index.basePath'");
        }
        return path != null
            ? path
            : Path.of(System.getProperty("java.io.tmpdir"), "pangrova", "indexes").toString();
    }

    /** Returns the internal auth secret from security config, or null if not configured. */
    public String internalAuthSecret() {
        return securityConfig != null ? securityConfig.internalAuthSecret() : null;
    }

    public long queryTimeoutSeconds() { return query.queryTimeoutSeconds(); }
    public long timeoutCheckIntervalSeconds() { return query.timeoutCheckIntervalSeconds(); }
    public long taskTimeoutSeconds() { return query.taskTimeoutSeconds(); }
    public long queryRetentionSeconds() { return query.queryRetentionSeconds(); }
    public long queryCleanupIntervalSeconds() { return query.queryCleanupIntervalSeconds(); }

    public int coordinatorTransportPort() { return transport.port(); }
    public long coordinatorTransportAllocatorLimitBytes() { return transport.allocatorLimitBytes(); }
    public String coordinatorTransportAdvertisedHost() { return transport.advertisedHost(); }
    public ShuffleTlsConfig shuffleTlsConfig() { return transport.shuffleTls(); }

    public long workerSuspectThresholdMs() { return workerHealth.suspectThresholdMs(); }
    public long workerDeadThresholdMs() { return workerHealth.deadThresholdMs(); }
    public long workerHealthCheckIntervalMs() { return workerHealth.checkIntervalMs(); }

    /**
     * Returns the effective per-task timeout in seconds.
     * If {@code taskTimeoutSeconds} is 0 (default), derives from {@code queryTimeoutSeconds / 2}.
     */
    public long effectiveTaskTimeoutSeconds() {
        return query.taskTimeoutSeconds() > 0
                ? query.taskTimeoutSeconds()
                : query.queryTimeoutSeconds() / 2;
    }

    // -- Sub-config records --

    /** Core identity and lifecycle parameters. */
    public record CoreConfig(String coordinatorId, int httpPort, long shutdownTimeoutMs,
                             CoordinatorDeploymentRole role, String httpHost, Optional<Path> pluginDirectory) {
        public CoreConfig(String coordinatorId, int httpPort, long shutdownTimeoutMs,
                          CoordinatorDeploymentRole role, String httpHost) {
            this(coordinatorId, httpPort, shutdownTimeoutMs, role, httpHost, Optional.empty());
        }

        public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

        public CoreConfig {
            java.util.Objects.requireNonNull(pluginDirectory, "pluginDirectory");
            if (coordinatorId == null || coordinatorId.isBlank()) {
                throw new IllegalArgumentException("coordinatorId must not be blank");
            }
            if (httpPort < 0) {
                throw new IllegalArgumentException("httpPort must be >= 0");
            }
            if (role == null) {
                role = CoordinatorDeploymentRole.COMBINED;
            }
            if (httpHost == null || httpHost.isBlank()) {
                throw new IllegalArgumentException("HTTP bind host must not be blank");
            }
            httpHost = httpHost.trim();
        }
    }

    /** Feature toggle flags for optional modules. */
    public record FeatureFlags(
            boolean observability, boolean security, boolean metadata,
            boolean streaming, boolean governance, boolean resilience,
            boolean tenant, boolean operations,
            boolean speculation, boolean pullModeSplitAssignment,
            boolean cacheWriteback, boolean indexing,
            boolean embedding, boolean ddlLeaderRouting,
            ShuffleMode shuffleMode, List<String> shuffleEndpoints, String shuffleProvider,
            java.util.Map<String, String> shuffleProviderSettings) {

        public FeatureFlags {
            if (shuffleProvider == null || shuffleProvider.isBlank()) throw new IllegalArgumentException("Blank shuffle provider");
            shuffleProviderSettings = java.util.Map.copyOf(shuffleProviderSettings);
            if (shuffleMode == null) shuffleMode = ShuffleMode.NATIVE;
            if (shuffleEndpoints == null) shuffleEndpoints = List.of();
            else shuffleEndpoints = List.copyOf(shuffleEndpoints);
        }

        public FeatureFlags(boolean observability, boolean security, boolean metadata, boolean streaming,
                boolean governance, boolean resilience, boolean tenant, boolean operations, boolean speculation,
                boolean pullModeSplitAssignment, boolean cacheWriteback, boolean indexing, boolean embedding,
                boolean ddlLeaderRouting, ShuffleMode shuffleMode, List<String> shuffleEndpoints) {
            this(observability, security, metadata, streaming, governance, resilience, tenant, operations, speculation,
                    pullModeSplitAssignment, cacheWriteback, indexing, embedding, ddlLeaderRouting, shuffleMode,
                    shuffleEndpoints, CoordinatorOptions.SHUFFLE_PROVIDER.defaultValue(), java.util.Map.of());
        }

        // Existing positional config adapter preserves the declared feature ordering.
        @SuppressWarnings("checkstyle:ParameterNumber")
        public FeatureFlags(boolean observability, boolean security, boolean metadata,
                            boolean streaming, boolean governance, boolean resilience,
                            boolean tenant, boolean operations,
                            boolean speculation, boolean pullModeSplitAssignment,
                            boolean cacheWriteback, boolean indexing) {
            this(observability, security, metadata, streaming, governance, resilience,
                    tenant, operations, speculation, pullModeSplitAssignment,
                    cacheWriteback, indexing, false, false, ShuffleMode.NATIVE, List.of());
        }

        public FeatureFlags(boolean observability, boolean security, boolean metadata,
                            boolean streaming, boolean governance, boolean resilience,
                            boolean tenant, boolean operations) {
            this(observability, security, metadata, streaming, governance, resilience,
                    tenant, operations, false, false, false, false,
                    false, false, ShuffleMode.NATIVE, List.of());
        }

        @Override
        public String toString() {
            return "FeatureFlags[observability=" + observability + ", security=" + security + ", metadata=" + metadata
                    + ", streaming=" + streaming + ", governance=" + governance + ", resilience=" + resilience
                    + ", tenant=" + tenant + ", operations=" + operations + ", speculation=" + speculation
                    + ", pullModeSplitAssignment=" + pullModeSplitAssignment + ", cacheWriteback=" + cacheWriteback
                    + ", indexing=" + indexing + ", embedding=" + embedding + ", ddlLeaderRouting=" + ddlLeaderRouting
                    + ", shuffleMode=" + shuffleMode + ", shuffleProvider=" + shuffleProvider
                    + ", shuffleEndpointCount=" + shuffleEndpoints.size()
                    + ", configuredShuffleProperties=" + shuffleProviderSettings.keySet() + "]";
        }

        public static FeatureFlags allDisabled() {
            return new FeatureFlags(false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, ShuffleMode.NATIVE, List.of());
        }
    }

    /** Query execution timeout parameters. */
    public record QueryConfig(
            long queryTimeoutSeconds, long timeoutCheckIntervalSeconds, long taskTimeoutSeconds,
            long queryRetentionSeconds, long queryCleanupIntervalSeconds,
            SpillBudget spillBudget, int workerMaxRetry, String placementProvider) {

        public static final long DEFAULT_QUERY_TIMEOUT_SECONDS = 300;
        public static final long DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS = 10;
        /** Default task timeout = queryTimeoutSeconds / 2. Use 0 to derive from query timeout at runtime. */
        public static final long DEFAULT_TASK_TIMEOUT_SECONDS = 0;
        public static final long DEFAULT_QUERY_RETENTION_SECONDS = 600;
        public static final long DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS = 60;
        public static final int DEFAULT_WORKER_MAX_RETRY = 3;

        public QueryConfig {
            if (placementProvider == null || placementProvider.isBlank()) {
                throw new IllegalArgumentException("coordinator.query.placement-provider must not be blank");
            }
            if (spillBudget == null) spillBudget = SpillBudget.DISABLED;
            if (workerMaxRetry < 0) {
                throw new IllegalArgumentException("workerMaxRetry must not be negative");
            }
        }

        /** Uses default retention, cleanup, and disabled task spilling. */
        public QueryConfig(long queryTimeoutSeconds, long timeoutCheckIntervalSeconds,
                           long taskTimeoutSeconds) {
            this(queryTimeoutSeconds, timeoutCheckIntervalSeconds, taskTimeoutSeconds,
                    DEFAULT_QUERY_RETENTION_SECONDS, DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS,
                    SpillBudget.DISABLED, DEFAULT_WORKER_MAX_RETRY, CoordinatorOptions.PLACEMENT_PROVIDER.defaultValue());
        }

        /** Uses disabled task spilling. */
        public QueryConfig(long queryTimeoutSeconds, long timeoutCheckIntervalSeconds,
                           long taskTimeoutSeconds, long queryRetentionSeconds,
                           long queryCleanupIntervalSeconds) {
            this(queryTimeoutSeconds, timeoutCheckIntervalSeconds, taskTimeoutSeconds,
                    queryRetentionSeconds, queryCleanupIntervalSeconds, SpillBudget.DISABLED,
                    DEFAULT_WORKER_MAX_RETRY, CoordinatorOptions.PLACEMENT_PROVIDER.defaultValue());
        }

        /** Returns the default query execution configuration. */
        public static QueryConfig defaults() {
            return new QueryConfig(
                    DEFAULT_QUERY_TIMEOUT_SECONDS,
                    DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS,
                    DEFAULT_TASK_TIMEOUT_SECONDS,
                    DEFAULT_QUERY_RETENTION_SECONDS,
                    DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS,
                    SpillBudget.DISABLED,
                    DEFAULT_WORKER_MAX_RETRY, CoordinatorOptions.PLACEMENT_PROVIDER.defaultValue());
        }
    }

    /** Transport layer parameters. */
    public record TransportConfig(
            int port,
            long allocatorLimitBytes,
            String advertisedHost,
            ShuffleTlsConfig shuffleTls) {
        public static final int DEFAULT_PORT = 0;
        private static final int MAX_PORT = 65_535;
        public static final long DEFAULT_ALLOCATOR_LIMIT_BYTES = 256L * 1024 * 1024;
        public static final String DEFAULT_ADVERTISED_HOST = "localhost";

        public TransportConfig {
            if (port < 0 || port > MAX_PORT) {
                throw new IllegalArgumentException("transport port must be in 0..65535");
            }
            if (allocatorLimitBytes <= 0) {
                throw new IllegalArgumentException("transport allocatorLimitBytes must be positive");
            }
            if (advertisedHost == null || advertisedHost.isBlank()) {
                throw new IllegalArgumentException("advertisedHost must not be blank");
            }
            advertisedHost = advertisedHost.trim();
            if (java.util.Set.of("0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0", "*").contains(advertisedHost)) {
                throw new IllegalArgumentException("transport advertisedHost must not be a wildcard bind address");
            }
        }

        /**
         * Returns true if the advertised host is a loopback address
         * (localhost, 127.0.0.1, or ::1).
         */
        public boolean isLoopbackAdvertisedHost() {
            String normalized = advertisedHost.trim().toLowerCase(Locale.ROOT);
            return "localhost".equals(normalized)
                    || "127.0.0.1".equals(normalized)
                    || "::1".equals(normalized);
        }

        public static TransportConfig defaults() {
            return new TransportConfig(
                    DEFAULT_PORT, DEFAULT_ALLOCATOR_LIMIT_BYTES,
                    DEFAULT_ADVERTISED_HOST, null);
        }
    }

    /** Worker health monitoring thresholds. */
    public record WorkerHealthConfig(long suspectThresholdMs, long deadThresholdMs, long checkIntervalMs) {
        public static final long DEFAULT_SUSPECT_THRESHOLD_MS = 20_000;
        public static final long DEFAULT_DEAD_THRESHOLD_MS = 60_000;
        public static final long DEFAULT_CHECK_INTERVAL_MS = 10_000;

        /** Returns the default worker health thresholds. */
        public static WorkerHealthConfig defaults() {
            return new WorkerHealthConfig(
                    DEFAULT_SUSPECT_THRESHOLD_MS,
                    DEFAULT_DEAD_THRESHOLD_MS,
                    DEFAULT_CHECK_INTERVAL_MS);
        }
    }

    /** JDBC connection parameters for the metadata store backend. */
    public record MetadataJdbcConfig(String jdbcUrl, String username, String password,
            MetadataCacheDeployment cache) {
        public MetadataJdbcConfig(String jdbcUrl, String username, String password) {
            this(jdbcUrl, username, password, MetadataCacheDeployment.defaults());
        }
        public MetadataJdbcConfig {
            java.util.Objects.requireNonNull(cache);
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                throw new IllegalArgumentException("jdbcUrl must not be blank");
            }
        }

        @Override
        public String toString() {
            return "MetadataJdbcConfig[jdbcUrl=<redacted>, username=<redacted>, password=<redacted>]";
        }
    }

    /** The selected factory owns the configuration schema and creates the process authenticator. */
    public record SecurityConfig(Optional<AuthenticatorFactory> clientAuthenticator,
                                 String internalAuthSecret, java.util.Map<String, String> authenticatorProperties) {
        public SecurityConfig(String providerId, String internalAuthSecret,
                java.util.Map<String, String> authenticatorProperties) {
            this(CoordinatorAuthenticatorFactories.select(providerId), internalAuthSecret, authenticatorProperties);
        }

        public SecurityConfig(AuthenticatorFactory factory, String internalAuthSecret,
                java.util.Map<String, String> authenticatorProperties) {
            this(Optional.of(factory), internalAuthSecret, authenticatorProperties);
        }

        public SecurityConfig {
            java.util.Objects.requireNonNull(clientAuthenticator, "clientAuthenticator");
            authenticatorProperties = java.util.Map.copyOf(authenticatorProperties);
            if (authenticatorProperties.containsKey("requireSignatureVerification")) {
                throw new IllegalArgumentException("Authentication signature bypass settings are not supported");
            }
            if (authenticatorProperties.containsKey("bypass.enabled")) {
                throw new IllegalArgumentException("Authentication bypass requires selecting the NOOP provider");
            }
            if (clientAuthenticator.isPresent()) {
                clientAuthenticator.orElseThrow().validate(authenticatorProperties);
            } else if (!authenticatorProperties.isEmpty()) {
                throw new IllegalArgumentException("Client authentication properties require a provider");
            }
        }

        static SecurityConfig internalRequestsOnly(String secret) {
            return new SecurityConfig(Optional.empty(), secret, java.util.Map.of());
        }

        public AuthenticatorFactory requireAuthenticator() {
            return clientAuthenticator.orElseThrow(() -> new IllegalStateException("Client authenticator is missing"));
        }

        boolean authenticatesClients() {
            return clientAuthenticator.isPresent() && !bypassesAuthentication();
        }

        boolean bypassesAuthentication() {
            return clientAuthenticator.isPresent() && "NOOP".equals(requireAuthenticator().providerId());
        }

        public static SecurityConfig noop() {
            return new SecurityConfig("NOOP", null, java.util.Map.of());
        }

        @Override
        public String toString() {
            return "SecurityConfig[type=" + clientAuthenticator.map(AuthenticatorFactory::providerId).orElse("disabled")
                    + ", configuredAuthProperties=" + authenticatorProperties.keySet() + "]";
        }
    }

    /** Returns the spill budget derived from query config, defaulting to disabled. */
    public SpillBudget spillBudget() {
        return query.spillBudget();
    }

    // -- Legacy constants for backward compatibility in tests --

    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS;
    public static final long DEFAULT_QUERY_TIMEOUT_SECONDS = QueryConfig.DEFAULT_QUERY_TIMEOUT_SECONDS;
    public static final long DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS =
            QueryConfig.DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS;
    public static final long DEFAULT_TASK_TIMEOUT_SECONDS = QueryConfig.DEFAULT_TASK_TIMEOUT_SECONDS;
    public static final int DEFAULT_COORDINATOR_TRANSPORT_PORT = TransportConfig.DEFAULT_PORT;
    public static final long DEFAULT_COORDINATOR_TRANSPORT_ALLOCATOR_LIMIT_BYTES =
            TransportConfig.DEFAULT_ALLOCATOR_LIMIT_BYTES;
    public static final long DEFAULT_WORKER_SUSPECT_THRESHOLD_MS = WorkerHealthConfig.DEFAULT_SUSPECT_THRESHOLD_MS;
    public static final long DEFAULT_WORKER_DEAD_THRESHOLD_MS = WorkerHealthConfig.DEFAULT_DEAD_THRESHOLD_MS;
    public static final long DEFAULT_WORKER_HEALTH_CHECK_INTERVAL_MS = WorkerHealthConfig.DEFAULT_CHECK_INTERVAL_MS;
}
