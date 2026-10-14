package org.pangrova.coordinator.bootstrap;

import java.nio.file.Path;
import java.util.List;

import org.pangrova.config.ShuffleMode;
import org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget;
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
        ObservabilityBootstrapConfig observabilityBootstrapConfig) {

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
                new CoreConfig(coordinatorId, httpPort, CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS),
                FeatureFlags.allDisabled(),
                QueryConfig.defaults(),
                TransportConfig.defaults(),
                WorkerHealthConfig.defaults(),
                null, null, null);
    }

    // -- Convenience accessors delegating to sub-configs --

    public String coordinatorId() { return core.coordinatorId(); }
    public int httpPort() { return core.httpPort(); }
    public long shutdownTimeoutMs() { return core.shutdownTimeoutMs(); }
    public CoordinatorRole role() { return core.role(); }

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
                    || securityConfig.authenticatorType()
                        == SecurityConfig.AuthenticatorType.NOOP)) {
            throw new IllegalStateException(
                    "Tenant isolation requires an enabled, non-NOOP authenticator");
        }
    }
    public boolean operationsEnabled() { return featureFlags.operations(); }
    public boolean speculationEnabled() { return featureFlags.speculation(); }
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
    public record CoreConfig(String coordinatorId, int httpPort, long shutdownTimeoutMs, CoordinatorRole role) {
        public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

        public CoreConfig {
            if (coordinatorId == null || coordinatorId.isBlank()) {
                throw new IllegalArgumentException("coordinatorId must not be blank");
            }
            if (httpPort < 0) {
                throw new IllegalArgumentException("httpPort must be >= 0");
            }
            if (role == null) {
                role = CoordinatorRole.COMBINED;
            }
        }

        /** Backward-compatible constructor defaulting to COMBINED role. */
        public CoreConfig(String coordinatorId, int httpPort, long shutdownTimeoutMs) {
            this(coordinatorId, httpPort, shutdownTimeoutMs, CoordinatorRole.COMBINED);
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
            ShuffleMode shuffleMode, List<String> shuffleEndpoints) {

        public FeatureFlags {
            if (shuffleMode == null) shuffleMode = ShuffleMode.NATIVE;
            if (shuffleEndpoints == null) shuffleEndpoints = List.of();
            else shuffleEndpoints = List.copyOf(shuffleEndpoints);
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

        public static FeatureFlags allDisabled() {
            return new FeatureFlags(false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, ShuffleMode.NATIVE, List.of());
        }
    }

    /** Query execution timeout parameters. */
    public record QueryConfig(
            long queryTimeoutSeconds, long timeoutCheckIntervalSeconds, long taskTimeoutSeconds,
            long queryRetentionSeconds, long queryCleanupIntervalSeconds,
            SpillConfig spillConfig) {

        public static final long DEFAULT_QUERY_TIMEOUT_SECONDS = 300;
        public static final long DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS = 10;
        /** Default task timeout = queryTimeoutSeconds / 2. Use 0 to derive from query timeout at runtime. */
        public static final long DEFAULT_TASK_TIMEOUT_SECONDS = 0;
        public static final long DEFAULT_QUERY_RETENTION_SECONDS = 600;
        public static final long DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS = 60;

        public QueryConfig {
            if (spillConfig == null) spillConfig = SpillConfig.DISABLED;
        }

        /** Backward-compatible constructor without retention/cleanup/spill params. */
        public QueryConfig(long queryTimeoutSeconds, long timeoutCheckIntervalSeconds,
                           long taskTimeoutSeconds) {
            this(queryTimeoutSeconds, timeoutCheckIntervalSeconds, taskTimeoutSeconds,
                    DEFAULT_QUERY_RETENTION_SECONDS, DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS,
                    SpillConfig.DISABLED);
        }

        /** Backward-compatible constructor without spill config. */
        public QueryConfig(long queryTimeoutSeconds, long timeoutCheckIntervalSeconds,
                           long taskTimeoutSeconds, long queryRetentionSeconds,
                           long queryCleanupIntervalSeconds) {
            this(queryTimeoutSeconds, timeoutCheckIntervalSeconds, taskTimeoutSeconds,
                    queryRetentionSeconds, queryCleanupIntervalSeconds, SpillConfig.DISABLED);
        }

        /** Returns the default query execution configuration. */
        public static QueryConfig defaults() {
            return new QueryConfig(
                    DEFAULT_QUERY_TIMEOUT_SECONDS,
                    DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS,
                    DEFAULT_TASK_TIMEOUT_SECONDS,
                    DEFAULT_QUERY_RETENTION_SECONDS,
                    DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS,
                    SpillConfig.DISABLED);
        }
    }

    /** Transport layer parameters. */
    public record TransportConfig(
            int port,
            long allocatorLimitBytes,
            String advertisedHost,
            ShuffleTlsConfig shuffleTls) {
        public static final int DEFAULT_PORT = 0;
        public static final long DEFAULT_ALLOCATOR_LIMIT_BYTES = 256L * 1024 * 1024;
        public static final String DEFAULT_ADVERTISED_HOST = "localhost";

        public TransportConfig {
            if (advertisedHost == null || advertisedHost.isBlank()) {
                throw new IllegalArgumentException("advertisedHost must not be blank");
            }
        }

        /**
         * Returns true if the advertised host is a loopback address
         * (localhost, 127.0.0.1, or ::1).
         */
        public boolean isLoopbackAdvertisedHost() {
            String normalized = advertisedHost.trim().toLowerCase();
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
    public record MetadataJdbcConfig(String jdbcUrl, String username, String password) {
        public MetadataJdbcConfig {
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                throw new IllegalArgumentException("jdbcUrl must not be blank");
            }
        }

        @Override
        public String toString() {
            return "MetadataJdbcConfig[jdbcUrl=<redacted>, username=<redacted>, password=<redacted>]";
        }
    }

    /** Security subsystem configuration: authenticator type and backend-specific properties. */
    public record SecurityConfig(AuthenticatorType authenticatorType,
                                 String internalAuthSecret, java.util.Map<String, String> authenticatorProperties) {
        public enum AuthenticatorType { NOOP, OAUTH2, LDAP }

        public SecurityConfig {
            if (authenticatorType == null) {
                throw new IllegalArgumentException("authenticatorType must not be null");
            }
            authenticatorProperties = java.util.Map.copyOf(authenticatorProperties);
            if (authenticatorType == AuthenticatorType.OAUTH2
                    && authenticatorProperties.getOrDefault("issuerUri", "").isBlank()) {
                throw new IllegalArgumentException("OAuth2 requires security.auth.properties.issuer-uri");
            }
            if (authenticatorProperties.containsKey("requireSignatureVerification")) {
                throw new IllegalArgumentException("Authentication signature bypass settings are not supported");
            }
            if (authenticatorType == AuthenticatorType.LDAP
                    && authenticatorProperties.getOrDefault("ldapUrl", "").isBlank()) {
                throw new IllegalArgumentException("LDAP requires security.auth.properties.ldap-url");
            }
        }

        public static SecurityConfig noop() {
            return new SecurityConfig(AuthenticatorType.NOOP, null, java.util.Map.of());
        }

        @Override
        public String toString() {
            return "SecurityConfig[type=" + authenticatorType
                    + ", configuredAuthProperties=" + authenticatorProperties.keySet() + "]";
        }

    }

    /** OTel bootstrap parameters for observability initialization. */
    public record ObservabilityBootstrapConfig(String serviceName, String otlpEndpoint) {
        public ObservabilityBootstrapConfig {
            if (serviceName == null || serviceName.isBlank()) {
                throw new IllegalArgumentException("serviceName must not be blank");
            }
            if (otlpEndpoint == null || otlpEndpoint.isBlank()) {
                throw new IllegalArgumentException("otlpEndpoint must not be blank");
            }
        }
    }

    /** Spill budget configuration for worker task resource limits. */
    public record SpillConfig(long maxSpillBytes, String spillDirectory) {
        public static final SpillConfig DISABLED = new SpillConfig(0, "");

        public SpillConfig {
            if (maxSpillBytes < 0) {
                throw new IllegalArgumentException("maxSpillBytes must be >= 0, got " + maxSpillBytes);
            }
            if (spillDirectory == null) {
                spillDirectory = "";
            }
        }
    }

    /**
     * L2 result cache configuration.
     * When {@code redisUrl} is non-null, Redis is used as the L2 cache backend.
     * Otherwise, an in-memory L2 cache is used.
     */
    public record CacheConfig(String redisUrl, long ttlSeconds, long maxEntryBytes) {
        public static final CacheConfig IN_MEMORY = new CacheConfig(null, 3600, 10_485_760);

        public CacheConfig {
            if (ttlSeconds < 0) {
                throw new IllegalArgumentException("ttlSeconds must be >= 0, got " + ttlSeconds);
            }
            if (maxEntryBytes < 0) {
                throw new IllegalArgumentException("maxEntryBytes must be >= 0, got " + maxEntryBytes);
            }
        }

        /** Returns true if Redis is configured as the L2 cache backend. */
        public boolean isRedisEnabled() {
            return redisUrl != null && !redisUrl.isBlank();
        }
    }

    /**
     * Returns the L2 cache configuration.
     * Reads from system properties once at construction time and caches the result.
     * Defaults to in-memory cache when Redis URL is not set.
     */
    private static final CacheConfig RESOLVED_CACHE_CONFIG = resolveCacheConfig();

    public CacheConfig cacheConfig() {
        return RESOLVED_CACHE_CONFIG;
    }

    private static CacheConfig resolveCacheConfig() {
        String redisUrl = System.getProperty("pangrova.cache.result.redis-url");
        if (redisUrl != null && !redisUrl.isBlank()) {
            long ttl = Long.parseLong(System.getProperty("pangrova.cache.result.ttl-seconds", "3600"));
            long max = Long.parseLong(System.getProperty("pangrova.cache.result.max-entry-bytes", "10485760"));
            return new CacheConfig(redisUrl, ttl, max);
        }
        return CacheConfig.IN_MEMORY;
    }

    /** Returns the spill budget derived from query config, defaulting to disabled. */
    public SpillBudget spillBudget() {
        SpillConfig spill = query.spillConfig();
        return new SpillBudget(spill.maxSpillBytes(), spill.spillDirectory());
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
