package org.pangrova.worker.bootstrap;

import org.pangrova.config.ShuffleMode;

/**
 * Configuration for Worker bootstrap sequence.
 * Groups all startup parameters into a single record to keep constructor params ≤ 4.
 *
 * <p><b>coordinatorUrl</b>: In multi-coordinator deployments, this should point
 * to the load-balancer VIP (e.g. {@code http://coordinator-lb:8080}), not a
 * single coordinator instance. Task status reports sent by
 * {@code RemoteTaskStatusReporter} are idempotent (keyed by taskId +
 * reportSequence), so routing through a load balancer is safe and ensures
 * reports reach a surviving coordinator after failover.</p>
 *
 * <p><b>httpHost vs advertisedHost</b>: {@code httpHost} ({@code worker.host}) is a
 * <em>bind</em> address and defaults to the wildcard {@code 0.0.0.0}.
 * {@code advertisedHost} ({@code worker.advertised-host}) is the address remote peers
 * and the coordinator use to reach this worker. Never advertise a wildcard bind
 * address — see {@code WorkerHttpBootstrap#resolveAdvertisedHost}.</p>
 */
public record WorkerBootstrapConfig(
        String workerId,
        int httpPort,
        int transportPort,
        String coordinatorUrl,
        long heartbeatIntervalMs,
        boolean observabilityEnabled,
        ObservabilityBootstrapConfig observabilityBootstrapConfig,
        ResourceConfig resourceConfig,
        int resultRetentionSeconds,
        boolean cdcEnabled,
        RegistrationRetryConfig registrationRetryConfig,
        String httpHost,
        String internalAuthSecret,
        ShuffleMode shuffleMode,
        CacheConfig cacheConfig,
        String advertisedHost) {

    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;
    public static final int DEFAULT_RESULT_RETENTION_SECONDS = 300;

    public WorkerBootstrapConfig {
        checkWorkerId(workerId);
        checkPort(httpPort, "httpPort");
        checkPort(transportPort, "transportPort");
        resourceConfig = orDefault(resourceConfig, ResourceConfig.defaults());
        if (resultRetentionSeconds <= 0) {
            resultRetentionSeconds = DEFAULT_RESULT_RETENTION_SECONDS;
        }
        registrationRetryConfig = orDefault(registrationRetryConfig, RegistrationRetryConfig.defaults());
        shuffleMode = orDefault(shuffleMode, ShuffleMode.NATIVE);
        cacheConfig = orDefault(cacheConfig, CacheConfig.defaults());
    }

    private static void checkWorkerId(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
    }

    private static void checkPort(int port, String name) {
        if (port < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static <T> T orDefault(T value, T fallback) {
        return value != null ? value : fallback;
    }

    /** Returns a configuration populated with default values for the given worker id. */
    public static WorkerBootstrapConfig withDefaults(String workerId) {
        return new WorkerBootstrapConfig(
                workerId, 0, 0, null, DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, ResourceConfig.defaults(), DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null, ShuffleMode.NATIVE, null, null);
    }

    /**
     * Resource allocation parameters for Worker runtime.
     */
    public record ResourceConfig(
            long memoryBudgetBytes,
            long systemReservedBytes,
            int queryThreads,
            int ioThreads,
            int schedulerThreads,
            String spillDirectory,
            long nodeMemoryBytes,
            double pressureThresholdLow,
            double pressureThresholdMedium,
            double pressureThresholdHigh,
            double pressureThresholdCritical) {

        public static final long DEFAULT_MEMORY_BUDGET = 512L * 1024 * 1024;
        public static final long DEFAULT_SYSTEM_RESERVED = 64L * 1024 * 1024;
        public static final long DEFAULT_NODE_MEMORY = 1024L * 1024 * 1024;

        /** Default memory-pressure thresholds (percent) for each escalation level. */
        public static final double DEFAULT_PRESSURE_THRESHOLD_LOW = 50.0;
        public static final double DEFAULT_PRESSURE_THRESHOLD_MEDIUM = 70.0;
        public static final double DEFAULT_PRESSURE_THRESHOLD_HIGH = 85.0;
        public static final double DEFAULT_PRESSURE_THRESHOLD_CRITICAL = 95.0;

        public ResourceConfig {
            if (nodeMemoryBytes <= 0) {
                nodeMemoryBytes = DEFAULT_NODE_MEMORY;
            }
        }

        /** Returns a resource configuration populated with default values. */
        public static ResourceConfig defaults() {
            return new ResourceConfig(
                    DEFAULT_MEMORY_BUDGET, DEFAULT_SYSTEM_RESERVED,
                    4, 2, 1, null, DEFAULT_NODE_MEMORY,
                    DEFAULT_PRESSURE_THRESHOLD_LOW, DEFAULT_PRESSURE_THRESHOLD_MEDIUM,
                    DEFAULT_PRESSURE_THRESHOLD_HIGH, DEFAULT_PRESSURE_THRESHOLD_CRITICAL);
        }
    }

    /**
     * OTel bootstrap parameters for observability initialization.
     */
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

    /**
     * Retry parameters for Worker → Coordinator registration with exponential backoff.
     */
    public record RegistrationRetryConfig(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {

        public static final int DEFAULT_MAX_ATTEMPTS = 5;
        public static final long DEFAULT_INITIAL_BACKOFF_MS = 1_000;
        public static final long DEFAULT_MAX_BACKOFF_MS = 30_000;

        public RegistrationRetryConfig {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            if (initialBackoffMs <= 0) {
                throw new IllegalArgumentException("initialBackoffMs must be > 0");
            }
            if (maxBackoffMs < initialBackoffMs) {
                throw new IllegalArgumentException("maxBackoffMs must be >= initialBackoffMs");
            }
        }

        public static RegistrationRetryConfig defaults() {
            return new RegistrationRetryConfig(
                    DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_MS);
        }
    }

    /**
     * Native async data cache parameters (native engine in-memory + optional SSD cache).
     * Disabled by default; enable in production on nodes with local NVMe storage.
     */
    public record CacheConfig(
            boolean enabled,
            long memoryCacheBytes,
            String ssdCachePath,
            long ssdCacheBytes,
            int ssdShards,
            long checkpointIntervalBytes) {

        /** Backward-compatible constructor (no checkpoint interval). */
        public CacheConfig(boolean enabled, long memoryCacheBytes,
                           String ssdCachePath, long ssdCacheBytes, int ssdShards) {
            this(enabled, memoryCacheBytes, ssdCachePath, ssdCacheBytes, ssdShards, 0);
        }

        public CacheConfig {
            if (ssdCachePath == null) {
                ssdCachePath = "/dev/null";
            }
            if (enabled && memoryCacheBytes <= 0) {
                throw new IllegalArgumentException("memoryCacheBytes must be positive when cache is enabled");
            }
            if (enabled && ssdShards <= 0) {
                throw new IllegalArgumentException("ssdShards must be positive when cache is enabled");
            }
            if (checkpointIntervalBytes < 0) {
                throw new IllegalArgumentException("checkpointIntervalBytes must be non-negative");
            }
        }

        public static CacheConfig defaults() {
            return new CacheConfig(false, 0, "/dev/null", 0, 1, 0);
        }
    }
}
