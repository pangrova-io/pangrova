package org.pangrova.config;

import java.time.Duration;

/**
 * Typed configuration definitions for the Pangrova Worker.
 *
 * <p>All options are defined as {@code public static final ConfigOption<T>} constants
 * using the {@link ConfigOptions#key(String)} fluent builder. Feature flags default
 * to {@code true} except {@link #SECURITY_ENABLED} which defaults to {@code false},
 * per requirement R1.3.
 *
 * <p>This class is not instantiable — it serves as a namespace for worker config keys.
 */
public final class WorkerOptions {

    private WorkerOptions() {
        // static constants only
    }

    // ---- Server ----

    public static final ConfigOption<String> WORKER_HOST = ConfigOptions.key("worker.host")
            .stringType()
            .defaultValue("0.0.0.0")
            .withDescription("Bind address for the worker HTTP server")
            .build();

    public static final ConfigOption<Integer> WORKER_PORT = ConfigOptions.key("worker.port")
            .intType()
            .defaultValue(8081)
            .withDescription("Listen port for the worker HTTP server")
            .build();

    public static final ConfigOption<String> WORKER_ADVERTISED_HOST = ConfigOptions.key("worker.advertised-host")
            .stringType()
            .withDescription("Hostname advertised to coordinator for control and result fetching")
            .build();

    public static final ConfigOption<String> SHUFFLE_ADVERTISED_HOST =
            ConfigOptions.key("worker.shuffle.advertised-host")
            .stringType()
            .withDescription("Peer-worker Native Flight shuffle hostname; defaults to worker.advertised-host")
            .build();

    public static final ConfigOption<String> WORKER_ID = ConfigOptions.key("worker.id")
            .stringType()
            .defaultValue("worker-1")
            .withDescription("Unique identifier for this worker instance")
            .build();

    // ---- Coordinator Connection ----

    public static final ConfigOption<String> COORDINATOR_URL = ConfigOptions.key("worker.coordinator-url")
            .stringType()
            .withDescription("URL of the coordinator to register with")
            .build();

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL = ConfigOptions.key("worker.heartbeat-interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("Interval between heartbeat reports to the coordinator")
            .build();

    // ---- Transport ----

    public static final ConfigOption<Integer> TRANSPORT_PORT = ConfigOptions.key("worker.transport.port")
            .intType()
            .defaultValue(0)
            .withDescription("Listen port for the Arrow Flight transport server (0 = auto-assign)")
            .build();

    public static final ConfigOption<Boolean> TRANSPORT_TLS_ENABLED =
            ConfigOptions.key("worker.transport.tls.enabled")
            .booleanType()
            .defaultValue(false)
            .withDescription("Require mutual TLS for native shuffle and result transport")
            .build();

    public static final ConfigOption<Integer> COPY_TASK_TLS_PORT =
            ConfigOptions.key("worker.copy.task-tls-port")
            .intType()
            .defaultValue(0)
            .withDescription("Dedicated mutual-TLS COPY task-control port (0 = disabled)")
            .build();

    public static final ConfigOption<String> TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH =
            ConfigOptions.key("worker.transport.tls.certificate-chain-path")
            .stringType()
            .defaultValue("")
            .withDescription("PEM certificate chain presented by the worker transport")
            .build();

    public static final ConfigOption<String> TRANSPORT_TLS_PRIVATE_KEY_PATH =
            ConfigOptions.key("worker.transport.tls.private-key-path")
            .stringType()
            .defaultValue("")
            .withDescription("PEM private key for the worker transport certificate")
            .build();

    public static final ConfigOption<String> TRANSPORT_TLS_ROOT_CA_PATH =
            ConfigOptions.key("worker.transport.tls.root-ca-path")
            .stringType()
            .defaultValue("")
            .withDescription("PEM root CA used for mutual peer verification")
            .build();

    public static final ConfigOption<String> TRANSPORT_TLS_IDENTITY =
            ConfigOptions.key("worker.transport.tls.identity")
            .stringType()
            .defaultValue("")
            .withDescription("Certificate DNS identity advertised to transport peers")
            .build();

    // ---- Task Execution ----

    public static final ConfigOption<Integer> RESULT_RETENTION_SECONDS =
            ConfigOptions.key("worker.task.result-retention-seconds")
            .intType()
            .defaultValue(300)
            .withDescription("Seconds to retain query results before expiration")
            .build();

    // ---- Shuffle ----

    public static final ConfigOption<Integer> SHUFFLE_BUFFER_CAPACITY =
            ConfigOptions.key("worker.shuffle.buffer-capacity")
            .intType()
            .defaultValue(100)
            .withDescription("Maximum number of batches per shuffle partition buffer")
            .build();

    public static final ConfigOption<Long> SHUFFLE_MAX_MEMORY_BYTES =
            ConfigOptions.key("worker.shuffle.max-memory-bytes")
            .longType()
            .defaultValue(512L * 1024 * 1024)
            .withDescription("Total memory budget in bytes for all shuffle partitions combined")
            .build();

    public static final ConfigOption<Double> SHUFFLE_SPILL_THRESHOLD =
            ConfigOptions.key("worker.shuffle.spill-threshold")
            .doubleType()
            .defaultValue(0.8)
            .withDescription("Memory usage ratio (0.0-1.0) that triggers shuffle spill to disk")
            .build();

    // ---- Memory / Resources ----
    public static final ConfigOption<Integer> SHUFFLE_CLIENT_MAX_OWNED_CHANNELS =
            ConfigOptions.key("worker.shuffle.client.max-owned-channels")
            .intType().defaultValue(100)
            .withDescription("Worker-wide owned and creating ManagedChannel limit across shuffle ingress and egress")
            .build();

    public static final ConfigOption<Integer> SHUFFLE_CLIENT_MAX_OWNED_CHANNELS_PER_ENDPOINT =
            ConfigOptions.key("worker.shuffle.client.max-owned-channels-per-endpoint")
            .intType().defaultValue(8)
            .withDescription("Owned and creating ManagedChannel limit per exact TLS-bound shuffle endpoint")
            .build();

    public static final ConfigOption<Long> MEMORY_BUDGET_BYTES = ConfigOptions.key("worker.memory.budget-bytes")
            .longType()
            .defaultValue(512L * 1024 * 1024)
            .withDescription("Total memory budget in bytes for the native execution engine")
            .build();

    public static final ConfigOption<Long> MEMORY_SYSTEM_RESERVED_BYTES =
            ConfigOptions.key("worker.memory.system-reserved-bytes")
            .longType()
            .defaultValue(64L * 1024 * 1024)
            .withDescription("Memory reserved for system overhead in bytes")
            .build();

    public static final ConfigOption<Long> MEMORY_NODE_BYTES = ConfigOptions.key("worker.memory.node-bytes")
            .longType()
            .defaultValue(1024L * 1024 * 1024)
            .withDescription("Total node memory pool capacity in bytes")
            .build();

    public static final ConfigOption<Double> MEMORY_PRESSURE_THRESHOLD_LOW =
            ConfigOptions.key("worker.memory.pressure-threshold-low")
            .doubleType()
            .defaultValue(50.0)
            .withDescription("Memory usage percentage that triggers LOW pressure level")
            .build();

    public static final ConfigOption<Double> MEMORY_PRESSURE_THRESHOLD_MEDIUM =
            ConfigOptions.key("worker.memory.pressure-threshold-medium")
            .doubleType()
            .defaultValue(70.0)
            .withDescription("Memory usage percentage that triggers MEDIUM pressure level")
            .build();

    public static final ConfigOption<Double> MEMORY_PRESSURE_THRESHOLD_HIGH =
            ConfigOptions.key("worker.memory.pressure-threshold-high")
            .doubleType()
            .defaultValue(85.0)
            .withDescription("Memory usage percentage that triggers HIGH pressure level")
            .build();

    public static final ConfigOption<Double> MEMORY_PRESSURE_THRESHOLD_CRITICAL =
            ConfigOptions.key("worker.memory.pressure-threshold-critical")
            .doubleType()
            .defaultValue(95.0)
            .withDescription("Memory usage percentage that triggers CRITICAL pressure level")
            .build();

    public static final ConfigOption<Integer> THREADS_QUERY = ConfigOptions.key("worker.threads.query")
            .intType()
            .defaultValue(4)
            .withDescription("Number of threads for query execution")
            .build();

    public static final ConfigOption<Integer> THREADS_IO = ConfigOptions.key("worker.threads.io")
            .intType()
            .defaultValue(2)
            .withDescription("Number of threads for I/O operations")
            .build();

    public static final ConfigOption<Integer> THREADS_SCHEDULER = ConfigOptions.key("worker.threads.scheduler")
            .intType()
            .defaultValue(1)
            .withDescription("Number of threads for the task scheduler")
            .build();

    public static final ConfigOption<String> SPILL_DIRECTORY = ConfigOptions.key("worker.spill.directory")
            .stringType()
            .withDescription("Directory for spilling intermediate data to disk")
            .build();

    public static final ConfigOption<Long> SHUFFLE_SERVICE_RETENTION_BYTES =
            ConfigOptions.key("worker.shuffle.service.retention-bytes")
                    .longType().defaultValue(16L * 1024 * 1024)
                    .withDescription("Node-reserved bytes for Native Flight shuffle retention")
                    .build();

    public static final ConfigOption<Long> SHUFFLE_SERVICE_TRANSIT_BYTES =
            ConfigOptions.key("worker.shuffle.service.transit-bytes")
                    .longType().defaultValue(16L * 1024 * 1024)
                    .withDescription("Node-reserved bytes for Native Flight replay and responses")
                    .build();

    public static final ConfigOption<Long> SHUFFLE_SERVICE_SPILL_BYTES =
            ConfigOptions.key("worker.shuffle.service.spill-bytes")
                    .longType().defaultValue(1024L * 1024 * 1024)
                    .withDescription("Hard byte cap for Native Flight shuffle spill files")
                    .build();

    public static final ConfigOption<Integer> SHUFFLE_SERVICE_SPILL_FILES =
            ConfigOptions.key("worker.shuffle.service.spill-files")
                    .intType().defaultValue(256)
                    .withDescription("Hard file cap for Native Flight shuffle spill files")
                    .build();

    public static final ConfigOption<String> SHUFFLE_SERVICE_SPILL_DIRECTORY =
            ConfigOptions.key("worker.shuffle.service.spill-directory")
                    .stringType()
                    .withDescription("Required existing, writable, absolute Native Flight spill directory")
                    .build();

    // ---- Native Engine ----

    public static final ConfigOption<String> NATIVE_PATH = ConfigOptions.key("worker.native.path")
            .stringType()
            .withDescription("Explicit path to the native library file")
            .build();

    public static final ConfigOption<String> NATIVE_WORKDIR = ConfigOptions.key("worker.native.workdir")
            .stringType()
            .withDescription("Working directory for native library extraction")
            .build();

    public static final ConfigOption<Boolean> NATIVE_DELETE_AFTER_LOAD =
            ConfigOptions.key("worker.native.delete-after-load")
            .booleanType()
            .defaultValue(true)
            .withDescription("Delete extracted native library after loading")
            .build();

    // ---- Native Cache ----

    public static final ConfigOption<Boolean> CACHE_ENABLED = ConfigOptions.key("worker.native.cache.enabled")
            .booleanType()
            .defaultValue(false)
            .withDescription("Enable native async data cache (in-memory + optional SSD)")
            .build();

    public static final ConfigOption<Long> CACHE_MEMORY_BYTES = ConfigOptions.key("worker.native.cache.memory-bytes")
            .longType()
            .defaultValue(8L * 1024 * 1024 * 1024)
            .withDescription("RAM budget in bytes for the in-memory data cache")
            .build();

    public static final ConfigOption<String> CACHE_SSD_PATH = ConfigOptions.key("worker.native.cache.ssd-path")
            .stringType()
            .defaultValue("")
            .withDescription("Local filesystem path for SSD-backed cache")
            .build();

    public static final ConfigOption<Long> CACHE_SSD_BYTES = ConfigOptions.key("worker.native.cache.ssd-bytes")
            .longType()
            .defaultValue(100L * 1024 * 1024 * 1024)
            .withDescription("SSD cache capacity in bytes")
            .build();

    public static final ConfigOption<Integer> CACHE_SSD_SHARDS = ConfigOptions.key("worker.native.cache.ssd-shards")
            .intType()
            .defaultValue(8)
            .withDescription("Number of SSD cache shards for parallel I/O")
            .build();

    public static final ConfigOption<Long> CACHE_CHECKPOINT_INTERVAL_BYTES =
            ConfigOptions.key("worker.native.cache.checkpoint-interval-bytes")
            .longType()
            .defaultValue(1L * 1024 * 1024 * 1024)
            .withDescription("SSD cache checkpoint interval in bytes. The native engine persists its SSD "
                    + "cache index every N bytes so the cache survives process restart (warm "
                    + "restart). Set to 0 to disable checkpointing (cache lost on restart).")
            .build();

    public static final ConfigOption<String> CACHE_ARTIFACT_PATH =
            ConfigOptions.key("worker.native.cache.artifact-path")
            .stringType()
            .defaultValue("")
            .withDescription("Dedicated local filesystem path for immutable AI/search artifacts")
            .build();

    public static final ConfigOption<Long> CACHE_ARTIFACT_BYTES =
            ConfigOptions.key("worker.native.cache.artifact-bytes")
            .longType()
            .defaultValue(100L * 1024 * 1024 * 1024)
            .withDescription("Independent SSD capacity for immutable AI/search artifact objects")
            .build();

    public static final ConfigOption<Long> CACHE_ARTIFACT_INDEX_MEMORY_BYTES =
            ConfigOptions.key("worker.native.cache.artifact-index-memory-bytes")
            .longType()
            .defaultValue(2L * 1024 * 1024 * 1024)
            .withDescription("Hard RAM quota for parsed immutable artifact indexes")
            .build();

    // ---- Registration Retry ----

    public static final ConfigOption<Integer> REGISTRATION_MAX_ATTEMPTS =
            ConfigOptions.key("worker.registration.max-attempts")
            .intType()
            .defaultValue(5)
            .withDescription("Maximum number of registration attempts to the coordinator")
            .build();

    public static final ConfigOption<Duration> REGISTRATION_INITIAL_BACKOFF =
            ConfigOptions.key("worker.registration.initial-backoff")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Initial backoff duration for registration retry")
            .build();

    public static final ConfigOption<Duration> REGISTRATION_MAX_BACKOFF =
            ConfigOptions.key("worker.registration.max-backoff")
            .durationType()
            .defaultValue(Duration.ofSeconds(30))
            .withDescription("Maximum backoff duration for registration retry")
            .build();

    // ---- Security ----

    public static final ConfigOption<String> INTERNAL_AUTH_SECRET = ConfigOptions.key("worker.internal-auth-secret")
            .stringType()
            .withDescription("Shared secret for X-Internal-Auth header between Coordinator and Worker")
            .build();

    // ---- Feature Flags (all default true except security, per R1.3) ----

    public static final ConfigOption<Boolean> OBSERVABILITY_ENABLED = CommonOptions.OBSERVABILITY_ENABLED;

    public static final ConfigOption<Boolean> CDC_ENABLED = ConfigOptions.key("worker.cdc.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable CDC (Change Data Capture) processing on this worker")
            .build();

    public static final ConfigOption<Boolean> SECURITY_ENABLED = CommonOptions.SECURITY_ENABLED;

    // ---- AI invocation governance ----

    public static final ConfigOption<String> AI_AUTHORIZATION_MODE =
            ConfigOptions.key("worker.ai.authorization.mode")
            .stringType()
            .defaultValue("deny")
            .withDescription("AI invocation policy: deny or authenticated")
            .build();

    public static final ConfigOption<String> AI_ALLOWED_MODELS =
            ConfigOptions.key("worker.ai.authorization.allowed-models")
            .stringType()
            .defaultValue("")
            .withDescription("Comma-separated exact AI model allowlist")
            .build();

    public static final ConfigOption<String> AI_ALLOWED_CAPABILITIES =
            ConfigOptions.key("worker.ai.authorization.allowed-capabilities")
            .stringType()
            .defaultValue("")
            .withDescription("Comma-separated exact AI capability allowlist")
            .build();

    public static final ConfigOption<String> AI_GRANTED_PERMISSIONS =
            ConfigOptions.key("worker.ai.authorization.granted-permissions")
            .stringType()
            .defaultValue("")
            .withDescription("Comma-separated permissions granted to authenticated AI task contexts")
            .build();

    public static final ConfigOption<Integer> AI_MAX_BATCH_SIZE =
            ConfigOptions.key("worker.ai.authorization.max-batch-size")
            .intType()
            .defaultValue(128)
            .withDescription("Maximum AI items admitted in one provider call")
            .build();

    public static final ConfigOption<Long> AI_MAX_INPUT_BYTES =
            ConfigOptions.key("worker.ai.authorization.max-input-bytes")
            .longType()
            .defaultValue(8L * 1024 * 1024)
            .withDescription("Maximum aggregate input bytes admitted in one AI provider call")
            .build();

    // ---- Observability ----

    public static final ConfigOption<String> OBSERVABILITY_SERVICE_NAME =
            ConfigOptions.key("observability.service-name")
            .stringType()
            .withDescription("OTel service name for this worker")
            .build();

    public static final ConfigOption<String> OBSERVABILITY_OTLP_ENDPOINT =
            ConfigOptions.key("observability.otlp-endpoint")
            .stringType()
            .withDescription("OTel OTLP exporter endpoint URL")
            .build();
}
