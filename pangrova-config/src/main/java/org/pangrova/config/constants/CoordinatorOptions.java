package org.pangrova.config.constants;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

import org.pangrova.config.enums.ShuffleMode;

import java.time.Duration;

/**
 * Typed configuration definitions for the Pangrova Coordinator.
 *
 * <p>All options are defined as {@code public static final ConfigOption<T>} constants
 * using the {@link ConfigOptions#key(String)} fluent builder. Feature flags default
 * to {@code true} except {@link #SECURITY_ENABLED} which defaults to {@code false},
 * per requirement R1.3.
 *
 * <p>This class is not instantiable — it serves as a namespace for coordinator config keys.
 */
public final class CoordinatorOptions {

    private CoordinatorOptions() {
        // static constants only
    }

    // ---- Server ----

    public static final ConfigOption<String> COORDINATOR_ID = ConfigOptions.key("coordinator.id")
            .stringType()
            .defaultValue("coordinator-1")
            .withDescription("Unique identifier for this coordinator instance")
            .build();

    public static final ConfigOption<String> COORDINATOR_HOST = ConfigOptions.key("coordinator.host")
            .stringType()
            .defaultValue("0.0.0.0")
            .withDescription("Bind address for the coordinator HTTP server")
            .build();

    public static final ConfigOption<Integer> COORDINATOR_PORT = ConfigOptions.key("coordinator.port")
            .intType()
            .defaultValue(8080)
            .withDescription("Listen port for the coordinator HTTP server")
            .build();

    public static final ConfigOption<String> COORDINATOR_ROLE = ConfigOptions.key("coordinator.role")
            .stringType()
            .defaultValue("COMBINED")
            .withDescription("Deployment role: QUERY (serve queries only), MAINTENANCE "
                    + "(background tasks only), or COMBINED (both, default)")
            .build();

    // ---- Internal transport security ----

    public static final ConfigOption<Integer> TRANSPORT_PORT = ConfigOptions.key("coordinator.transport.port")
            .intType().defaultValue(0)
            .withDescription("Coordinator acknowledgement listen port; zero selects an ephemeral port")
            .build();

    public static final ConfigOption<String> TRANSPORT_ADVERTISED_HOST =
            ConfigOptions.key("coordinator.transport.advertised-host")
                    .stringType().defaultValue("localhost")
                    .withDescription("Routable acknowledgement endpoint host matching its TLS certificate SAN")
                    .build();

    public static final ConfigOption<Long> TRANSPORT_ALLOCATOR_LIMIT_BYTES =
            ConfigOptions.key("coordinator.transport.allocator-limit-bytes")
                    .longType().defaultValue(256L * 1024 * 1024)
                    .withDescription("Positive maximum memory for Coordinator acknowledgement transport")
                    .build();

    public static final ConfigOption<String> INTERNAL_AUTH_SECRET =
            ConfigOptions.key("coordinator.internal-auth-secret")
                    .stringType()
                    .withDescription("Shared internal control-plane secret; "
                            + "production requires at least 32 UTF-8 bytes")
                    .build();

    public static final ConfigOption<Boolean> TRANSPORT_TLS_ENABLED =
            ConfigOptions.key("coordinator.transport.tls.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Require mutual TLS for Coordinator transport server and control-plane clients")
                    .build();

    public static final ConfigOption<String> TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH =
            ConfigOptions.key("coordinator.transport.tls.certificate-chain-path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Coordinator PEM certificate chain")
                    .build();

    public static final ConfigOption<String> TRANSPORT_TLS_PRIVATE_KEY_PATH =
            ConfigOptions.key("coordinator.transport.tls.private-key-path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Coordinator PEM private key")
                    .build();

    public static final ConfigOption<String> TRANSPORT_TLS_ROOT_CA_PATH =
            ConfigOptions.key("coordinator.transport.tls.root-ca-path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("PEM root CA for internal peer verification")
                    .build();

    public static final ConfigOption<String> SHUFFLE_TLS_SERVER_IDENTITY =
            ConfigOptions.key("coordinator.shuffle.tls.server-identity")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Certificate DNS identity expected from shuffle servers")
                    .build();

    public static final ConfigOption<ShuffleMode> SHUFFLE_MODE =
            ConfigOptions.key("coordinator.shuffle.mode")
                    .enumType(ShuffleMode.class)
                    .defaultValue(ShuffleMode.NATIVE)
                    .withDescription(
                            "NATIVE uses Arrow Flight shuffle; LOCAL pins disk-backed exchanges to one Worker; "
                            + "REMOTE delegates to the "
                            + "external shuffle service so a dead Worker's output can be re-read instead of "
                            + "recomputed. REMOTE requires endpoints, shared PostgreSQL execution authority "
                            + "and mutual TLS.")
                    .build();

    public static final ConfigOption<String> SHUFFLE_PROVIDER =
            ConfigOptions.key("coordinator.shuffle.provider").stringType().defaultValue("pangrova")
                    .withDescription("External shuffle coordinator factory ID for REMOTE exchanges.").build();

    public static final ConfigOption<String> SHUFFLE_ENDPOINTS =
            ConfigOptions.key("coordinator.shuffle.endpoints")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Comma-separated host:port list of external shuffle servers. "
                            + "Pangrova requires endpoints here or in its scoped provider settings.")
                    .build();

    // ---- Feature Flags (all default true except security) ----

    public static final ConfigOption<Boolean> METADATA_ENABLED = ConfigOptions.key("metadata.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the metadata module")
            .build();

    public static final ConfigOption<Boolean> GOVERNANCE_ENABLED = ConfigOptions.key("governance.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the governance module")
            .build();

    public static final ConfigOption<Boolean> RESILIENCE_ENABLED = ConfigOptions.key("resilience.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the resilience module")
            .build();

    public static final ConfigOption<Boolean> STREAMING_ENABLED = ConfigOptions.key("streaming.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the streaming module")
            .build();

    public static final ConfigOption<Boolean> TENANT_ENABLED = ConfigOptions.key("tenant.enabled")
            .booleanType()
            .defaultValue(false)
            .withDescription("Enable tenant isolation; requires authenticated user identities")
            .build();

    public static final ConfigOption<Boolean> INDEXING_ENABLED = ConfigOptions.key("indexing.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the indexing module")
            .build();

    public static final ConfigOption<Boolean> OPERATIONS_ENABLED = ConfigOptions.key("operations.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Enable the operations module")
            .build();

    public static final ConfigOption<Boolean> OBSERVABILITY_ENABLED = CommonOptions.OBSERVABILITY_ENABLED;

    public static final ConfigOption<Boolean> SECURITY_ENABLED = CommonOptions.SECURITY_ENABLED;

    public static final ConfigOption<String> SECURITY_AUTH_TYPE = ConfigOptions.key("security.auth.type")
            .stringType()
            .withDescription("ID of an installed Coordinator authenticator factory (NOOP is reserved for development)")
            .build();

    // ---- High Availability ----

    public static final ConfigOption<Boolean> HIGH_AVAILABILITY_ENABLED = ConfigOptions.key("coordinator.ha.enabled")
            .booleanType()
            .defaultValue(false)
            .withDescription("Enable coordinator high-availability mode")
            .build();

    public static final ConfigOption<Boolean> DDL_LEADER_ROUTING_ENABLED =
            ConfigOptions.key("coordinator.ha.ddl-leader-routing")
            .booleanType()
            .defaultValue(false)
            .withDescription("When enabled, DDL statements are routed exclusively to the leader "
                    + "coordinator (HTTP 307 redirect). When disabled (default), DDL can execute "
                    + "on any coordinator — metadata consistency is guaranteed by the connector's "
                    + "own concurrency control and the cross-node invalidation propagation mechanism.")
            .build();

    // ---- Query ----

    public static final ConfigOption<Boolean> SPECULATION_ENABLED =
            ConfigOptions.key("coordinator.speculation.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable speculative copies of straggling tasks with exact-attempt drain cleanup")
                    .build();

    public static final ConfigOption<String> PLACEMENT_PROVIDER = ConfigOptions.key(
            "coordinator.query.placement-provider")
            .stringType()
            .defaultValue("load-balanced")
            .withDescription("Installed worker placement strategy used to rank eligible workers")
            .build();

    public static final ConfigOption<Duration> QUERY_TIMEOUT = ConfigOptions.key("coordinator.query.timeout")
            .durationType()
            .defaultValue(Duration.ofMinutes(5))
            .withDescription("Maximum query execution time before timeout")
            .build();

    public static final ConfigOption<Integer> QUERY_MAX_CONCURRENT =
            ConfigOptions.key("coordinator.query.max-concurrent")
            .intType()
            .defaultValue(100)
            .withDescription("Maximum number of concurrently executing queries")
            .build();

    /** Per-task disk grant; the Worker independently owns the local directory. */
    public static final ConfigOption<Long> TASK_SPILL_MAX_BYTES =
            ConfigOptions.key("coordinator.task.spill.max-bytes")
                    .longType()
                    .defaultValue(8L * 1024 * 1024 * 1024)
                    .withDescription("Maximum spill bytes per task; zero disables task spilling. "
                            + "Requires worker.spill.directory and is bounded by Worker-local policy.")
                    .build();

    // ---- Worker Management ----

    public static final ConfigOption<Duration> WORKER_HEARTBEAT_TIMEOUT =
            ConfigOptions.key("coordinator.worker.heartbeat-timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(30))
            .withDescription("Duration after which a worker is considered dead if no heartbeat received")
            .build();

    public static final ConfigOption<Integer> WORKER_MAX_RETRY = ConfigOptions.key("coordinator.worker.max-retry")
            .intType()
            .defaultValue(3)
            .withDescription("Maximum number of task dispatch retries to a worker")
            .build();

    // ---- Metadata ----

    public static final ConfigOption<String> METADATA_JDBC_URL = ConfigOptions.key("metadata.jdbc-url")
            .stringType()
            .withDescription("JDBC URL for the metadata catalog store")
            .build();

    public static final ConfigOption<String> METADATA_JDBC_USERNAME = ConfigOptions.key("metadata.jdbc-username")
            .stringType()
            .defaultValue("")
            .withDescription("Metadata database username, supplied separately from the JDBC URL")
            .build();

    public static final ConfigOption<String> METADATA_JDBC_PASSWORD = ConfigOptions.key("metadata.jdbc-password")
            .stringType()
            .defaultValue("")
            .withDescription("Metadata database password, supplied separately from the JDBC URL")
            .build();

    public static final ConfigOption<String> METADATA_CACHE_L2_PROVIDER = ConfigOptions.key(
            "metadata.cache.l2.provider")
            .stringType().defaultValue("disabled")
            .withDescription("Metadata L2 provider ID (disabled, memory or redis)").build();

    public static final ConfigOption<Integer> METADATA_CACHE_L1_MAX_SIZE =
            ConfigOptions.key("metadata.cache.l1-max-size")
            .intType()
            .defaultValue(10_000)
            .withDescription("Maximum number of entries in the L1 metadata cache")
            .build();

    public static final ConfigOption<Duration> METADATA_CACHE_L1_TTL = ConfigOptions.key("metadata.cache.l1-ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(60))
            .withDescription("Time-to-live for L1 metadata cache entries")
            .build();
}
