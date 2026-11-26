package org.pangrova.config.constants;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

import java.util.List;

/** Typed configuration definitions owned by the indexing lifecycle. */
public final class IndexingOptions {
    private IndexingOptions() {}

    public static final ConfigOption<Integer> BUILD_THREAD_POOL_SIZE =
            ConfigOptions.key("indexing.build.thread-pool-size")
                    .intType().defaultValue(4)
            .withDescription("Coordinator threads available to scalar index builds").build();
    public static final ConfigOption<Integer> BUILD_PARALLELISM = ConfigOptions.key("indexing.build.parallelism")
            .intType().defaultValue(4)
            .withDescription("Maximum concurrent scalar file builds per index").build();
    public static final ConfigOption<Integer> BUILD_BATCH_FLUSH_SIZE =
            ConfigOptions.key("indexing.build.batch-flush-size")
                    .intType().defaultValue(500)
            .withDescription("Number of scalar mappings accumulated before persistence").build();
    public static final ConfigOption<Integer> BUILD_MAX_CONCURRENT_PER_TENANT =
            ConfigOptions.key("indexing.build.max-concurrent-per-tenant")
                    .intType().defaultValue(2)
                    .withDescription("Maximum admitted scalar builds for one tenant").build();
    public static final ConfigOption<Long> CACHE_SIZE_BYTES = ConfigOptions.key("indexing.cache.size-bytes")
            .longType().defaultValue(-1L)
            .withDescription("Coordinator scalar evaluation cache bytes; "
                    + "negative selects the bounded heap-derived default").build();
    public static final ConfigOption<Integer> EVAL_PARALLELISM = ConfigOptions.key("indexing.eval.parallelism")
            .intType().defaultValue(8)
            .withDescription("Maximum parallel scalar split evaluations").build();
    public static final ConfigOption<Long> EVAL_TIMEOUT_MS = ConfigOptions.key("indexing.eval.timeout-ms")
            .longType().defaultValue(5_000L)
            .withDescription("Timeout for one scalar split evaluation").build();
    public static final ConfigOption<Integer> EVAL_CIRCUIT_FAILURE_THRESHOLD =
            ConfigOptions.key("indexing.eval.circuit-failure-threshold")
                    .intType().defaultValue(5)
                    .withDescription("Failures before the scalar evaluation circuit opens").build();
    public static final ConfigOption<Integer> EVAL_CIRCUIT_OPEN_SECONDS =
            ConfigOptions.key("indexing.eval.circuit-open-seconds")
                    .intType().defaultValue(60)
                    .withDescription("Seconds the scalar evaluation circuit stays open").build();
    public static final ConfigOption<Boolean> EVAL_INCLUDE_STALE = ConfigOptions.key("indexing.eval.include-stale")
            .booleanType().defaultValue(true)
            .withDescription("Allow conservative evaluation hints for STALE scalar indexes").build();
    public static final ConfigOption<Integer> GC_INTERVAL_MINUTES = ConfigOptions.key("indexing.gc.interval-minutes")
            .intType().defaultValue(60)
            .withDescription("Interval between index garbage-collection passes").build();
    public static final ConfigOption<Integer> GC_GRACE_PERIOD_MINUTES =
            ConfigOptions.key("indexing.gc.grace-period-minutes")
                    .intType().defaultValue(1_440)
                    .withDescription("Grace period before dropped index objects are purged").build();
    public static final ConfigOption<Boolean> COMMIT_LISTENER_ENABLED =
            ConfigOptions.key("indexing.commit-listener.enabled")
                    .booleanType().defaultValue(false)
                    .withDescription("Enable durable commit-driven index maintenance "
                            + "after lifecycle qualification").build();
    public static final ConfigOption<Integer> COMMIT_LISTENER_POLL_BATCH_SIZE =
            ConfigOptions.key("indexing.commit-listener.poll-batch-size")
                    .intType().defaultValue(64)
                    .withDescription("Maximum durable commit events claimed per poll").build();
    public static final ConfigOption<Integer> CONSISTENCY_CHECK_INTERVAL_MINUTES =
            ConfigOptions.key("indexing.consistency.check-interval-minutes")
                    .intType().defaultValue(30)
                    .withDescription("Interval between index coverage checks").build();
    public static final ConfigOption<Double> CONSISTENCY_REBUILD_THRESHOLD =
            ConfigOptions.key("indexing.consistency.rebuild-threshold")
                    .doubleType().defaultValue(0.8)
                    .withDescription("Coverage ratio below which a refresh is requested").build();
    public static final ConfigOption<Boolean> CONSISTENCY_AUTO_REBUILD_ENABLED =
            ConfigOptions.key("indexing.consistency.auto-rebuild-enabled")
                    .booleanType().defaultValue(false)
                    .withDescription("Enable automatic index replacement after lifecycle qualification").build();
    public static final ConfigOption<Integer> CONSISTENCY_STUCK_MULTIPLIER =
            ConfigOptions.key("indexing.consistency.stuck-detection-multiplier")
                    .intType().defaultValue(2)
                    .withDescription("Build-time multiplier used to detect stuck generations").build();
    public static final ConfigOption<java.time.Duration> ARTIFACT_DISPATCH_LEASE =
            ConfigOptions.key("indexing.artifact.dispatch-lease")
                    .durationType().defaultValue(java.time.Duration.ofMinutes(2))
                    .withDescription("Lease duration for one durable Native build page").build();
    public static final ConfigOption<java.time.Duration> ARTIFACT_DISPATCH_RETRY_DELAY =
            ConfigOptions.key("indexing.artifact.dispatch-retry-delay")
                    .durationType().defaultValue(java.time.Duration.ofSeconds(5))
                    .withDescription("Backoff before a failed Native build page can be retried").build();
    public static final ConfigOption<Integer> ARTIFACT_DISPATCH_MAX_IN_FLIGHT =
            ConfigOptions.key("indexing.artifact.dispatch-max-in-flight")
                    .intType().defaultValue(32)
                    .withDescription("Maximum Native build pages leased by one Coordinator").build();
    public static final ConfigOption<java.time.Duration> ARTIFACT_PUBLISH_CLAIM_TTL =
            ConfigOptions.key("indexing.artifact.publish-claim-ttl")
                    .durationType().defaultValue(java.time.Duration.ofSeconds(30))
                    .withDescription("Lease duration for atomic Native generation publication").build();
    public static final ConfigOption<java.time.Duration> ARTIFACT_PUBLISH_RETRY_DELAY =
            ConfigOptions.key("indexing.artifact.publish-retry-delay")
                    .durationType().defaultValue(java.time.Duration.ofSeconds(15))
                    .withDescription("Backoff before failed Native publication can be retried").build();
    public static final ConfigOption<java.time.Duration> ARTIFACT_PUBLISH_POLL_INTERVAL =
            ConfigOptions.key("indexing.artifact.publish-poll-interval")
                    .durationType().defaultValue(java.time.Duration.ofSeconds(1))
                    .withDescription("Poll interval for completed Native generations").build();
    public static final ConfigOption<Integer> ARTIFACT_PUBLISH_BATCH_SIZE =
            ConfigOptions.key("indexing.artifact.publish-batch-size")
                    .intType().defaultValue(64)
                    .withDescription("Maximum generations considered per publication poll").build();

    public static final List<ConfigOption<?>> ALL = List.of(
            BUILD_THREAD_POOL_SIZE, BUILD_PARALLELISM, BUILD_BATCH_FLUSH_SIZE,
            BUILD_MAX_CONCURRENT_PER_TENANT, CACHE_SIZE_BYTES, EVAL_PARALLELISM,
            EVAL_TIMEOUT_MS, EVAL_CIRCUIT_FAILURE_THRESHOLD, EVAL_CIRCUIT_OPEN_SECONDS,
            EVAL_INCLUDE_STALE, GC_INTERVAL_MINUTES, GC_GRACE_PERIOD_MINUTES,
            COMMIT_LISTENER_ENABLED, COMMIT_LISTENER_POLL_BATCH_SIZE,
            CONSISTENCY_CHECK_INTERVAL_MINUTES, CONSISTENCY_REBUILD_THRESHOLD,
            CONSISTENCY_AUTO_REBUILD_ENABLED, CONSISTENCY_STUCK_MULTIPLIER,
            ARTIFACT_DISPATCH_LEASE, ARTIFACT_DISPATCH_RETRY_DELAY,
            ARTIFACT_DISPATCH_MAX_IN_FLIGHT, ARTIFACT_PUBLISH_CLAIM_TTL,
            ARTIFACT_PUBLISH_RETRY_DELAY, ARTIFACT_PUBLISH_POLL_INTERVAL,
            ARTIFACT_PUBLISH_BATCH_SIZE);
}
