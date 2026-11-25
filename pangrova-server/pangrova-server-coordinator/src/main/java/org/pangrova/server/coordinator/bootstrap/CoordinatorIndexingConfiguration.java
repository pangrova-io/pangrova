package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.IndexingOptions;
import org.pangrova.indexing.config.BuildConfig;
import org.pangrova.indexing.config.CacheConfig;
import org.pangrova.indexing.config.CommitListenerConfig;
import org.pangrova.indexing.config.ConsistencyConfig;
import org.pangrova.indexing.config.EvalConfig;
import org.pangrova.indexing.config.GcConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.config.RuntimeConfig;

import java.time.Duration;

/** Maps immutable deployment settings to the indexing lifecycle configuration. */
public final class CoordinatorIndexingConfiguration {
    private CoordinatorIndexingConfiguration() {}

    /** Resolves immutable indexing settings and the effective durable root. */
    public static IndexingConfig from(PangrovaConfiguration config, String basePath) {
        if (basePath == null || basePath.isBlank()) {
            basePath = config.getString(org.pangrova.config.constants.CoordinatorOptions.INDEX_BASE_PATH);
        }
        if (basePath == null || basePath.isBlank()) {
            basePath = System.getProperty("pangrova.index.basePath");
        }
        long cacheBytes = config.getLong(IndexingOptions.CACHE_SIZE_BYTES);
        var build = new BuildConfig(
                config.getInt(IndexingOptions.BUILD_THREAD_POOL_SIZE),
                config.getInt(IndexingOptions.BUILD_PARALLELISM),
                config.getInt(IndexingOptions.BUILD_BATCH_FLUSH_SIZE),
                config.getInt(IndexingOptions.BUILD_MAX_CONCURRENT_PER_TENANT));
        var cache = cacheBytes < 0 ? CacheConfig.DEFAULT : new CacheConfig(cacheBytes);
        var eval = new EvalConfig(
                config.getInt(IndexingOptions.EVAL_PARALLELISM),
                config.getLong(IndexingOptions.EVAL_TIMEOUT_MS),
                config.getInt(IndexingOptions.EVAL_CIRCUIT_FAILURE_THRESHOLD),
                config.getInt(IndexingOptions.EVAL_CIRCUIT_OPEN_SECONDS),
                config.getBoolean(IndexingOptions.EVAL_INCLUDE_STALE));
        var gc = new GcConfig(
                config.getInt(IndexingOptions.GC_INTERVAL_MINUTES),
                config.getInt(IndexingOptions.GC_GRACE_PERIOD_MINUTES));
        var consistency = new ConsistencyConfig(
                config.getInt(IndexingOptions.CONSISTENCY_CHECK_INTERVAL_MINUTES),
                config.getDouble(IndexingOptions.CONSISTENCY_REBUILD_THRESHOLD),
                config.getBoolean(IndexingOptions.CONSISTENCY_AUTO_REBUILD_ENABLED),
                config.getInt(IndexingOptions.CONSISTENCY_STUCK_MULTIPLIER));
        var commits = new CommitListenerConfig(
                config.getBoolean(IndexingOptions.COMMIT_LISTENER_ENABLED),
                config.getInt(IndexingOptions.COMMIT_LISTENER_POLL_BATCH_SIZE));
        return new IndexingConfig(basePath, true,
                new RuntimeConfig(build, cache, eval), new LifecycleConfig(gc, consistency, commits));
    }

    public static Duration buildTimeout(IndexingConfig config) {
        return Duration.ofMinutes(Math.max(1, config.lifecycle().consistency().checkIntervalMinutes()))
                .multipliedBy(config.lifecycle().consistency().buildStuckDetectionMultiplier());
    }
}
