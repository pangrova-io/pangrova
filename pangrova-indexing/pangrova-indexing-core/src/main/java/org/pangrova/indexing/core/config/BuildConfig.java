package org.pangrova.indexing.core.config;

/**
 * Configuration for index build operations.
 *
 * @param threadPoolSize         number of threads in the build pool (default 4)
 * @param parallelism            max concurrent file builds per index (default 4)
 * @param batchFlushSize         metadata batch flush threshold (default 500)
 * @param maxConcurrentPerTenant per-tenant max concurrent build tasks (default 2)
 */
public record BuildConfig(int threadPoolSize, int parallelism, int batchFlushSize, int maxConcurrentPerTenant) {

    public static final BuildConfig DEFAULT = new BuildConfig(4, 4, 500, 2);

    /** Backward-compatible constructor without per-tenant limit. */
    public BuildConfig(int threadPoolSize, int parallelism, int batchFlushSize) {
        this(threadPoolSize, parallelism, batchFlushSize, 2);
    }

    public BuildConfig {
        if (threadPoolSize < 1) {
            throw new IllegalArgumentException("threadPoolSize must be >= 1, got " + threadPoolSize);
        }
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got " + parallelism);
        }
        if (batchFlushSize < 1) {
            throw new IllegalArgumentException("batchFlushSize must be >= 1, got " + batchFlushSize);
        }
        if (maxConcurrentPerTenant < 1) {
            throw new IllegalArgumentException("maxConcurrentPerTenant must be >= 1, got " + maxConcurrentPerTenant);
        }
    }
}
