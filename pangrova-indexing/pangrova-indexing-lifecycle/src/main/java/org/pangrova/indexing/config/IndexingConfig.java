package org.pangrova.indexing.config;

/**
 * Top-level immutable configuration for the indexing module.
 *
 * <p>Organized as a two-level hierarchy to satisfy the ≤4 constructor params
 * governance rule: {@link RuntimeConfig} groups runtime behavior, and
 * {@link LifecycleConfig} groups lifecycle management.
 *
 * @param baseIndexPath   root path for index file storage (must not be blank)
 * @param checksumEnabled whether CRC32 checksum verification is enabled
 * @param runtime         runtime behavior configuration (build, cache, eval)
 * @param lifecycle       lifecycle management configuration (GC, consistency, commit listener)
 */
public record IndexingConfig(
        String baseIndexPath,
        boolean checksumEnabled,
        RuntimeConfig runtime,
        LifecycleConfig lifecycle
) {

    public IndexingConfig {
        if (baseIndexPath == null || baseIndexPath.isBlank()) {
            throw new IllegalArgumentException("baseIndexPath must not be blank");
        }
        if (runtime == null) runtime = RuntimeConfig.DEFAULT;
        if (lifecycle == null) lifecycle = LifecycleConfig.DEFAULT;
    }

    // ── Convenience accessors ──

    public int buildThreadPoolSize() { return runtime.build().threadPoolSize(); }
    public int buildParallelism() { return runtime.build().parallelism(); }
    public int batchFlushSize() { return runtime.build().batchFlushSize(); }
    public long cacheSizeBytes() { return runtime.cache().cacheSizeBytes(); }
    public int evalParallelism() { return runtime.eval().parallelism(); }
    public long evalTimeoutMs() { return runtime.eval().evalTimeoutMs(); }
    public boolean includeStale() { return runtime.eval().includeStale(); }
    public int gcIntervalMinutes() { return lifecycle.gc().gcIntervalMinutes(); }
    public int gcGracePeriodMinutes() { return lifecycle.gc().gcGracePeriodMinutes(); }
    public boolean commitListenerEnabled() { return lifecycle.commitListener().enabled(); }
}
