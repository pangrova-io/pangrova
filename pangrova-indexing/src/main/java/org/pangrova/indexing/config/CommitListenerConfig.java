package org.pangrova.indexing.config;

/**
 * Configuration for the commit listener that triggers incremental index builds.
 *
 * @param enabled       whether the commit listener is enabled (default true)
 * @param queueCapacity max pending commit events in the queue (default 1024)
 * @param batchMax      max events processed per batch (default 64)
 */
public record CommitListenerConfig(boolean enabled, int queueCapacity, int batchMax) {

    public static final CommitListenerConfig DEFAULT = new CommitListenerConfig(true, 1024, 64);

    public CommitListenerConfig {
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1, got " + queueCapacity);
        }
        if (batchMax < 1) {
            throw new IllegalArgumentException("batchMax must be >= 1, got " + batchMax);
        }
    }
}
