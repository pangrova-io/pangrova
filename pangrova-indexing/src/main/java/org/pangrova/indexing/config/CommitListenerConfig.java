package org.pangrova.indexing.config;

/**
 * Configuration for the commit listener that triggers incremental index builds.
 *
 * @param enabled       whether the durable commit listener is enabled (default true)
 * @param pollBatchSize maximum durable log entries claimed by one poll (default 64)
 */
public record CommitListenerConfig(boolean enabled, int pollBatchSize) {

    public static final CommitListenerConfig DEFAULT = new CommitListenerConfig(true, 64);

    public CommitListenerConfig {
        if (pollBatchSize < 1) {
            throw new IllegalArgumentException(
                    "pollBatchSize must be >= 1, got " + pollBatchSize);
        }
    }
}
