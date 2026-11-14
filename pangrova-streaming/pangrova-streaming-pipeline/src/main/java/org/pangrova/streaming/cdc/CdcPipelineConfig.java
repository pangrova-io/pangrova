package org.pangrova.streaming.cdc;

import org.pangrova.streaming.api.commit.SourceId;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for a single CDC pipeline loop.
 *
 * @param streamName              unique stream identifier
 * @param sourceId                durable identity of this stream creation
 * @param targetTable             fully qualified target table name
 * @param batchSize               maximum records per poll batch
 * @param pollTimeout             maximum wait time for poll
 * @param pollPauseIntervalMs     sleep duration when quota enforcer pauses polling
 * @param maxConsecutiveFailures  failure threshold before transitioning to FAILED
 * @param maxRetriesPerOffset     maximum retries for the same offset before failing the pipeline
 * @param flushInterval           interval between forced flushes
 */
public record CdcPipelineConfig(
    String streamName,
    SourceId sourceId,
    String targetTable,
    int batchSize,
    Duration pollTimeout,
    long pollPauseIntervalMs,
    int maxConsecutiveFailures,
    int maxRetriesPerOffset,
    Duration flushInterval
) {

    public CdcPipelineConfig {
        Objects.requireNonNull(streamName, "streamName must not be null");
        Objects.requireNonNull(sourceId, "sourceId must not be null");
        Objects.requireNonNull(targetTable, "targetTable must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
        if (pollPauseIntervalMs <= 0) {
            throw new IllegalArgumentException(
                "pollPauseIntervalMs must be positive, got: " + pollPauseIntervalMs);
        }
        if (maxConsecutiveFailures <= 0) {
            throw new IllegalArgumentException(
                "maxConsecutiveFailures must be positive, got: " + maxConsecutiveFailures);
        }
        if (maxRetriesPerOffset <= 0) {
            throw new IllegalArgumentException(
                "maxRetriesPerOffset must be positive, got: " + maxRetriesPerOffset);
        }
        Objects.requireNonNull(flushInterval, "flushInterval must not be null");
    }
}
