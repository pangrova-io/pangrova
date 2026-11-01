package org.pangrova.streaming.cdc;

import org.pangrova.spi.streaming.source.SourceId;

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
 * @param policies                watermark, late-data, dead-letter, and epoch policies
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
    CdcPipelinePolicies policies
) {

    /** Default-policy constructor for single-node/tests; production passes explicit policies. */
    public CdcPipelineConfig(String streamName, SourceId sourceId, String targetTable, int batchSize,
                      Duration pollTimeout, long pollPauseIntervalMs, int maxConsecutiveFailures,
                      int maxRetriesPerOffset) {
        this(streamName, sourceId, targetTable, batchSize, pollTimeout, pollPauseIntervalMs,
                maxConsecutiveFailures, maxRetriesPerOffset, CdcPipelinePolicies.defaults());
    }

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
        Objects.requireNonNull(policies, "policies must not be null");
    }
}
