package org.pangrova.streaming.watermark;

/**
 * Defines how streaming aggregations handle late-arriving data
 * (records with event time before the current watermark).
 */
public enum LateDataPolicy {

    /**
     * Drop late records silently. No output is produced.
     * Suitable when late data is rare and correctness of aggregation
     * results is not critical.
     */
    DROP,

    /**
     * Keep the late record and let the sink apply its upsert/retraction
     * semantics so the target converges to the corrected result.
     */
    UPDATE
}
