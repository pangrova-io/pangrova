package org.pangrova.streaming.enums;

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
     * Update the aggregation by emitting a retraction of the previous result
     * followed by the corrected result. Downstream consumers must handle
     * retraction semantics (e.g., changelog streams).
     */
    UPDATE,

    /**
     * Route late records to a side-output table for separate processing.
     * The main aggregation result is not modified. Late data can be
     * reconciled asynchronously.
     */
    SIDE_OUTPUT
}
