package org.pangrova.streaming.watermark;

import java.time.Duration;

/**
 * Configuration for late data handling in streaming aggregations.
 *
 * @param policy                the late data handling policy (DROP, UPDATE, or SIDE_OUTPUT)
 * @param sideOutputTableSuffix suffix appended to the table name for side-output routing
 * @param lateDataRetention     how long to retain late data in the side-output table
 * @param allowedLateness       maximum lateness before a record is considered late
 */
public record LateDataConfig(
    LateDataPolicy policy,
    String sideOutputTableSuffix,
    Duration lateDataRetention,
    Duration allowedLateness
) {
    public static LateDataConfig defaults() {
        return new LateDataConfig(
            LateDataPolicy.DROP, "_late_data", Duration.ofDays(7), Duration.ZERO);
    }

    public static LateDataConfig sideOutput() {
        return new LateDataConfig(
            LateDataPolicy.SIDE_OUTPUT, "_late_data", Duration.ofDays(7), Duration.ZERO);
    }

    public static LateDataConfig update(Duration allowedLateness) {
        return new LateDataConfig(
            LateDataPolicy.UPDATE, "_late_data", Duration.ofDays(7), allowedLateness);
    }

    /**
     * Returns whether late data handling is enabled (policy is not DROP).
     */
    public boolean enabled() {
        return policy != LateDataPolicy.DROP;
    }
}
