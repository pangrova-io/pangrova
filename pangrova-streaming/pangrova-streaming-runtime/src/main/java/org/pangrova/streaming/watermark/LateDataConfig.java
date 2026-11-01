package org.pangrova.streaming.watermark;

import java.time.Duration;

/**
 * Configuration for late data handling in streaming aggregations.
 *
 * @param policy          the late data handling policy (DROP or UPDATE)
 * @param allowedLateness maximum lateness before a record is considered late
 */
public record LateDataConfig(
    LateDataPolicy policy,
    Duration allowedLateness
) {
    public static LateDataConfig defaults() {
        return new LateDataConfig(LateDataPolicy.DROP, Duration.ZERO);
    }

    public static LateDataConfig update(Duration allowedLateness) {
        return new LateDataConfig(LateDataPolicy.UPDATE, allowedLateness);
    }
}
