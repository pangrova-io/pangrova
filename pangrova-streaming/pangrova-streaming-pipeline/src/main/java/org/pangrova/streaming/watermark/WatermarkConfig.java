package org.pangrova.streaming.watermark;

import java.time.Duration;

/**
 * Watermark configuration per table.
 */
public record WatermarkConfig(
    String watermarkColumn,
    Duration watermarkDelay,
    Duration idleTimeout
) {
    public static WatermarkConfig defaults() {
        return new WatermarkConfig("event_time", Duration.ofSeconds(10), Duration.ofMinutes(5));
    }
}
