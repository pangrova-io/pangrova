package org.pangrova.streaming.core.pipeline;

import java.time.Duration;

public record StreamingWriterConfig(
    long maxBufferBytes,
    int maxBufferRows,
    Duration maxBufferTime
) {
    public static StreamingWriterConfig defaults() {
        return new StreamingWriterConfig(
            128L * 1024 * 1024, 1_000_000, Duration.ofMinutes(1));
    }
}
