package org.pangrova.streaming.writer;

import java.time.Duration;

public record IncrementalWriterConfig(
    long maxBufferBytes,
    int maxBufferRows,
    Duration maxBufferTime
) {
    private static final int DEFAULT_MAX_BUFFER_ROWS = 1_000_000;

    public static IncrementalWriterConfig defaults() {
        return new IncrementalWriterConfig(
            128L * 1024 * 1024, DEFAULT_MAX_BUFFER_ROWS, Duration.ofMinutes(1));
    }
}
