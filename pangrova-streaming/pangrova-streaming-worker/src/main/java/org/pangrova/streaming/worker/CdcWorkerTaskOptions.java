package org.pangrova.streaming.worker;

import java.time.Duration;

/** Bounded polling configuration for one Worker CDC executor. */
public record CdcWorkerTaskOptions(
        int maxRecordsPerPoll,
        Duration pollTimeout,
        Duration idleTimeout) {

    public CdcWorkerTaskOptions {
        if (maxRecordsPerPoll <= 0
                || pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()
                || idleTimeout == null || idleTimeout.isNegative() || idleTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "CDC Worker options require bounded polling and positive timeouts");
        }
    }
}
