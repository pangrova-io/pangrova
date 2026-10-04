package org.pangrova.protocol.task;

import java.time.Duration;

/** Finite metadata-delivery bounds; none of these durations authorize task execution. */
public final class TerminalReportDeliveryPolicy {
    public static final int ATTEMPTS_PER_ROUND = 5;
    public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration INITIAL_BACKOFF = Duration.ofSeconds(1);
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    // Two complete worst-case rounds plus one cleanup-scheduler retry interval.
    public static final Duration DELIVERY_HORIZON = Duration.ofSeconds(360);
    // A round admitted just before the horizon can still have five in-flight attempts.
    // First acceptance cannot precede first sending; retain ACK facts for that full tail.
    public static final Duration ACK_RETENTION = DELIVERY_HORIZON
            .plus(REQUEST_TIMEOUT.multipliedBy(ATTEMPTS_PER_ROUND)).plusSeconds(15);
    public static final int MAX_RETAINED_IDENTITIES = 50_000;

    private TerminalReportDeliveryPolicy() { }
}
