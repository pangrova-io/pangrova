package org.pangrova.planner.core.throttle;

import java.time.Instant;

/**
 * Represents an acquired throttle permit for a query.
 */
public record ThrottlePermit(
        String user,
        String tenant,
        Instant acquiredAt
) {}
