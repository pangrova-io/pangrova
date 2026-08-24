package org.pangrova.security.session;

import java.time.Duration;

/**
 * Immutable configuration for session management.
 *
 * @param idleTimeout          duration after which an idle session expires
 * @param absoluteTimeout      maximum session lifetime regardless of activity
 * @param maxConcurrentPerUser maximum concurrent sessions allowed per user
 */
public record SessionConfig(
        Duration idleTimeout,
        Duration absoluteTimeout,
        int maxConcurrentPerUser) {

    private static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 30;

    public SessionConfig {
        if (idleTimeout == null || idleTimeout.isNegative() || idleTimeout.isZero()) {
            throw new IllegalArgumentException("idleTimeout must be positive: " + idleTimeout);
        }
        if (absoluteTimeout == null || absoluteTimeout.isNegative() || absoluteTimeout.isZero()) {
            throw new IllegalArgumentException("absoluteTimeout must be positive: " + absoluteTimeout);
        }
        if (maxConcurrentPerUser <= 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentPerUser must be positive: " + maxConcurrentPerUser);
        }
    }

    public static SessionConfig defaults() {
        return new SessionConfig(Duration.ofMinutes(DEFAULT_IDLE_TIMEOUT_MINUTES), Duration.ofHours(8), 5);
    }
}
