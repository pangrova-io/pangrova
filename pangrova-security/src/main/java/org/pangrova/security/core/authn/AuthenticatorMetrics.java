package org.pangrova.security.core.authn;

import java.util.Objects;
import org.pangrova.spi.security.authn.AuthenticatorHealthStatus;

/**
 * Immutable snapshot of authentication metrics for a single authenticator.
 *
 * <p>Captures success/failure counts, cumulative latency, and current health status.
 * Instances are created by {@link AuthenticationManager} and exposed for monitoring.
 *
 * @param authenticatorName unique name of the authenticator
 * @param successCount      total successful authentications
 * @param failureCount      total failed authentications (including exceptions)
 * @param totalLatencyMs    cumulative authentication latency in milliseconds
 * @param healthStatus      current health status
 * @param consecutiveFailures number of consecutive failures (resets on success)
 */
public record AuthenticatorMetrics(
        String authenticatorName,
        long successCount,
        long failureCount,
        long totalLatencyMs,
        AuthenticatorHealthStatus healthStatus,
        int consecutiveFailures
) {
    public AuthenticatorMetrics {
        Objects.requireNonNull(authenticatorName, "authenticatorName must not be null");
        Objects.requireNonNull(healthStatus, "healthStatus must not be null");
    }

    /**
     * Average authentication latency in milliseconds, or 0 if no attempts recorded.
     */
    public long averageLatencyMs() {
        long total = successCount + failureCount;
        return total > 0 ? totalLatencyMs / total : 0;
    }
}
