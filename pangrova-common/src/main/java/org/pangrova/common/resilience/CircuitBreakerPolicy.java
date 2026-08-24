package org.pangrova.common.resilience;

/**
 * SPI contract for circuit breaker behavior in task dispatch.
 *
 * <p>Implementations control whether requests to a target (typically a Worker node)
 * should be allowed based on recent failure history. This decouples the coordinator
 * from any specific circuit breaker implementation in the resilience module.</p>
 */
public interface CircuitBreakerPolicy {

    /**
     * Returns a no-op policy that permits all requests unconditionally.
     * Useful as a default when no resilience module is wired.
     */
    static CircuitBreakerPolicy noOp() {
        return NoOpCircuitBreakerPolicy.INSTANCE;
    }

    /**
     * Returns {@code true} if a request to the given target should be allowed.
     *
     * @param target identifier of the target (e.g., worker host)
     * @return {@code true} if the circuit is closed or half-open and accepting probes
     */
    boolean allowRequest(String target);

    /**
     * Records a successful interaction with the given target.
     *
     * @param target identifier of the target
     */
    void recordSuccess(String target);

    /**
     * Records a failed interaction with the given target.
     *
     * @param target identifier of the target
     * @param cause  the failure cause
     */
    void recordFailure(String target, Throwable cause);
}
