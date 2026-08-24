package org.pangrova.common.resilience;

/**
 * Always-open circuit breaker policy — permits all requests unconditionally.
 * Package-private singleton backing {@link CircuitBreakerPolicy#noOp()}.
 */
enum NoOpCircuitBreakerPolicy implements CircuitBreakerPolicy {

    INSTANCE;

    @Override
    public boolean allowRequest(String target) {
        return true;
    }

    @Override
    public void recordSuccess(String target) {
        // no-op
    }

    @Override
    public void recordFailure(String target, Throwable cause) {
        // no-op
    }
}
