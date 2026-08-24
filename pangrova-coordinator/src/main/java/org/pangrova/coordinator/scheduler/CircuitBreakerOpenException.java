package org.pangrova.coordinator.scheduler;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when CircuitBreaker is OPEN and dispatch is rejected for fail-fast.
 */
public class CircuitBreakerOpenException extends PangrovaException {

    private final String workerHost;

    public CircuitBreakerOpenException(String workerHost) {
        super(PangrovaErrorCode.COORDINATOR_CIRCUIT_BREAKER_OPEN,
                "Circuit breaker is open for worker: " + workerHost);
        this.workerHost = workerHost;
    }

    public String getWorkerHost() {
        return workerHost;
    }
}
