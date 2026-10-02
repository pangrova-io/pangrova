package org.pangrova.coordinator.scheduler.failure;

import org.pangrova.spi.annotation.Internal;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception thrown when task dispatch to a Worker fails.
 * Carries an {@link ErrorCategory} for structured routing of failure handling.
 *
 * <p>Circuit breaker logic uses the category to count network, timeout and
 * Worker fatal failures, while settling backpressure and local fencing as healthy.</p>
 */
@Internal
public class TaskDispatchException extends PangrovaException {

    private final ErrorCategory category;
    private final String workerHost;

    public TaskDispatchException(ErrorCategory category, String workerHost,
                                 String message, Throwable cause) {
        super(toErrorCode(category), message, cause);
        this.category = category;
        this.workerHost = workerHost;
    }

    public TaskDispatchException(ErrorCategory category, String workerHost, String message) {
        super(toErrorCode(category), message);
        this.category = category;
        this.workerHost = workerHost;
    }

    public ErrorCategory getCategory() {
        return category;
    }

    public String getWorkerHost() {
        return workerHost;
    }

    private static PangrovaErrorCode toErrorCode(ErrorCategory category) {
        return switch (category) {
            case NETWORK -> PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
            case WORKER_BUSY, CONTROL_FENCED -> PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED;
            case WORKER_FATAL -> PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
            case RPC_TIMEOUT -> PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
        };
    }
}
