package org.pangrova.coordinator.scheduler;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception thrown when task dispatch to a Worker fails.
 * Carries an {@link ErrorCategory} for structured routing of failure handling.
 *
 * <p>Circuit breaker logic uses the category to decide whether to count the failure:
 * only {@link ErrorCategory#WORKER_FATAL} increments the breaker counter.</p>
 */
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
            case WORKER_BUSY -> PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED;
            case WORKER_FATAL -> PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
            case RPC_TIMEOUT -> PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED;
        };
    }
}
