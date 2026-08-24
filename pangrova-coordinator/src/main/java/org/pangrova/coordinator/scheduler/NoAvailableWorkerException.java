package org.pangrova.coordinator.scheduler;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when no active worker is available for task dispatch.
 */
public class NoAvailableWorkerException extends PangrovaException {

    public NoAvailableWorkerException() {
        super(PangrovaErrorCode.COORDINATOR_NO_AVAILABLE_WORKER, "No available worker for task dispatch");
    }

    public NoAvailableWorkerException(String message) {
        super(PangrovaErrorCode.COORDINATOR_NO_AVAILABLE_WORKER, message);
    }
}
