package org.pangrova.coordinator.exception;

/**
 * Thrown when a task status report arrives from a Worker that is not registered
 * with the Coordinator's WorkerManager. Signals the HTTP layer to return 409 Conflict.
 */
public class UnregisteredWorkerException extends RuntimeException {

    private final String workerId;

    public UnregisteredWorkerException(String workerId) {
        super("Status report rejected: worker not registered: " + workerId);
        this.workerId = workerId;
    }

    public String workerId() {
        return workerId;
    }
}
