package org.pangrova.scheduler.speculation;

/** Retries cancellation of a speculative attempt against its physical Worker. */
@FunctionalInterface
public interface SpeculativeAttemptCanceller {

    void cancelWithRetry(String queryId, String taskId, String workerHost, int httpPort);
}
