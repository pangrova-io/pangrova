package org.pangrova.scheduler.api.speculation;

import org.pangrova.common.api.annotation.Experimental;

/** Retries cancellation of a speculative attempt against its physical Worker. */
@Experimental
@FunctionalInterface
public interface SpeculativeAttemptCanceller {

    void cancelWithRetry(String queryId, String taskId, String workerHost, int httpPort);
}
