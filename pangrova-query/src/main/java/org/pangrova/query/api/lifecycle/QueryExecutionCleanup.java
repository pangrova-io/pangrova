package org.pangrova.query.api.lifecycle;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Execution owner's terminal cleanup boundary, borrowed by the query lifecycle owner.
 * Owns no query state transitions, journal, audit or retention decisions. Callers must
 * preserve the issued query identity; a retry must not target a replacement lifetime.
 *
 * <p>Logical termination and cancellation are not physical drain. Implementations must
 * retain live operation, task, connector and shuffle resources until their actual
 * completion is established. Partial cleanup may fail and must be retryable without
 * releasing another execution owner. Failures propagate to the lifecycle owner's
 * retry mechanism; no persistence fallback or synthetic drain is permitted.</p>
 *
 * <p>Calls for different queries may run concurrently. The lifecycle owner serializes
 * terminal cleanup for each query. Duplicate cancellation and cleanup requests must
 * be safe; successful cleanup does not itself retire the query's diagnostic state.</p>
 */
@Experimental
@ThreadSafe
public interface QueryExecutionCleanup {
    /**
     * Closes dispatch admission and requests cancellation of outstanding issued tasks.
     * Return does not establish task/operation drain or release execution ownership.
     */
    void cancelDispatchedTasks(String queryId);

    /**
     * Cleans an execution-owned terminal query after actual drain, including connector
     * scopes and required Shuffle cleanup. The caller still owns the execution grant.
     * Returns false while local operations, report publication or exact task drain
     * receipts are outstanding. Physical progress signals another cleanup attempt.
     * Cleanup failures propagate to the caller.
     */
    boolean tryCleanupQuery(String queryId);

    /**
     * Cleans only the local creation scope of a query that did not acquire execution
     * ownership. Must not cancel or release a foreign execution owner or its resources.
     * Returns false while local operations or issued task evidence are outstanding.
     */
    boolean tryCleanupUnadmittedQuery(String queryId);
}
