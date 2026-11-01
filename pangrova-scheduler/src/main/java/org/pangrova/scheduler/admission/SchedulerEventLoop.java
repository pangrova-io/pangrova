package org.pangrova.scheduler.admission;

import java.util.concurrent.CompletableFuture;

/**
 * Event-driven scheduler contract for coordinator query lifecycle management.
 *
 * <p>G-SCHED-08: This interface models the target event-driven scheduling
 * architecture. Instead of one-thread-per-query busy-polling to track query
 * completion, implementations use {@link CompletableFuture} composition and
 * reactive callbacks from the DAG engine's stage lifecycle events.</p>
 *
 * <h2>Migration from polling to event-driven</h2>
 *
 * <p>The legacy pattern spawned a daemon thread per deferred query that polled
 * {@code isQueryComplete()} every 50ms for up to 30 seconds. This had O(N)
 * threading overhead where N = concurrent queries with deferred DAG completion.
 * The event-driven replacement registers a single {@link CompletableFuture}
 * callback per deferred query, completed by the DAG engine when all stages
 * finish — O(1) per query, no polling threads.</p>
 *
 * <h2>Design</h2>
 *
 * <p>Implementations receive stage lifecycle events (started, finished, failed)
 * and evaluate query-level completion. When all stages for a query are terminal,
 * the completion future is resolved, waking any registered callbacks.</p>
 *
 * <p>Key events:</p>
 * <ul>
 *   <li>{@link #onStageCompleted} — a stage finished successfully</li>
 *   <li>{@link #onStageFailed} — a stage failed (may trigger query failure)</li>
 *   <li>{@link #whenQueryComplete} — returns a future for query completion</li>
 * </ul>
 *
 * @see org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine
 * @see org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback
 */
public interface SchedulerEventLoop {

    /**
     * Called when a stage completes successfully. The implementation should
     * evaluate whether all stages for the query are now terminal and, if so,
     * resolve the query completion future.
     *
     * @param queryId the owning query
     * @param stageId the completed stage
     */
    void onStageCompleted(String queryId, int stageId);

    /**
     * Called when a stage fails. The implementation should propagate the failure
     * to downstream stages and resolve the query completion future with failure.
     *
     * @param queryId the owning query
     * @param stageId the failed stage
     * @param reason  human-readable failure reason
     */
    void onStageFailed(String queryId, int stageId, String reason);

    /**
     * Returns a future that completes when all stages for the given query reach
     * a terminal state. Completes with {@code true} on success, {@code false}
     * on failure or cancellation.
     *
     * <p>This is the primary mechanism for event-driven query completion
     * notification. Callers attach callbacks via {@code thenAccept},
     * {@code thenRun}, etc. instead of polling.</p>
     *
     * @param queryId the query to watch
     * @return completion future
     */
    CompletableFuture<Boolean> whenQueryComplete(String queryId);

    /**
     * Cleans up any tracked state for the given query. Called on query terminal
     * (success, failure, or cancellation) to prevent memory leaks.
     *
     * @param queryId the query to clean up
     */
    void cleanup(String queryId);
}
