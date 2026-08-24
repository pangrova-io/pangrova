package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

import java.util.concurrent.CompletableFuture;

/**
 * SPI for asynchronous query submission with decoupled lifecycle phases.
 *
 * <h3>Problem this solves (G-SCHED-01)</h3>
 *
 * <p>The current {@link QueryLifecycle#createQuery(SessionContext, String)} and
 * the coordinator's {@code QueryManager.submitQuery()} are synchronous: the
 * HTTP request thread blocks through the entire query lifecycle (parse,
 * plan, schedule, dispatch, execute, collect results). This means:</p>
 * <ul>
 *   <li>Concurrency is capped by the request thread pool size</li>
 *   <li>Submission timeout and execution timeout are conflated</li>
 *   <li>No early return of {@code state=RUNNING} before results are ready</li>
 *   <li>No streaming result path (worker-redirect)</li>
 * </ul>
 *
 * <h3>Design</h3>
 *
 * <p>This interface decouples query submission into three phases:</p>
 * <ol>
 *   <li><b>Submit</b> ({@link #submitAsync}): accepts the query, assigns a
 *       query ID, and returns immediately with a future that resolves once the
 *       query is <em>scheduled</em> (not finished). The HTTP handler can return
 *       {@code state=RUNNING} and a query ID without blocking.</li>
 *   <li><b>Poll</b> ({@link #getQueryProgress}): clients poll for state
 *       transitions and result endpoint availability.</li>
 *   <li><b>Stream</b> ({@link #streamResults}): clients open a streaming result
 *       iterator that pulls batches from the output worker (worker-redirect
 *       path) without routing data through the coordinator.</li>
 * </ol>
 *
 * <h3>Industry precedent</h3>
 *
 * <ul>
 *   <li><b>Trino:</b> {@code POST /v1/statement} returns immediately with a
 *       {@code nextUri}; the client polls {@code nextUri} to retrieve pages
 *       of results. Coordinator never buffers full result sets.</li>
 *   <li><b>Spark Thrift Server:</b> {@code ExecuteStatement} is async;
 *       {@code GetOperationStatus} polls; {@code FetchResults} streams.</li>
 *   <li><b>Snowflake:</b> async execution with {@code resultUrl} redirect
 *       to cloud storage; no coordinator data routing.</li>
 * </ul>
 *
 * @see QueryLifecycle
 * @see QuerySubmissionResult
 * @see StreamingResultIterator
 */
@Experimental(reason = "G-SCHED-01: async query submission to decouple submit from execution")
public interface AsyncQuerySubmission {

    /**
     * Submits a query for asynchronous execution.
     *
     * <p>The returned future completes when the query has been accepted and
     * scheduled (transitioned to {@link QueryState#RUNNING} or
     * {@link QueryState#QUEUED}). It does <em>not</em> wait for query
     * completion.</p>
     *
     * <p>The future completes exceptionally if:</p>
     * <ul>
     *   <li>The query is syntactically invalid (parse error)</li>
     *   <li>Admission control rejects the query (quota, circuit breaker)</li>
     *   <li>Planning fails (semantic error, missing table, etc.)</li>
     * </ul>
     *
     * <p>For queries that pass validation and scheduling, the
     * {@link QuerySubmissionResult} contains the query ID, initial state,
     * and an optional result endpoint for worker-redirect streaming.</p>
     *
     * @param session the session context providing user identity and properties
     * @param sql     the SQL statement to execute
     * @return a future that resolves to the submission result once scheduled
     * @throws NullPointerException if session or sql is null
     */
    CompletableFuture<QuerySubmissionResult> submitAsync(SessionContext session, String sql);

    /**
     * Returns the current progress of a submitted query.
     *
     * <p>This is a lightweight, non-blocking call suitable for client polling.
     * It returns the current state, row count (if available), and result
     * endpoint (once the output stage is dispatched).</p>
     *
     * @param queryId the query identifier from {@link QuerySubmissionResult}
     * @return the current progress, or empty if the query ID is unknown
     * @throws NullPointerException if queryId is null
     */
    java.util.Optional<QueryProgress> getQueryProgress(QueryId queryId);

    /**
     * Opens a streaming result iterator for a completed or running query.
     *
     * <p>The iterator pulls result batches from the output worker directly
     * (worker-redirect path), bypassing the coordinator data plane. This
     * allows the coordinator to remain a pure control-plane node.</p>
     *
     * <p>The caller must close the returned iterator to release transport
     * resources.</p>
     *
     * @param queryId the query identifier from {@link QuerySubmissionResult}
     * @return a streaming result iterator
     * @throws IllegalStateException if the query has not yet reached
     *                               a state where results are available
     * @throws NullPointerException  if queryId is null
     */
    StreamingResultIterator streamResults(QueryId queryId);

    /**
     * Requests cancellation of a running query.
     *
     * <p>Returns a future that completes when the cancellation has been
     * acknowledged. The future resolves to {@code true} if the query was
     * successfully cancelled, {@code false} if it had already reached a
     * terminal state.</p>
     *
     * @param queryId the query identifier
     * @return a future resolving to the cancellation outcome
     * @throws NullPointerException if queryId is null
     */
    CompletableFuture<Boolean> cancelAsync(QueryId queryId);
}
