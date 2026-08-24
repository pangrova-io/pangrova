package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

import java.util.concurrent.CompletableFuture;

/**
 * Streaming iterator for query result batches.
 *
 * <h3>Problem this solves (G-SCHED-01)</h3>
 *
 * <p>The current query result flow buffers the entire result set in the
 * coordinator before returning to the client. This has two consequences:</p>
 * <ul>
 *   <li>The coordinator becomes a data-plane bottleneck, violating the
 *       read-write separation architecture.</li>
 *   <li>Large result sets exhaust coordinator memory.</li>
 * </ul>
 *
 * <h3>Design</h3>
 *
 * <p>This iterator provides a pull-based streaming interface where each
 * call to {@link #nextBatch()} returns the next batch of results. Batches
 * are pulled from the output worker directly (worker-redirect path),
 * bypassing the coordinator data plane.</p>
 *
 * <p>The iterator supports both blocking ({@link #nextBatch()}) and
 * non-blocking ({@link #nextBatchAsync()}) consumption patterns.</p>
 *
 * <h3>Lifecycle</h3>
 *
 * <p>A {@code StreamingResultIterator} is created by
 * {@link AsyncQuerySubmission#streamResults(QueryId)} and must be closed
 * by the caller when done (or on error) to release transport resources.</p>
 *
 * <pre>
 *   try (var it = asyncSubmission.streamResults(queryId)) {
 *       while (it.hasMoreBatches()) {
 *           ResultBatch batch = it.nextBatch();
 *           // process batch
 *       }
 *   }
 * </pre>
 *
 * <h3>Industry precedent</h3>
 *
 * <ul>
 *   <li><b>Trino:</b> paged result model -- client polls {@code nextUri}
 *       for each page; coordinator returns a bounded batch and a link
 *       to the next page.</li>
 *   <li><b>Arrow Flight:</b> {@code FlightStream} -- streaming gRPC
 *       iterator; each call to {@code next()} returns the next
 *       {@code RecordBatch}.</li>
 *   <li><b>Spark:</b> {@code FetchResults} with offset/orientation for
 *       incremental retrieval from the Thrift server.</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 *
 * <p>Implementations must be safe for single-threaded sequential access.
 * Concurrent calls from multiple threads are not supported.</p>
 *
 * @see AsyncQuerySubmission#streamResults(QueryId)
 * @see ResultBatch
 */
@Experimental(reason = "G-SCHED-01: streaming result iterator for async query execution")
public interface StreamingResultIterator extends AutoCloseable {

    /**
     * Returns whether more batches may be available.
     *
     * <p>Returns {@code true} if the stream has not yet reached EOF or
     * error. This method may block briefly to determine availability.</p>
     *
     * @return true if {@link #nextBatch()} may return more data
     * @throws IllegalStateException if the iterator is closed
     */
    boolean hasMoreBatches();

    /**
     * Returns the next result batch, blocking until one is available.
     *
     * <p>Returns a {@link ResultBatch.Data} for each batch of results,
     * a {@link ResultBatch.End} when the stream is exhausted, or a
     * {@link ResultBatch.Error} if streaming failed.</p>
     *
     * @return the next result batch, never null
     * @throws IllegalStateException if the iterator is closed
     */
    ResultBatch nextBatch();

    /**
     * Returns a future that completes with the next result batch.
     *
     * <p>This is the non-blocking counterpart of {@link #nextBatch()}.
     * The future completes on the transport's I/O thread; callers
     * should avoid blocking operations in downstream processing.</p>
     *
     * @return a future resolving to the next batch
     * @throws IllegalStateException if the iterator is closed
     */
    CompletableFuture<ResultBatch> nextBatchAsync();

    /**
     * Returns the query ID this iterator is streaming results for.
     *
     * @return the query identifier, never null
     */
    QueryId queryId();

    /**
     * Returns the total number of batches delivered so far.
     *
     * <p>Useful for progress tracking and diagnostics.</p>
     *
     * @return number of DATA batches returned by {@link #nextBatch()}
     */
    long batchesDelivered();

    /**
     * Returns the total number of rows delivered so far.
     *
     * <p>May return -1 if row counting is not supported by the
     * transport layer.</p>
     *
     * @return total rows delivered, or -1 if unknown
     */
    long rowsDelivered();

    /**
     * Releases all transport resources associated with this iterator.
     *
     * <p>After close, all methods throw {@link IllegalStateException}.
     * Idempotent -- calling close on an already-closed iterator is a no-op.</p>
     */
    @Override
    void close();
}
