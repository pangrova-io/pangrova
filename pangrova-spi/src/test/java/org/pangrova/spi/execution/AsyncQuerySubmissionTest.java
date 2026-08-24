package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Unit tests for the {@link AsyncQuerySubmission} and related SPI contracts.
 * Uses a stub implementation to verify the interface contract.
 */
class AsyncQuerySubmissionTest {

    // ── Stub implementations ──

    static class StubAsyncQuerySubmission implements AsyncQuerySubmission {

        private final Map<String, QueryEntry> queries = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<QuerySubmissionResult> submitAsync(
                SessionContext session, String sql) {
            if (session == null || sql == null) {
                throw new NullPointerException("session and sql must not be null");
            }
            String id = "q-" + queries.size();
            QueryId queryId = new QueryId(id);
            QuerySubmissionResult result = new QuerySubmissionResult(
                    queryId, QueryState.QUEUED, null);
            queries.put(id, new QueryEntry(queryId, QueryState.QUEUED, sql));
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public Optional<QueryProgress> getQueryProgress(QueryId queryId) {
            if (queryId == null) {
                throw new NullPointerException("queryId must not be null");
            }
            QueryEntry entry = queries.get(queryId.id());
            if (entry == null) {
                return Optional.empty();
            }
            return Optional.of(QueryProgress.of(entry.queryId, entry.state));
        }

        @Override
        public StreamingResultIterator streamResults(QueryId queryId) {
            if (queryId == null) {
                throw new NullPointerException("queryId must not be null");
            }
            QueryEntry entry = queries.get(queryId.id());
            if (entry == null || entry.state == QueryState.QUEUED
                    || entry.state == QueryState.PLANNING) {
                throw new IllegalStateException("Results not available yet");
            }
            return new StubStreamingResultIterator(queryId);
        }

        @Override
        public CompletableFuture<Boolean> cancelAsync(QueryId queryId) {
            if (queryId == null) {
                throw new NullPointerException("queryId must not be null");
            }
            QueryEntry entry = queries.get(queryId.id());
            if (entry == null || entry.state == QueryState.FINISHED
                    || entry.state == QueryState.FAILED) {
                return CompletableFuture.completedFuture(false);
            }
            entry.state = QueryState.FAILED;
            return CompletableFuture.completedFuture(true);
        }

        void transitionTo(String id, QueryState newState) {
            QueryEntry entry = queries.get(id);
            if (entry != null) {
                entry.state = newState;
            }
        }
    }

    static class QueryEntry {
        final QueryId queryId;
        volatile QueryState state;
        final String sql;

        QueryEntry(QueryId queryId, QueryState state, String sql) {
            this.queryId = queryId;
            this.state = state;
            this.sql = sql;
        }
    }

    static class StubStreamingResultIterator implements StreamingResultIterator {

        private final QueryId queryId;
        private final List<ResultBatch> batches = new ArrayList<>();
        private int index = 0;
        private boolean closed = false;
        private long batchCount = 0;
        private long rowCount = 0;

        StubStreamingResultIterator(QueryId queryId) {
            this.queryId = queryId;
            // Provide two data batches and an end marker
            batches.add(new ResultBatch.Data(
                    ByteBuffer.wrap(new byte[]{1, 2, 3}), () -> {}));
            batches.add(new ResultBatch.Data(
                    ByteBuffer.wrap(new byte[]{4, 5, 6}), () -> {}));
            batches.add(new ResultBatch.End());
        }

        @Override
        public boolean hasMoreBatches() {
            checkNotClosed();
            return index < batches.size()
                    && !(batches.get(index) instanceof ResultBatch.End);
        }

        @Override
        public ResultBatch nextBatch() {
            checkNotClosed();
            if (index >= batches.size()) {
                return new ResultBatch.End();
            }
            ResultBatch batch = batches.get(index++);
            if (batch instanceof ResultBatch.Data) {
                batchCount++;
                rowCount += 1; // stub: 1 row per batch
            }
            return batch;
        }

        @Override
        public CompletableFuture<ResultBatch> nextBatchAsync() {
            checkNotClosed();
            return CompletableFuture.completedFuture(nextBatch());
        }

        @Override
        public QueryId queryId() {
            return queryId;
        }

        @Override
        public long batchesDelivered() {
            return batchCount;
        }

        @Override
        public long rowsDelivered() {
            return rowCount;
        }

        @Override
        public void close() {
            closed = true;
        }

        private void checkNotClosed() {
            if (closed) {
                throw new IllegalStateException("Iterator is closed");
            }
        }

        boolean isClosed() {
            return closed;
        }
    }

    static class StubSessionContext implements SessionContext {
        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of("test_catalog");
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of("test_schema");
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of();
        }
    }

    // ── AsyncQuerySubmission tests ──

    @Test
    void submitAsyncReturnsQueryId() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        assertThat(result.queryId()).isNotNull();
        assertThat(result.queryId().id()).isEqualTo("q-0");
        assertThat(result.initialState()).isEqualTo(QueryState.QUEUED);
    }

    @Test
    void submitAsyncDoesNotBlock() {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        CompletableFuture<QuerySubmissionResult> future =
                submission.submitAsync(new StubSessionContext(), "SELECT 1");

        // Future should be already done (stub completes synchronously)
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCompletedExceptionally()).isFalse();
    }

    @Test
    void submitAsyncRejectsNullSession() {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();

        assertThatThrownBy(() -> submission.submitAsync(null, "SELECT 1"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void submitAsyncRejectsNullSql() {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();

        assertThatThrownBy(() -> submission.submitAsync(
                new StubSessionContext(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void multipleSubmitsGetUniqueIds() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        SessionContext session = new StubSessionContext();

        QuerySubmissionResult r1 = submission.submitAsync(session, "SELECT 1").get();
        QuerySubmissionResult r2 = submission.submitAsync(session, "SELECT 2").get();

        assertThat(r1.queryId()).isNotEqualTo(r2.queryId());
    }

    // ── QueryProgress tests ──

    @Test
    void getProgressReturnsState() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        Optional<QueryProgress> progress =
                submission.getQueryProgress(result.queryId());

        assertThat(progress).isPresent();
        assertThat(progress.get().state()).isEqualTo(QueryState.QUEUED);
        assertThat(progress.get().isTerminal()).isFalse();
    }

    @Test
    void getProgressReturnsEmptyForUnknownQuery() {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();

        Optional<QueryProgress> progress =
                submission.getQueryProgress(new QueryId("unknown"));

        assertThat(progress).isEmpty();
    }

    @Test
    void progressReflectsStateTransitions() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);
        QueryProgress running = submission.getQueryProgress(result.queryId()).get();
        assertThat(running.state()).isEqualTo(QueryState.RUNNING);
        assertThat(running.isTerminal()).isFalse();

        submission.transitionTo(result.queryId().id(), QueryState.FINISHED);
        QueryProgress finished = submission.getQueryProgress(result.queryId()).get();
        assertThat(finished.state()).isEqualTo(QueryState.FINISHED);
        assertThat(finished.isTerminal()).isTrue();
    }

    // ── Cancel tests ──

    @Test
    void cancelRunningQuerySucceeds() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);
        boolean cancelled = submission.cancelAsync(result.queryId()).get();

        assertThat(cancelled).isTrue();
    }

    @Test
    void cancelTerminalQueryReturnsFalse() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        submission.transitionTo(result.queryId().id(), QueryState.FINISHED);
        boolean cancelled = submission.cancelAsync(result.queryId()).get();

        assertThat(cancelled).isFalse();
    }

    @Test
    void cancelUnknownQueryReturnsFalse() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        boolean cancelled = submission.cancelAsync(new QueryId("unknown")).get();
        assertThat(cancelled).isFalse();
    }

    // ── StreamingResultIterator tests ──

    @Test
    void streamResultsReturnsIterator() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        try (StreamingResultIterator it =
                     submission.streamResults(result.queryId())) {
            assertThat(it.queryId()).isEqualTo(result.queryId());
        }
    }

    @Test
    void streamResultsThrowsWhenNotReady() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();

        // Query is still QUEUED -- no results available
        assertThatThrownBy(() -> submission.streamResults(result.queryId()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void iteratorDeliversBatchesInOrder() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        try (StreamingResultIterator it =
                     submission.streamResults(result.queryId())) {
            assertThat(it.hasMoreBatches()).isTrue();

            ResultBatch b1 = it.nextBatch();
            assertThat(b1).isInstanceOf(ResultBatch.Data.class);

            ResultBatch b2 = it.nextBatch();
            assertThat(b2).isInstanceOf(ResultBatch.Data.class);

            ResultBatch end = it.nextBatch();
            assertThat(end).isInstanceOf(ResultBatch.End.class);
        }
    }

    @Test
    void iteratorTracksBatchCount() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        try (StreamingResultIterator it =
                     submission.streamResults(result.queryId())) {
            assertThat(it.batchesDelivered()).isEqualTo(0);
            it.nextBatch();
            assertThat(it.batchesDelivered()).isEqualTo(1);
            it.nextBatch();
            assertThat(it.batchesDelivered()).isEqualTo(2);
        }
    }

    @Test
    void iteratorTracksRowCount() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        try (StreamingResultIterator it =
                     submission.streamResults(result.queryId())) {
            assertThat(it.rowsDelivered()).isEqualTo(0);
            it.nextBatch();
            it.nextBatch();
            assertThat(it.rowsDelivered()).isEqualTo(2);
        }
    }

    @Test
    void iteratorAsyncMatchesSync() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        try (StreamingResultIterator it =
                     submission.streamResults(result.queryId())) {
            CompletableFuture<ResultBatch> asyncBatch = it.nextBatchAsync();
            assertThat(asyncBatch.isDone()).isTrue();
            assertThat(asyncBatch.get()).isInstanceOf(ResultBatch.Data.class);
        }
    }

    @Test
    void closedIteratorThrows() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        StreamingResultIterator it = submission.streamResults(result.queryId());
        it.close();

        assertThatThrownBy(it::hasMoreBatches)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(it::nextBatch)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(it::nextBatchAsync)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeIsIdempotent() throws Exception {
        StubAsyncQuerySubmission submission = new StubAsyncQuerySubmission();
        QuerySubmissionResult result = submission.submitAsync(
                new StubSessionContext(), "SELECT 1").get();
        submission.transitionTo(result.queryId().id(), QueryState.RUNNING);

        StubStreamingResultIterator it = (StubStreamingResultIterator)
                submission.streamResults(result.queryId());
        it.close();
        it.close(); // should not throw
        assertThat(it.isClosed()).isTrue();
    }

    @Test
    void iteratorIsAutoCloseable() {
        assertThat(AutoCloseable.class)
                .isAssignableFrom(StreamingResultIterator.class);
    }

    // ── QuerySubmissionResult tests ──

    @Test
    void submissionResultEndpointWhenPresent() {
        QuerySubmissionResult.ResultEndpointInfo endpoint =
                new QuerySubmissionResult.ResultEndpointInfo(
                        "worker-1", 9090, "ticket-123", "arrow-flight");
        QuerySubmissionResult result = new QuerySubmissionResult(
                new QueryId("q-1"), QueryState.RUNNING, endpoint);

        assertThat(result.endpoint()).isPresent();
        assertThat(result.endpoint().get().host()).isEqualTo("worker-1");
        assertThat(result.endpoint().get().port()).isEqualTo(9090);
        assertThat(result.endpoint().get().ticket()).isEqualTo("ticket-123");
        assertThat(result.endpoint().get().protocol()).isEqualTo("arrow-flight");
    }

    @Test
    void submissionResultEndpointWhenAbsent() {
        QuerySubmissionResult result = new QuerySubmissionResult(
                new QueryId("q-1"), QueryState.QUEUED, null);

        assertThat(result.endpoint()).isEmpty();
    }

    @Test
    void submissionResultRejectsNullQueryId() {
        assertThatThrownBy(() -> new QuerySubmissionResult(
                null, QueryState.QUEUED, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void submissionResultRejectsNullState() {
        assertThatThrownBy(() -> new QuerySubmissionResult(
                new QueryId("q-1"), null, null))
                .isInstanceOf(NullPointerException.class);
    }

    // ── QueryProgress tests ──

    @Test
    void progressOfFactoryCreatesMinimalSnapshot() {
        QueryProgress progress = QueryProgress.of(
                new QueryId("q-1"), QueryState.RUNNING);

        assertThat(progress.queryId().id()).isEqualTo("q-1");
        assertThat(progress.state()).isEqualTo(QueryState.RUNNING);
        assertThat(progress.processedRows()).isEmpty();
        assertThat(progress.processedBytes()).isEmpty();
        assertThat(progress.resultEndpoint()).isEmpty();
        assertThat(progress.failureMessage()).isEmpty();
        assertThat(progress.isTerminal()).isFalse();
    }

    @Test
    void progressFailedFactoryCreatesFailedSnapshot() {
        QueryProgress progress = QueryProgress.failed(
                new QueryId("q-1"), "OOM killed");

        assertThat(progress.state()).isEqualTo(QueryState.FAILED);
        assertThat(progress.failureMessage()).contains("OOM killed");
        assertThat(progress.isTerminal()).isTrue();
    }

    @Test
    void finishedStateIsTerminal() {
        QueryProgress progress = QueryProgress.of(
                new QueryId("q-1"), QueryState.FINISHED);
        assertThat(progress.isTerminal()).isTrue();
    }

    @Test
    void queuedStateIsNotTerminal() {
        QueryProgress progress = QueryProgress.of(
                new QueryId("q-1"), QueryState.QUEUED);
        assertThat(progress.isTerminal()).isFalse();
    }
}
