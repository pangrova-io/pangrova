package org.pangrova.coordinator.columnar;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Streams query results to Flight listeners, polling the result buffer on a
 * scheduled executor instead of blocking threads.
 */
final class ColumnarResultStreamer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnarResultStreamer.class);
    private static final long POLL_TIMEOUT_MS = 5_000;
    private static final long POLL_INTERVAL_MS = 50;

    private final QueryManager queryManager;
    private final CoordinatorResultBuffer resultBuffer;
    private final BufferAllocator allocator;
    private final ScheduledExecutorService pollExecutor;

    ColumnarResultStreamer(QueryManager queryManager,
                           CoordinatorResultBuffer resultBuffer,
                           BufferAllocator allocator) {
        this.queryManager = queryManager;
        this.resultBuffer = resultBuffer;
        this.allocator = allocator;
        this.pollExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "columnar-result-poll");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Streams the results of the given query to the listener, or an error status. */
    void streamResultsForQuery(String queryId, FlightSqlProducer.ServerStreamListener listener) {
        Optional<QueryStateMachine.State> stateOpt = queryManager.getQueryState(queryId);
        if (stateOpt.isEmpty()) {
            listener.error(CallStatus.NOT_FOUND
                    .withDescription("Query not found: " + queryId)
                    .toRuntimeException());
            return;
        }

        QueryStateMachine.State state = stateOpt.get();
        if (isTerminalFailure(state)) {
            listener.error(CallStatus.INTERNAL
                    .withDescription("Query failed or cancelled: " + queryId)
                    .toRuntimeException());
            return;
        }

        byte[] resultBytes = pollForResult(queryId);
        if (resultBytes == null) {
            listener.error(CallStatus.UNAVAILABLE
                    .withDescription("Query results not yet available: " + queryId)
                    .toRuntimeException());
            return;
        }

        try {
            streamBatchToListener(resultBytes, listener);
        } catch (IOException e) {
            LOG.error("event=result_stream_error queryId={}", queryId, e);
            listener.error(CallStatus.INTERNAL
                    .withDescription("Failed to stream results: " + e.getMessage())
                    .toRuntimeException());
        }
    }

    @Override
    public void close() {
        pollExecutor.shutdownNow();
    }

    private void streamBatchToListener(byte[] ipcBytes, FlightSqlProducer.ServerStreamListener listener)
            throws IOException {
        try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new ByteArrayInputStream(ipcBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            listener.start(root);
            while (reader.loadNextBatch()) {
                listener.putNext();
            }
            listener.completed();
        }
    }

    private byte[] pollForResult(String queryId) {
        byte[] immediate = resultBuffer.get(queryId);
        if (immediate != null) {
            return immediate;
        }

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ScheduledFuture<?> pollTask = pollExecutor.scheduleWithFixedDelay(
                () -> checkForResult(queryId, future),
                POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        try {
            return future.get(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (TimeoutException e) {
            return resultBuffer.get(queryId);
        } catch (ExecutionException e) {
            LOG.error("event=result_poll_error queryId={}", queryId, e);
            return null;
        } finally {
            pollTask.cancel(false);
        }
    }

    private void checkForResult(String queryId, CompletableFuture<byte[]> future) {
        if (future.isDone()) {
            return;
        }
        byte[] result = resultBuffer.get(queryId);
        if (result != null) {
            future.complete(result);
            return;
        }
        Optional<QueryStateMachine.State> state = queryManager.getQueryState(queryId);
        if (state.isPresent() && isTerminalFailure(state.get())) {
            future.complete(null);
        }
    }

    private static boolean isTerminalFailure(QueryStateMachine.State state) {
        return state == QueryStateMachine.State.FAILED
                || state == QueryStateMachine.State.CANCELLED;
    }
}
