package org.pangrova.worker.write;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.data.ConnectorPageSink;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Owns one connector sink until its synchronous cleanup has actually returned. */
@ThreadSafe
final class NativeWriteSession {

    private ConnectorPageSink sink;
    private final String taskId;
    private final List<String> columnNames;
    private final String catalogName;
    private final boolean physicalColumns;
    private final long startTimeNanos = System.nanoTime();
    private final AtomicLong rowsAppended = new AtomicLong();
    private final AtomicLong batchCount = new AtomicLong();
    private boolean stopped;
    private boolean finished;
    private boolean aborted;
    private boolean closed;
    private boolean operationRunning = true;
    private byte[] metadata;

    NativeWriteSession(
            String taskId, List<String> columnNames, String catalogName, boolean physicalColumns) {
        this.taskId = taskId;
        this.columnNames = List.copyOf(columnNames);
        this.catalogName = catalogName;
        this.physicalColumns = physicalColumns;
    }

    /**
     * The session is published before opening a sink, so failed initialization still has an owner.
     */
    void initialize(Supplier<ConnectorPageSink> factory) {
        try {
            sink = Objects.requireNonNull(factory.get(), "sink");
        } finally {
            endOperation();
        }
    }

    ConnectorPageSink sink() {
        return sink;
    }

    String taskId() {
        return taskId;
    }

    List<String> columnNames() {
        return columnNames;
    }

    String catalogName() {
        return catalogName;
    }

    boolean physicalColumns() {
        return physicalColumns;
    }

    long startTimeNanos() {
        return startTimeNanos;
    }

    AtomicLong rowsAppended() {
        return rowsAppended;
    }

    AtomicLong batchCount() {
        return batchCount;
    }

    synchronized void beginAppend() {
        if (stopped) {
            throw new IllegalStateException("Write session has stopped accepting batches");
        }
        claimOperation();
    }

    synchronized void stop() {
        stopped = true;
    }

    synchronized void endOperation() {
        operationRunning = false;
    }

    private void claimOperation() {
        if (operationRunning) {
            throw new IllegalStateException("Write session operation is still in progress");
        }
        operationRunning = true;
    }

    private synchronized void beginFinish() {
        if (operationRunning) {
            throw new IllegalStateException("Write session operation is still in progress");
        }
        if (stopped && !finished) {
            throw new IllegalStateException("Write session cannot finish after failure or abort");
        }
        stopped = true;
        operationRunning = true;
    }

    private synchronized void beginAbort() {
        stopped = true;
        claimOperation();
    }

    /** A close retry must never repeat finish or erase the already collected fragment. */
    byte[] finish() {
        beginFinish();
        try {
            if (!finished) {
                byte[] collected = sink.finishWithMetadata();
                // Actual finish transfers prepared files even if metadata validation fails.
                finished = true;
                metadata = Objects.requireNonNull(collected, "Connector commit metadata").clone();
            }
            closeSink();
            return metadata.clone();
        } finally {
            endOperation();
        }
    }

    /** Failure keeps this owner and its precise completed cleanup steps for retry. */
    void abort() {
        beginAbort();
        try {
            Throwable failure = null;
            try {
                if (sink != null && !finished && !aborted) {
                    sink.abort();
                    aborted = true;
                }
            } catch (RuntimeException | Error abortFailure) {
                failure = abortFailure;
            }
            // Release resources even after failed discard, but retain this
            // owner until both phases have actually completed successfully.
            try {
                closeSink();
            } catch (RuntimeException | Error closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else if (failure != closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
        } finally {
            endOperation();
        }
    }

    private void closeSink() {
        if (sink != null && !closed) {
            sink.close();
            closed = true;
        }
    }
}
