package org.pangrova.worker.execution;

import org.pangrova.spi.execution.BatchResult;
import org.pangrova.spi.execution.TaskExecutor;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit-test-only TaskExecutor SPI implementation for testing without native library.
 *
 * <p>Simulates the execution loop: createTask allocates a handle,
 * startTask marks it active, getNextBatchResult returns configurable batches
 * then EOF, releaseTask frees the handle. NOT a mock — this is a real
 * implementation of the SPI contract for in-JVM testing.</p>
 *
 * <p><b>Restriction:</b> This class is restricted to {@code pangrova-worker} unit tests only.
 * E2E tests (in {@code pangrova-testing/pangrova-test-e2e}) MUST use {@code NativeTaskExecutor}
 * with the real native engine. See Requirement 38.6.</p>
 */
public final class SimulatedTaskExecutor implements TaskExecutor {

    private static final int EOF = 1;

    private final AtomicLong handleCounter = new AtomicLong(1);
    private final Map<Long, TaskState> tasks = new ConcurrentHashMap<>();
    private final int batchesBeforeEof;
    private final int errorOnBatch;
    private final int notReadyBeforeData;

    /**
     * Creates a simulated executor that produces N batches then EOF.
     *
     * @param batchesBeforeEof number of DATA_AVAILABLE batches before EOF
     */
    public SimulatedTaskExecutor(int batchesBeforeEof) {
        this(batchesBeforeEof, -1, 0);
    }

    /**
     * Creates a simulated executor that returns error on a specific batch.
     *
     * @param batchesBeforeEof number of batches before EOF (if no error)
     * @param errorOnBatch     batch index to return error (-1 for no error)
     */
    public SimulatedTaskExecutor(int batchesBeforeEof, int errorOnBatch) {
        this(batchesBeforeEof, errorOnBatch, 0);
    }

    /**
     * Creates a simulated executor with configurable NOT_READY cycles.
     *
     * @param batchesBeforeEof   number of DATA_AVAILABLE batches before EOF
     * @param errorOnBatch       batch index to return error (-1 for no error)
     * @param notReadyBeforeData number of NOT_READY (null) returns before first data batch
     */
    public SimulatedTaskExecutor(int batchesBeforeEof, int errorOnBatch, int notReadyBeforeData) {
        this.batchesBeforeEof = batchesBeforeEof;
        this.errorOnBatch = errorOnBatch;
        this.notReadyBeforeData = notReadyBeforeData;
    }

    @Override
    public long createTask(byte[] taskRequestBytes) {
        long handle = handleCounter.getAndIncrement();
        try {
            var request = org.pangrova.protocol.v2.TaskRequest.parseFrom(taskRequestBytes);
            if (!request.hasAttempt()) {
                throw new IllegalArgumentException("simulated task requires attempt identity");
            }
            tasks.put(handle, new TaskState(request));
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid canonical simulated task request", e);
        }
        return handle;
    }

    @Override
    public void startTask(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        state.startedAtNanos = System.nanoTime();
        state.started = true;
    }

    @Override
    public BatchResult getNextBatchResult(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        long poll = state.batchCount.getAndIncrement();
        // NOT_READY phase
        if (poll < notReadyBeforeData) {
            return BatchResult.notReady();
        }
        long dataPoll = poll - notReadyBeforeData;
        if (errorOnBatch >= 0 && dataPoll >= errorOnBatch) {
            return BatchResult.error("Simulated batch error at batch " + dataPoll);
        }
        // DATA phase
        if (dataPoll < batchesBeforeEof) {
            return BatchResult.data(new byte[]{0x01});
        }
        return BatchResult.eof();
    }

    @Override
    public BatchResult getShuffleBatchResult(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        long poll = state.shuffleBatchCount.getAndIncrement();
        // NOT_READY phase for shuffle
        if (poll < notReadyBeforeData) {
            return BatchResult.notReady();
        }
        return BatchResult.eof();
    }

    @Override
    public void releaseTask(long executorHandle) {
        TaskState state = tasks.remove(executorHandle);
        if (state != null) {
            state.released = true;
        }
    }

    @Override
    public void cancelTask(long executorHandle) {
        TaskState state = tasks.get(executorHandle);
        if (state != null) {
            state.cancelled = true;
        }
    }

    @Override
    public boolean isFinished(long executorHandle) {
        TaskState state = tasks.get(executorHandle);
        if (state == null || state.cancelled) {
            return true;
        }
        if (state.started && !state.finishSignaled) {
            state.finishSignaled = true;
            if (errorOnBatch >= 0) {
                state.errorMessage = "Simulated batch error at batch " + errorOnBatch;
            }
        }
        return state.finishSignaled;
    }

    @Override
    public byte[] getTaskStatus(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        boolean failed = state.errorMessage != null;
        boolean finished = state.finishSignaled && !failed;
        long expected = Math.max(1, state.request.getSplitsCount());
        var coverage = org.pangrova.protocol.v2.CoverageReport.newBuilder()
                .setCompletionState(finished
                        ? org.pangrova.protocol.v2.CompletionState.COMPLETION_STATE_COMPLETE
                        : failed
                        ? org.pangrova.protocol.v2.CompletionState.COMPLETION_STATE_PARTIAL
                        : org.pangrova.protocol.v2.CompletionState.COMPLETION_STATE_UNSPECIFIED)
                .setExpectedWorkUnits(expected)
                .setCompletedWorkUnits(finished ? expected : 0)
                .setCoverageRatio(finished ? 1.0 : 0.0);
        if (failed) {
            if (state.request.getSplitsCount() == 0) {
                coverage.addFailedWorkUnitIds(state.attempt.getTaskId());
            } else {
                state.request.getSplitsList().forEach(
                        split -> coverage.addFailedWorkUnitIds(split.getSplitId()));
            }
        }
        Map<String, Long> sourceCounts = new LinkedHashMap<>();
        state.request.getSplitsList().forEach(split -> {
            String key = split.getSourceId() + '\0' + split.getSourceSnapshot();
            sourceCounts.merge(key, 1L, Math::addExact);
        });
        sourceCounts.forEach((key, count) -> {
            int separator = key.indexOf('\0');
            coverage.addSourceSnapshots(
                    org.pangrova.protocol.v2.SourceSnapshotReport.newBuilder()
                            .setSourceId(key.substring(0, separator))
                            .setSnapshotId(key.substring(separator + 1))
                            .setExpectedWorkUnits(count)
                            .setCompletedWorkUnits(finished ? count : 0));
        });
        var report = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(state.attempt)
                .setState(failed
                        ? org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                        : finished
                        ? org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                        : org.pangrova.protocol.v2.TaskState.TASK_STATE_RUNNING)
                .setCoverage(coverage);
        if (failed) {
            report.setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                    .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_INTERNAL)
                    .setCode("SIMULATED_TASK_FAILED")
                    .setMessage(state.errorMessage)
                    .setFailedComponent("simulated-executor"));
        }
        return report.build().toByteArray();
    }

    @Override
    public byte[] getTaskStats(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        if (!state.started || !state.finishSignaled) {
            throw new IllegalStateException("Simulated task statistics require terminal execution");
        }
        long elapsedMs = Math.max(1L,
                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - state.startedAtNanos));
        return org.pangrova.protocol.v2.NativeTaskStats.newBuilder()
                .setElapsedTimeMs(elapsedMs)
                .build().toByteArray();
    }

    @Override
    public byte[] executeCommand(long executorHandle, byte[] commandProto) {
        // Simulate command execution — return a minimal success response.
        // NativeCommandResponse: field 1 (success) = true → tag 0x08, value 0x01
        return new byte[]{0x08, 0x01};
    }

    /** Returns the error message for a finished task, or null if no error. */
    public String getErrorMessage(long handle) {
        TaskState state = tasks.get(handle);
        return state != null ? state.errorMessage : null;
    }


    /** Returns true if the handle was released. */
    public boolean isReleased(long handle) {
        return !tasks.containsKey(handle);
    }

    /** Returns the number of active (unreleased) tasks. */
    public int activeTaskCount() {
        return tasks.size();
    }

    private TaskState requireTask(long handle) {
        TaskState state = tasks.get(handle);
        if (state == null) {
            throw new IllegalStateException("Unknown handle: " + handle);
        }
        return state;
    }

    private static final class TaskState {
        final org.pangrova.protocol.v2.TaskAttemptId attempt;
        final org.pangrova.protocol.v2.TaskRequest request;
        volatile boolean started;
        volatile long startedAtNanos;
        volatile boolean released;
        volatile boolean cancelled;
        volatile boolean finishSignaled;
        volatile String errorMessage;
        final AtomicLong batchCount = new AtomicLong(0);
        final AtomicLong shuffleBatchCount = new AtomicLong(0);

        private TaskState(org.pangrova.protocol.v2.TaskRequest request) {
            this.request = request;
            this.attempt = request.getAttempt();
        }
    }
}
