package org.pangrova.worker.task;

import org.pangrova.worker.task.state.TaskState;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.client.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Monitors task completion by polling native executor state and handles task lifecycle transitions
 * on completion or error.
 *
 * <p>Concurrency strategy: stateless -- all mutable state lives in TaskInfo. Thread safety
 * delegated to TaskManager's ConcurrentHashMap registry and per-task AtomicReference state.
 *
 * <p><b>Task 188 -- Thread pool separation:</b> The completion monitor runs on a dedicated
 * orchestration executor pool ({@code WorkerTaskBootstrap.createOrchestrationExecutor}) that is
 * intentionally separate from the native driver pool (folly executor). This separation prevents
 * morsel-driven scheduling deadlock: blocking poll-until-finished calls on driver pool threads
 * would starve morsel delivery under fan-out workloads. The orchestration pool uses bounded {@link
 * java.util.concurrent.ThreadPoolExecutor} with {@link
 * java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} as the saturation strategy -- if all
 * orchestration threads are busy, the submitting thread runs the monitor task inline rather than
 * deadlocking.
 */
@ThreadSafe
public final class TaskCompletionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskCompletionMonitor.class);

    private final TaskManagerContext deps;
    private final TaskStatusCollector statusCollector;

    /** Runtime quota enforcer for stage boundary checks during polling. */
    private volatile TenantExecutionQuotaEnforcer quotaEnforcer;

    public TaskCompletionMonitor(TaskManagerContext deps, TaskStatusCollector statusCollector) {
        this.deps = deps;
        this.statusCollector = statusCollector;
    }

    void setQuotaEnforcer(TenantExecutionQuotaEnforcer enforcer) {
        this.quotaEnforcer = enforcer;
    }

    /**
     * Monitors task completion by polling the native executor state. Used for autonomous stages
     * where no Java data threads are started.
     */
    void startCompletionMonitor(
            String taskId,
            TaskInfo info,
            long handle,
            Consumer<String> onCompleted,
            Consumer<Exception> onError) {
        deps.completionMonitorExecutor()
                .submit(
                        () -> {
                            Thread.currentThread().setName("completion-monitor-" + taskId);
                            boolean metadataCaptureAttempted = false;
                            try {
                                pollUntilFinished(taskId, info, handle);
                                metadataCaptureAttempted = true;
                                captureTerminalMetadata(info, handle);
                            } catch (InterruptedException e) {
                                // Normal shutdown signal — TaskManager.close() interrupts
                                // completion monitor threads. Not a task failure.
                                Thread.currentThread().interrupt();
                                LOG.debug(
                                        "event=completion_monitor_shutdown taskId={} queryId={}",
                                        taskId,
                                        info.queryId());
                                return;
                            } catch (Exception e) {
                                if (!metadataCaptureAttempted) {
                                    try {
                                        captureTerminalMetadata(info, handle);
                                    } catch (Exception metadataFailure) {
                                        e.addSuppressed(metadataFailure);
                                        LOG.error(
                                                "event=terminal_metadata_capture_failed taskId={} handle={} error={}",
                                                info.taskId(),
                                                handle,
                                                metadataFailure.getMessage(),
                                                metadataFailure);
                                    }
                                }
                                onError.accept(e);
                                return;
                            }
                            try {
                                onCompleted.accept(taskId);
                            } catch (RuntimeException | Error failure) {
                                // Execution already succeeded. The completion callback retains the
                                // real cleanup/publication retry.
                                LOG.error(
                                        "event=task_completion_publication_pending taskId={} queryId={} error={}",
                                        taskId,
                                        info.queryId(),
                                        failure.getMessage(),
                                        failure);
                                throw failure;
                            }
                        });
    }

    /**
     * Freezes canonical Native status and task statistics before resource release. A successful
     * execution fails closed if either envelope is absent or malformed. For an already-failed
     * execution, capture failure is attached to the original failure by the caller and remains
     * observable.
     */
    private void captureTerminalMetadata(TaskInfo info, long handle) {
        if (info.executorHandle() == 0) {
            var released =
                    requireCanonicalTaskReport(
                            info.request().taskRequest(), info.executorReleaseReport());
            if (released.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
                throw new IllegalStateException(
                        "Released Native task did not finish successfully: " + released.getState());
            }
            return;
        }
        byte[] status;
        try {
            status = deps.taskExecutor().getTaskStatus(handle);
        } catch (IllegalStateException failure) {
            if (info.executorHandle() != 0) {
                throw failure;
            }
            status = info.executorReleaseReport();
        }
        var parsed = requireCanonicalTaskReport(info.request().taskRequest(), status);
        info.setTerminalTaskReport(status);
        captureTaskStats(info, handle, parsed);
        if (parsed.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
            throw new IllegalStateException(
                    "Native task did not finish successfully: task="
                            + info.taskId()
                            + " state="
                            + parsed.getState());
        }
    }

    private void captureTaskStats(
            TaskInfo info, long handle, org.pangrova.protocol.v2.TaskReport report) {
        byte[] stats =
                report.hasNativeTaskStats() ? report.getNativeTaskStats().toByteArray() : null;
        if (stats == null) {
            try {
                stats = deps.taskExecutor().getTaskStats(handle);
            } catch (IllegalStateException failure) {
                if (info.executorHandle() != 0) {
                    throw failure;
                }
                var receipt =
                        requireCanonicalTaskReport(
                                info.request().taskRequest(), info.executorReleaseReport());
                if (receipt.hasNativeTaskStats()) {
                    stats = receipt.getNativeTaskStats().toByteArray();
                }
            }
        }
        if (stats == null || stats.length == 0) {
            throw new IllegalStateException(
                    "Native task completed without actual statistics: " + info.taskId());
        }
        try {
            var parsed = org.pangrova.protocol.v2.NativeTaskStats.parseFrom(stats);
            if (!parsed.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalStateException(
                        "Native task statistics contain unknown fields: " + info.taskId());
            }
            info.setTerminalTaskStats(stats);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException(
                    "Native task statistics are malformed: " + info.taskId(), failure);
        }
    }

    static org.pangrova.protocol.v2.TaskReport requireCanonicalTaskReport(
            org.pangrova.protocol.v2.TaskRequest request, byte[] status) {
        if (status == null || status.length == 0) {
            throw new IllegalStateException(
                    "Native task completed without a canonical TaskReport: "
                            + request.getAttempt().getTaskId());
        }
        org.pangrova.protocol.v2.TaskReport parsed;
        try {
            parsed = org.pangrova.protocol.v2.TaskReport.parseFrom(status);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalStateException(
                    "Native task returned a malformed canonical TaskReport: "
                            + request.getAttempt().getTaskId(),
                    malformed);
        }
        if (!parsed.getUnknownFields().asMap().isEmpty()
                || !parsed.getAttempt().equals(request.getAttempt())
                || parsed.getReportOrigin()
                        != org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                || (parsed.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                        && parsed.hasError())
                || (parsed.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                        && !parsed.hasError())
                || (parsed.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                        && parsed.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                        && parsed.getState()
                                != org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED)) {
            throw new IllegalStateException(
                    "Native canonical TaskReport violates its terminal task authority");
        }
        return parsed;
    }

    void onTaskCompleted(
            String taskId,
            TaskInfo info,
            TaskLifecycleManager.LifecycleCallback callback,
            Runnable releaseResources) {
        if (!callback.transition(taskId, TaskState.FINISHING)) {
            // Task may already be CANCELLING or FAILED.
            // Still release resources to prevent native handle / memory leaks.
            releaseThen(info, releaseResources, () -> reportCancelledAfterRelease(info, callback));
            return;
        }
        releaseThen(info, releaseResources, () -> finishCompletedTask(taskId, info, callback));
    }

    private void finishCompletedTask(
            String taskId, TaskInfo info, TaskLifecycleManager.LifecycleCallback callback) {
        if (info.getState() != TaskState.FINISHED
                && !callback.transition(taskId, TaskState.FINISHED)) {
            reportCancelledAfterRelease(info, callback);
            return;
        }
        // Retain the actual Native drain before a terminal HTTP report can wait for ACK.
        // The Coordinator may concurrently seal this exact attempt to cancel a losing copy.
        callback.clearCoordinatorIndex(info);
        reportStatus(info, "FINISHED");
        logTaskCompleted(taskId);
        callback.notifyCompletion(info.queryId());
    }

    void onTaskError(
            String taskId,
            TaskInfo info,
            Exception cause,
            TaskLifecycleManager.LifecycleCallback callback,
            Runnable releaseResources) {
        if (info.getState() == TaskState.CANCELLING) {
            releaseThen(info, releaseResources, () -> reportCancelledAfterRelease(info, callback));
            return;
        }
        boolean nativeUnsupported = isNativeUnsupportedFeature(cause);
        logTaskError(taskId, info, cause, nativeUnsupported);
        logTaskErrorToCollector(taskId, cause);
        releaseThen(
                info,
                releaseResources,
                () -> finishFailedTask(taskId, info, callback, cause, nativeUnsupported));
    }

    private void finishFailedTask(
            String taskId,
            TaskInfo info,
            TaskLifecycleManager.LifecycleCallback callback,
            Exception cause,
            boolean nativeUnsupported) {
        if (info.getState() == TaskState.FAILED
                || callback.transition(taskId, TaskState.FAILED)) {
            callback.clearCoordinatorIndex(info);
            reportErrorStatus(info, cause, nativeUnsupported);
        } else {
            reportCancelledAfterRelease(info, callback);
        }
    }

    private static void releaseThen(TaskInfo info, Runnable release, Runnable continuation) {
        try {
            release.run();
            continuation.run();
        } catch (RuntimeException | Error failure) {
            info.retainCleanupRetry(() -> releaseThen(info, release, continuation));
            throw failure;
        }
    }

    private void reportCancelledAfterRelease(
            TaskInfo info, TaskLifecycleManager.LifecycleCallback callback) {
        if (info.getState() != TaskState.CANCELLED
                && (info.getState() != TaskState.CANCELLING
                        || !callback.transition(info.taskId(), TaskState.CANCELLED))) {
            return;
        }
        callback.clearCoordinatorIndex(info);
        if (statusCollector != null && info.request() != null) {
            statusCollector.reportWorkerTerminalStatus(
                    info,
                    "CANCELLED",
                    new org.pangrova.client.error.ErrorDetail(
                            "TASK_CANCELLED", "Cancellation", "Task cancelled by control plane"));
        }
        info.markCancellationReported();
    }

    private static final int MAX_TRANSIENT_RETRIES = 3;

    /** Upper bound for the transient-failure retry backoff. */
    private static final long MAX_TRANSIENT_BACKOFF_MS = 8_000;

    /**
     * Poll interval for checking native task completion. Must be low (50ms) because the native
     * memory pool is only freed when releaseAllResources executes after isFinished returns true. A
     * long poll interval causes subsequent tasks to block in PangrovaArbitrator::growCapacity()
     * waiting for the previous task's pool to be removed.
     */
    private static final long INITIAL_BACKOFF_MS = 50;

    private void pollUntilFinished(String taskId, TaskInfo info, long handle) throws Exception {
        int transientRetries = 0;
        long backoffMs = INITIAL_BACKOFF_MS;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (isFinishedOrReleased(info, handle)) {
                    return;
                }
                // Stage boundary quota check: verify tenant hasn't exceeded runtime limits
                checkExecutionQuota(taskId);
                transientRetries = 0;
                backoffMs = INITIAL_BACKOFF_MS;
                sleepInterruptibly(INITIAL_BACKOFF_MS);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                transientRetries++;
                backoffMs = handlePollFailure(handle, e, transientRetries, backoffMs);
            }
        }
        throw new InterruptedException(
                "Task completion monitor interrupted before Native termination");
    }

    private boolean isFinishedOrReleased(TaskInfo info, long handle) {
        if (info.executorHandle() == 0) {
            requireCanonicalTaskReport(info.request().taskRequest(), info.executorReleaseReport());
            return true;
        }
        try {
            return deps.taskExecutor().isFinished(handle);
        } catch (IllegalStateException failure) {
            if (info.executorHandle() != 0) {
                throw failure;
            }
            requireCanonicalTaskReport(info.request().taskRequest(), info.executorReleaseReport());
            return true;
        }
    }

    private void checkExecutionQuota(String taskId) {
        TenantExecutionQuotaEnforcer enforcer = this.quotaEnforcer;
        if (enforcer != null) {
            enforcer.checkDuringExecution(taskId);
        }
    }

    /** Interruptible sleep that propagates interruption as a shutdown signal. */
    private static void sleepInterruptibly(long millis) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(millis);
    }

    /**
     * Reacts to a poll failure based on its classification. Returns the updated backoff for
     * transient failures; rethrows for all other categories.
     */
    private static long handlePollFailure(
            long handle, Exception e, int transientRetries, long backoffMs) throws Exception {
        NativeTaskFailureClassifier.Category category = NativeTaskFailureClassifier.classify(e);
        switch (category) {
            case NATIVE_CRASHED -> {
                LOG.error(
                        "event=native_crashed executorHandle={} error={}",
                        handle,
                        e.getMessage(),
                        e);
                throw e;
            }
            case NATIVE_TASK_FAILED -> {
                LOG.error(
                        "event=native_task_failed executorHandle={} error={}",
                        handle,
                        e.getMessage(),
                        e);
                throw e;
            }
            case NATIVE_HUNG -> {
                LOG.warn("event=native_hung executorHandle={} error={}", handle, e.getMessage());
                TimeoutException timeout =
                        new TimeoutException("Native executor unresponsive: " + e.getMessage());
                timeout.initCause(e);
                throw new RuntimeException(timeout);
            }
            case TRANSIENT -> {
                if (transientRetries > MAX_TRANSIENT_RETRIES) {
                    LOG.error(
                            "event=transient_retries_exhausted executorHandle={} "
                                    + "retries={} error={}",
                            handle,
                            transientRetries,
                            e.getMessage(),
                            e);
                    throw e;
                }
                LOG.warn(
                        "event=transient_failure executorHandle={} retry={} "
                                + "backoffMs={} error={}",
                        handle,
                        transientRetries,
                        backoffMs,
                        e.getMessage());
                sleepInterruptibly(backoffMs);
                return Math.min(backoffMs * 2, MAX_TRANSIENT_BACKOFF_MS);
            }
            default ->
                    throw new IllegalStateException(
                            "Unhandled native failure category: " + category);
        }
    }

    private void reportStatus(TaskInfo info, String status) {
        if (statusCollector != null) {
            statusCollector.reportStatus(info, status, null);
        }
    }

    private void logTaskCompleted(String taskId) {
        if (statusCollector != null) {
            statusCollector.logTaskCompleted(taskId);
        }
    }

    private static void logTaskError(
            String taskId, TaskInfo info, Exception cause, boolean nativeUnsupported) {
        if (nativeUnsupported) {
            LOG.warn(
                    "event=native_unsupported_feature taskId={} queryId={} error={}",
                    taskId,
                    info.queryId(),
                    cause.getMessage());
        } else {
            LOG.error(
                    "event=task_execution_failed taskId={} queryId={} error={}",
                    taskId,
                    info.queryId(),
                    cause.getMessage(),
                    cause);
        }
    }

    private void logTaskErrorToCollector(String taskId, Exception cause) {
        if (statusCollector != null) {
            statusCollector.logTaskError(taskId, cause);
        }
    }

    private void reportErrorStatus(TaskInfo info, Exception cause, boolean nativeUnsupported) {
        String errorCode =
                nativeUnsupported
                        ? PangrovaErrorCode.NATIVE_FALLBACK_REQUIRED.getCode()
                        : "INTERNAL_ERROR";
        ErrorDetail error = nativeFailureDetail(info);
        var admission = admissionFailure(cause);
        if (error == null && admission != null) {
            if (statusCollector != null) {
                statusCollector.reportWorkerTerminalFailure(info, admission.executionError());
            }
            return;
        }
        if (error == null) {
            error =
                    new ErrorDetail(
                            errorCode, cause.getClass().getSimpleName(), cause.getMessage());
        }
        if (statusCollector != null) {
            // A Native report may be absent, malformed, or describe a different
            // state (for example FINISHED before stats capture failed). The
            // Worker owns its failure and reports conservative zero coverage.
            statusCollector.reportWorkerTerminalStatus(info, "FAILED", error);
        }
    }

    private static org.pangrova.worker.exchange.remote.RemoteShuffleAdmissionException
            admissionFailure(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current
                    instanceof
                    org.pangrova.worker.exchange.remote.RemoteShuffleAdmissionException admission) {
                return admission;
            }
        }
        return null;
    }

    private static ErrorDetail nativeFailureDetail(TaskInfo info) {
        byte[] encoded = info.terminalTaskReport();
        if (encoded == null || encoded.length == 0 || info.request() == null) {
            return null;
        }
        try {
            var status = org.pangrova.protocol.v2.TaskReport.parseFrom(encoded);
            if (status.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                    || !status.hasError()
                    || !status.getAttempt().equals(info.request().taskRequest().getAttempt())
                    || status.getError().getCode().isBlank()) {
                return null;
            }
            var nativeError = status.getError();
            return new ErrorDetail(
                    nativeError.getCode(),
                    nativeError.getErrorClass().name(),
                    nativeError.getMessage());
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            return null;
        }
    }

    static boolean isNativeUnsupportedFeature(Exception cause) {
        if (cause instanceof PangrovaException ae
                && ae.getErrorCode() == PangrovaErrorCode.NATIVE_FALLBACK_REQUIRED) {
            return true;
        }
        String msg = cause.getMessage();
        return msg != null && msg.startsWith("NATIVE_FALLBACK_REQUIRED");
    }
}
