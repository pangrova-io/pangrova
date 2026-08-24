package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Monitors task completion by polling native executor state and handles
 * task lifecycle transitions on completion or error.
 *
 * <p>Concurrency strategy: stateless -- all mutable state lives in TaskInfo.
 * Thread safety delegated to TaskManager's ConcurrentHashMap registry
 * and per-task AtomicReference state.</p>
 *
 * <p><b>Task 188 -- Thread pool separation:</b> The completion monitor runs
 * on a dedicated orchestration executor pool
 * ({@code WorkerTaskBootstrap.createOrchestrationExecutor}) that is
 * intentionally separate from the native driver pool (folly executor).
 * This separation prevents morsel-driven scheduling deadlock: blocking
 * poll-until-finished calls on driver pool threads would starve morsel
 * delivery under fan-out workloads. The orchestration pool uses bounded
 * {@link java.util.concurrent.ThreadPoolExecutor} with
 * {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} as
 * the saturation strategy -- if all orchestration threads are busy, the
 * submitting thread runs the monitor task inline rather than deadlocking.</p>
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
     * Monitors task completion by polling the native executor state.
     * Used for autonomous stages where no Java data threads are started.
     */
    void startCompletionMonitor(String taskId, TaskInfo info,
                                long handle,
                                Consumer<String> onCompleted,
                                Consumer<Exception> onError) {
        deps.completionMonitorExecutor().submit(() -> {
            Thread.currentThread().setName("completion-monitor-" + taskId);
            try {
                pollUntilFinished(taskId, handle);
                onCompleted.accept(taskId);
            } catch (InterruptedException e) {
                // Normal shutdown signal — TaskManager.close() interrupts
                // completion monitor threads. Not a task failure.
                Thread.currentThread().interrupt();
                LOG.debug("event=completion_monitor_shutdown taskId={} queryId={}",
                        taskId, info.queryId());
            } catch (Exception e) {
                onError.accept(e);
            }
        });
    }

    void onTaskCompleted(String taskId, TaskInfo info,
                         TaskLifecycleManager.LifecycleCallback callback,
                         Runnable releaseResources) {
        if (!callback.transition(taskId, TaskManager.TaskState.FINISHING)) {
            // Task may already be CANCELLED or FAILED.
            // Still release resources to prevent native handle / memory leaks.
            releaseResources.run();
            return;
        }
        releaseResources.run();
        reportStatus(info, "FINISHED");
        if (!callback.transition(taskId, TaskManager.TaskState.FINISHED)) {
            return;
        }
        logTaskCompleted(taskId);
        callback.clearCoordinatorIndex(info);
        callback.notifyCompletion(info.queryId());
    }

    void onTaskError(String taskId, TaskInfo info, Exception cause,
                     TaskLifecycleManager.LifecycleCallback callback,
                     Runnable releaseResources) {
        boolean nativeUnsupported = isNativeUnsupportedFeature(cause);
        logTaskError(taskId, info, cause, nativeUnsupported);
        logTaskErrorToCollector(taskId, cause);
        releaseResources.run();
        reportErrorStatus(info, cause, nativeUnsupported);
        if (callback.transition(taskId, TaskManager.TaskState.FAILED)) {
            callback.clearCoordinatorIndex(info);
        }
    }

    private static final int MAX_TRANSIENT_RETRIES = 3;

    /** Upper bound for the transient-failure retry backoff. */
    private static final long MAX_TRANSIENT_BACKOFF_MS = 8_000;

    /** Never-counted latch used as an interruptible sleep primitive. */
    private static final CountDownLatch SLEEP_LATCH = new CountDownLatch(1);

    /**
     * Poll interval for checking native task completion. Must be low (50ms)
     * because the native memory pool is only freed when releaseAllResources
     * executes after isFinished returns true. A long poll interval causes
     * subsequent tasks to block in PangrovaArbitrator::growCapacity() waiting
     * for the previous task's pool to be removed.
     */
    private static final long INITIAL_BACKOFF_MS = 50;

    private void pollUntilFinished(String taskId, long handle) throws Exception {
        int transientRetries = 0;
        long backoffMs = INITIAL_BACKOFF_MS;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (deps.taskExecutor().isFinished(handle)) {
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
    }

    private void checkExecutionQuota(String taskId) {
        TenantExecutionQuotaEnforcer enforcer = this.quotaEnforcer;
        if (enforcer != null) {
            enforcer.checkDuringExecution(taskId);
        }
    }

    /** Interruptible sleep that propagates interruption as a shutdown signal. */
    private static void sleepInterruptibly(long millis) throws InterruptedException {
        SLEEP_LATCH.await(millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Reacts to a poll failure based on its classification. Returns the updated
     * backoff for transient failures; rethrows for all other categories.
     */
    private static long handlePollFailure(long handle, Exception e,
                                          int transientRetries, long backoffMs) throws Exception {
        NativeTaskFailureClassifier.Category category =
                NativeTaskFailureClassifier.classify(e);
        switch (category) {
            case NATIVE_CRASHED -> {
                LOG.error("event=native_crashed executorHandle={} error={}",
                        handle, e.getMessage(), e);
                throw e;
            }
            case NATIVE_TASK_FAILED -> {
                LOG.error("event=native_task_failed executorHandle={} error={}",
                        handle, e.getMessage(), e);
                throw e;
            }
            case NATIVE_HUNG -> {
                LOG.warn("event=native_hung executorHandle={} error={}",
                        handle, e.getMessage());
                TimeoutException timeout = new TimeoutException(
                        "Native executor unresponsive: " + e.getMessage());
                timeout.initCause(e);
                throw new RuntimeException(timeout);
            }
            case TRANSIENT -> {
                if (transientRetries > MAX_TRANSIENT_RETRIES) {
                    LOG.error("event=transient_retries_exhausted executorHandle={} "
                                    + "retries={} error={}",
                            handle, transientRetries, e.getMessage(), e);
                    throw e;
                }
                LOG.warn("event=transient_failure executorHandle={} retry={} "
                                + "backoffMs={} error={}",
                        handle, transientRetries, backoffMs, e.getMessage());
                sleepInterruptibly(backoffMs);
                return Math.min(backoffMs * 2, MAX_TRANSIENT_BACKOFF_MS);
            }
            default -> throw new IllegalStateException(
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

    private static void logTaskError(String taskId, TaskInfo info,
                                     Exception cause, boolean nativeUnsupported) {
        if (nativeUnsupported) {
            LOG.warn("event=native_unsupported_feature taskId={} queryId={} error={}",
                    taskId, info.queryId(), cause.getMessage());
        } else {
            LOG.error("event=task_execution_failed taskId={} queryId={} error={}",
                    taskId, info.queryId(), cause.getMessage(), cause);
        }
    }

    private void logTaskErrorToCollector(String taskId, Exception cause) {
        if (statusCollector != null) {
            statusCollector.logTaskError(taskId, cause);
        }
    }

    private void reportErrorStatus(TaskInfo info, Exception cause,
                                   boolean nativeUnsupported) {
        String errorCode = nativeUnsupported
                ? PangrovaErrorCode.NATIVE_FALLBACK_REQUIRED.getCode()
                : "INTERNAL_ERROR";
        ErrorDetail error = new ErrorDetail(
                errorCode, cause.getClass().getSimpleName(), cause.getMessage());
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "FAILED", error);
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
