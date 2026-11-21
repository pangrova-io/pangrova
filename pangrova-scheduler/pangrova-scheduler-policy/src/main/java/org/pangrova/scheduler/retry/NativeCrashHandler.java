package org.pangrova.scheduler.retry;

import org.pangrova.scheduler.api.enums.NativeFaultPolicy;
import org.pangrova.scheduler.api.health.NativeHealthChecker;

import org.pangrova.common.api.annotation.Experimental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles native crash signals (SIGSEGV, SIGABRT) on the Java side by applying
 * the configured {@link NativeFaultPolicy}.
 *
 * <p>Replaces the old siglongjmp-based recovery: instead of continuing to serve
 * on a potentially corrupted native process, this handler either fails the task
 * immediately (FAIL_FAST) or conditionally continues after a health check.</p>
 */
@Experimental(reason = "G-SCHED-07 native crash handling")
public final class NativeCrashHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NativeCrashHandler.class);

    private final NativeFaultPolicy policy;
    private final NativeHealthChecker healthChecker;

    /**
     * Creates a crash handler with the given policy and optional health checker.
     *
     * @param policy        the fault policy to apply (never null)
     * @param healthChecker health checker for CONTINUE_WITH_HEALTH_CHECK (nullable)
     */
    public NativeCrashHandler(NativeFaultPolicy policy, NativeHealthChecker healthChecker) {
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        this.policy = policy;
        this.healthChecker = healthChecker;
    }

    /** Creates a handler with FAIL_FAST policy (no health checker needed). */
    public NativeCrashHandler() {
        this(NativeFaultPolicy.FAIL_FAST, null);
    }

    /**
     * Handles a native crash for the given task.
     *
     * @param taskId      the task that experienced the crash
     * @param signal      the signal name (e.g. "SIGSEGV", "SIGABRT")
     * @param crashDetail diagnostic detail from the native layer
     * @return the action the caller should take
     */
    public CrashAction handleCrash(String taskId, String signal, String crashDetail) {
        LOG.error("event=native_crash taskId={} signal={} detail={} policy={}",
                taskId, signal, crashDetail, policy);

        if (policy == NativeFaultPolicy.FAIL_FAST) {
            return CrashAction.failTask(taskId, "native crash: " + signal + " - " + crashDetail);
        }

        // CONTINUE_WITH_HEALTH_CHECK
        if (healthChecker == null) {
            LOG.warn("event=no_health_checker_configured taskId={} falling_back_to=FAIL_FAST", taskId);
            return CrashAction.failTask(taskId, "native crash (no health checker): " + signal);
        }

        NativeHealthChecker.NativeHealthResult result = healthChecker.checkHealth();
        if (result.healthy()) {
            LOG.info("event=health_check_passed taskId={} crashCount={}", taskId, result.crashCount());
            return CrashAction.continueOnWorker(taskId);
        } else {
            LOG.warn("event=health_check_failed taskId={} detail={} crashCount={}",
                    taskId, result.details(), result.crashCount());
            return CrashAction.failTask(taskId,
                    "native crash + health check failed: " + result.details());
        }
    }

    /** Returns the configured policy. */
    public NativeFaultPolicy policy() {
        return policy;
    }

    /**
     * Action to take after handling a native crash.
     */
    public sealed interface CrashAction {
        String taskId();

        /** Fail the task; scheduler will retry on a different worker. */
        record FailTask(String taskId, String reason) implements CrashAction {}

        /** Continue on the same worker (health check passed). */
        record ContinueOnWorker(String taskId) implements CrashAction {}

        static CrashAction failTask(String taskId, String reason) {
            return new FailTask(taskId, reason);
        }

        static CrashAction continueOnWorker(String taskId) {
            return new ContinueOnWorker(taskId);
        }
    }
}
