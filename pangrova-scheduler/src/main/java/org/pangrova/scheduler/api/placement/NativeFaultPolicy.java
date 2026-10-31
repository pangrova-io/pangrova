package org.pangrova.scheduler.api.placement;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Controls Java-side behavior after a native (C++) crash (SIGSEGV, SIGABRT, etc.)
 * is detected by the JNI bridge.
 *
 * <p>The native layer historically used {@code siglongjmp} to recover from SIGSEGV
 * and continue serving on the same worker process. This is unsafe: the crash may
 * have corrupted native heap, native operator state, or off-heap buffers, causing
 * silent data corruption on subsequent tasks.</p>
 *
 * <p>This SPI replaces the continue-after-crash model with explicit policy control:</p>
 * <ul>
 *   <li>{@link #FAIL_FAST} (default, recommended): mark the task as failed
 *       immediately; the scheduler retries on a different worker via the
 *       {@link FaultTolerancePolicy} retry budget.</li>
 *   <li>{@link #CONTINUE_WITH_HEALTH_CHECK}: attempt to continue, but only if
 *       a {@link NativeHealthChecker} confirms the native state is not corrupted.
 *       If the health check fails, falls back to {@code FAIL_FAST}.</li>
 * </ul>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Gluten/native engine:</b> Gluten marks the entire executor as unhealthy after
 *       a native crash and lets the cluster manager restart the container.</li>
 *   <li><b>Spark + Photon:</b> Photon crashes fail the task; Spark retries on a
 *       different executor.</li>
 * </ul>
 *
 * @see FaultTolerancePolicy
 * @see NativeHealthChecker
 */
@Experimental(reason = "G-SCHED-07 native fault model; replaces siglongjmp recovery")
public enum NativeFaultPolicy {

    /**
     * Default: immediately fail the task and mark the worker as degraded.
     * The scheduler retries the task on a healthy worker per the configured
     * {@link RetryBudget}. No attempt is made to reuse the crashed native
     * process for further tasks.
     */
    FAIL_FAST,

    /**
     * Attempt to continue on the same worker, but only after a
     * {@link NativeHealthChecker} confirms the native heap and operator
     * state are not corrupted. If the health check fails or is not
     * configured, falls back to {@link #FAIL_FAST}.
     *
     * <p>Use this only for crashes known to be contained (e.g., a native
     * expression evaluation OOB that does not corrupt global state).</p>
     */
    CONTINUE_WITH_HEALTH_CHECK
}
