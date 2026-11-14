package org.pangrova.scheduler.api.health;

import org.pangrova.scheduler.api.enums.NativeFaultPolicy;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for checking native (C++) runtime health after a crash signal.
 *
 * <p>Implementations typically call into the native layer via JNI to verify:
 * <ul>
 *   <li>Memory allocator integrity (no corrupted free-lists)</li>
 *   <li>Thread pool responsiveness (can dispatch a trivial task)</li>
 *   <li>Native runtime singleton state (not in a half-destroyed state)</li>
 * </ul>
 *
 * <p>Consulted by {@code NativeFaultPolicy#CONTINUE_WITH_HEALTH_CHECK} before
 * deciding whether the worker can accept new tasks after a native crash.</p>
 *
 * @see org.pangrova.scheduler.api.enums.NativeFaultPolicy
 */
@Experimental(reason = "G-SCHED-07 native health check after crash")
public interface NativeHealthChecker {

    /**
     * Checks whether the native runtime is healthy enough to accept new tasks.
     *
     * @return a health result indicating pass/fail and diagnostic details
     */
    NativeHealthResult checkHealth();

    /**
     * Result of a native health check.
     *
     * @param healthy     true if the native runtime passed all checks
     * @param details     human-readable diagnostic (e.g. "memory allocator OK, thread pool responsive")
     * @param crashCount  cumulative native crash count since worker start
     */
    record NativeHealthResult(boolean healthy, String details, int crashCount) {

        /** Convenience factory for a healthy result. */
        public static NativeHealthResult healthy(int crashCount) {
            return new NativeHealthResult(true, "all checks passed", crashCount);
        }

        /** Convenience factory for an unhealthy result. */
        public static NativeHealthResult unhealthy(String reason, int crashCount) {
            return new NativeHealthResult(false, reason, crashCount);
        }
    }
}
