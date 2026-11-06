package org.pangrova.planner.core.throttle;

/**
 * Result of a throttle acquisition attempt.
 */
public record ThrottleResult(
        boolean acquired,
        String reason,
        ThrottlePermit permit
) {

    public static ThrottleResult success(ThrottlePermit permit) {
        return new ThrottleResult(true, null, permit);
    }

    public static ThrottleResult rejected(String reason) {
        return new ThrottleResult(false, reason, null);
    }
}
