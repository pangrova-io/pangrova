package org.pangrova.security.authentication;

import java.time.Duration;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link BruteForceGuard} lockout threshold precision.
 *
 * <p><b>Validates: Requirements 8.6</b></p>
 *
 * <p>Correctness Property 10: For any IP address, after exactly 5 failed attempts
 * within 5 minutes, the next attempt SHALL be blocked; after lockout expiry,
 * attempts SHALL be allowed again.</p>
 */
class BruteForceGuardPropertyTest {

    /**
     * For any IP and any number of failed attempts below the threshold,
     * the IP SHALL NOT be blocked.
     */
    @Property(tries = 100)
    void belowThreshold_neverBlocked(
            @ForAll @StringLength(min = 7, max = 15) String ip,
            @ForAll @IntRange(min = 1, max = 4) int attempts) {

        BruteForceGuard guard = new BruteForceGuard(5, Duration.ofMinutes(5));
        for (int i = 0; i < attempts; i++) {
            guard.recordFailedAttempt(ip);
        }
        assertThat(guard.isBlocked(ip)).isFalse();
    }

    /**
     * For any IP and any number of failed attempts at or above the threshold,
     * the IP SHALL be blocked.
     */
    @Property(tries = 100)
    void atOrAboveThreshold_alwaysBlocked(
            @ForAll @StringLength(min = 7, max = 15) String ip,
            @ForAll @IntRange(min = 5, max = 20) int attempts) {

        BruteForceGuard guard = new BruteForceGuard(5, Duration.ofMinutes(5));
        for (int i = 0; i < attempts; i++) {
            guard.recordFailedAttempt(ip);
        }
        assertThat(guard.isBlocked(ip)).isTrue();
    }

    /**
     * For any IP that has been blocked, recording a success SHALL unblock it.
     */
    @Property(tries = 100)
    void successAfterLockout_unblocks(
            @ForAll @StringLength(min = 7, max = 15) String ip,
            @ForAll @IntRange(min = 5, max = 20) int attempts) {

        BruteForceGuard guard = new BruteForceGuard(5, Duration.ofMinutes(5));
        for (int i = 0; i < attempts; i++) {
            guard.recordFailedAttempt(ip);
        }
        assertThat(guard.isBlocked(ip)).isTrue();

        guard.recordSuccess(ip);
        assertThat(guard.isBlocked(ip)).isFalse();
        assertThat(guard.getFailureCount(ip)).isEqualTo(0);
    }

    /**
     * For any two distinct IPs, blocking one SHALL NOT affect the other.
     */
    @Property(tries = 100)
    void perIpIsolation(
            @ForAll @StringLength(min = 7, max = 15) String blockedIp,
            @ForAll @StringLength(min = 7, max = 15) String otherIp) {

        if (blockedIp.equals(otherIp)) {
            return; // skip trivial case
        }

        BruteForceGuard guard = new BruteForceGuard(5, Duration.ofMinutes(5));
        for (int i = 0; i < 5; i++) {
            guard.recordFailedAttempt(blockedIp);
        }
        assertThat(guard.isBlocked(blockedIp)).isTrue();
        assertThat(guard.isBlocked(otherIp)).isFalse();
    }

    /**
     * For any configurable maxAttempts, the threshold is respected precisely:
     * (maxAttempts - 1) failures → not blocked, maxAttempts failures → blocked.
     */
    @Property(tries = 100)
    void configurableThreshold_preciseEnforcement(
            @ForAll @IntRange(min = 1, max = 10) int maxAttempts) {

        BruteForceGuard guard = new BruteForceGuard(maxAttempts, Duration.ofMinutes(5));
        String ip = "10.0.0.1";

        for (int i = 0; i < maxAttempts - 1; i++) {
            guard.recordFailedAttempt(ip);
        }
        assertThat(guard.isBlocked(ip))
                .as("Should NOT be blocked after %d attempts (threshold=%d)", maxAttempts - 1, maxAttempts)
                .isFalse();

        guard.recordFailedAttempt(ip);
        assertThat(guard.isBlocked(ip))
                .as("Should be blocked after %d attempts (threshold=%d)", maxAttempts, maxAttempts)
                .isTrue();
    }
}
