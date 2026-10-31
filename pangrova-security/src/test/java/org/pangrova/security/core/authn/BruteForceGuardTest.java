package org.pangrova.security.core.authn;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BruteForceGuard}.
 */
class BruteForceGuardTest {

    @Test
    void newGuard_noAttemptsRecorded_isNotBlocked() {
        BruteForceGuard guard = new BruteForceGuard();
        assertThat(guard.isBlocked("192.168.1.1")).isFalse();
    }

    @Test
    void belowThreshold_isNotBlocked() {
        BruteForceGuard guard = new BruteForceGuard();
        for (int i = 0; i < 4; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        assertThat(guard.isBlocked("10.0.0.1")).isFalse();
        assertThat(guard.getFailureCount("10.0.0.1")).isEqualTo(4);
    }

    @Test
    void atThreshold_isBlocked() {
        BruteForceGuard guard = new BruteForceGuard();
        for (int i = 0; i < 5; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        assertThat(guard.isBlocked("10.0.0.1")).isTrue();
        assertThat(guard.getFailureCount("10.0.0.1")).isEqualTo(5);
    }

    @Test
    void aboveThreshold_remainsBlocked() {
        BruteForceGuard guard = new BruteForceGuard();
        for (int i = 0; i < 7; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        assertThat(guard.isBlocked("10.0.0.1")).isTrue();
    }

    @Test
    void successResetsCounter() {
        BruteForceGuard guard = new BruteForceGuard();
        for (int i = 0; i < 4; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        guard.recordSuccess("10.0.0.1");
        assertThat(guard.isBlocked("10.0.0.1")).isFalse();
        assertThat(guard.getFailureCount("10.0.0.1")).isEqualTo(0);
    }

    @Test
    void differentIpsAreIndependent() {
        BruteForceGuard guard = new BruteForceGuard();
        for (int i = 0; i < 5; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        assertThat(guard.isBlocked("10.0.0.1")).isTrue();
        assertThat(guard.isBlocked("10.0.0.2")).isFalse();
    }

    @Test
    void customMaxAttempts() {
        BruteForceGuard guard = new BruteForceGuard(3, Duration.ofMinutes(10));
        for (int i = 0; i < 2; i++) {
            guard.recordFailedAttempt("10.0.0.1");
        }
        assertThat(guard.isBlocked("10.0.0.1")).isFalse();
        guard.recordFailedAttempt("10.0.0.1");
        assertThat(guard.isBlocked("10.0.0.1")).isTrue();
    }

    @Test
    void constructorRejectsZeroMaxAttempts() {
        assertThatThrownBy(() -> new BruteForceGuard(0, Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts must be positive");
    }

    @Test
    void constructorRejectsNegativeMaxAttempts() {
        assertThatThrownBy(() -> new BruteForceGuard(-1, Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts must be positive");
    }

    @Test
    void constructorRejectsNullDuration() {
        assertThatThrownBy(() -> new BruteForceGuard(5, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsZeroDuration() {
        assertThatThrownBy(() -> new BruteForceGuard(5, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lockoutDuration must be positive");
    }

    @Test
    void constructorRejectsNegativeDuration() {
        assertThatThrownBy(() -> new BruteForceGuard(5, Duration.ofMinutes(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lockoutDuration must be positive");
    }

    @Test
    void isBlockedRejectsNullIp() {
        BruteForceGuard guard = new BruteForceGuard();
        assertThatThrownBy(() -> guard.isBlocked(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void recordFailedAttemptRejectsNullIp() {
        BruteForceGuard guard = new BruteForceGuard();
        assertThatThrownBy(() -> guard.recordFailedAttempt(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void recordSuccessRejectsNullIp() {
        BruteForceGuard guard = new BruteForceGuard();
        assertThatThrownBy(() -> guard.recordSuccess(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void accessors() {
        BruteForceGuard guard = new BruteForceGuard(7, Duration.ofMinutes(10));
        assertThat(guard.maxAttempts()).isEqualTo(7);
        assertThat(guard.lockoutDuration()).isEqualTo(Duration.ofMinutes(10));
    }
}
