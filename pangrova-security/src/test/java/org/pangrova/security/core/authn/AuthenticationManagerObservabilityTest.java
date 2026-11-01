package org.pangrova.security.core.authn;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.spi.security.authn.AuthenticationResult;
import org.pangrova.spi.security.authn.Authenticator;
import org.pangrova.spi.security.authn.AuthenticatorHealthStatus;
import org.pangrova.security.core.authn.provider.bypass.BypassAuthenticator;

/**
 * Validates: Requirements 2.54 — Auth failure observability and degradation strategy.
 *
 * Tests cover:
 * - Per-authenticator success/failure counters and latency tracking
 * - Health status transitions (HEALTHY → DEGRADED → UNHEALTHY)
 * - Unhealthy authenticators remain authoritative and cannot downgrade authentication
 * - Periodic health check recovers status
 * - Structured logging (verified indirectly via metrics)
 */
class AuthenticationManagerObservabilityTest {

    private static final AuthenticationRequest PASSWORD_REQUEST =
            new AuthenticationRequest("user", "pass".toCharArray(), null, null, Map.of(), "10.0.0.1");

    @Test
    void tracksSuccessCountAndLatency() {
        BypassAuthenticator noop = new BypassAuthenticator();
        noop.init(Map.of("bypass.enabled", "true"));
        try (AuthenticationManager manager = new AuthenticationManager(List.of(noop))) {
            manager.authenticate(PASSWORD_REQUEST);
            manager.authenticate(PASSWORD_REQUEST);

            Map<String, AuthenticatorMetrics> metrics = manager.getMetrics();
            AuthenticatorMetrics m = metrics.get("bypass");
            assertThat(m).isNotNull();
            assertThat(m.successCount()).isEqualTo(2);
            assertThat(m.failureCount()).isZero();
            assertThat(m.healthStatus()).isEqualTo(AuthenticatorHealthStatus.HEALTHY);
        }
    }

    @Test
    void tracksFailureCountAndUpdatesHealthStatus() {
        Authenticator alwaysFails = failingAuthenticator("failing-auth");
        try (AuthenticationManager manager = new AuthenticationManager(List.of(alwaysFails))) {
            // 5 failures should transition to UNHEALTHY
            for (int i = 0; i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }

            AuthenticatorMetrics m = manager.getMetrics().get("failing-auth");
            assertThat(m.failureCount()).isEqualTo(AuthenticationManager.UNHEALTHY_THRESHOLD);
            assertThat(m.successCount()).isZero();
            assertThat(m.healthStatus()).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
            assertThat(m.consecutiveFailures()).isEqualTo(AuthenticationManager.UNHEALTHY_THRESHOLD);
        }
    }

    @Test
    void healthStatusTransitionsDegradedBeforeUnhealthy() {
        Authenticator alwaysFails = failingAuthenticator("degrading");
        try (AuthenticationManager manager = new AuthenticationManager(List.of(alwaysFails))) {
            // Before threshold: HEALTHY
            assertThat(manager.getHealthStatus("degrading")).isEqualTo(AuthenticatorHealthStatus.HEALTHY);

            // Reach DEGRADED threshold
            for (int i = 0; i < AuthenticationManager.DEGRADED_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("degrading")).isEqualTo(AuthenticatorHealthStatus.DEGRADED);

            // Reach UNHEALTHY threshold
            for (int i = AuthenticationManager.DEGRADED_THRESHOLD;
                 i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("degrading")).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
        }
    }

    @Test
    void successResetsConsecutiveFailuresAndHealthStatus() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Authenticator toggleable = new Authenticator() {
            @Override public String getName() { return "toggle"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                if (shouldFail.get()) {
                    throw new RuntimeException("backend down");
                }
                return AuthenticationResult.success(
                        new org.pangrova.spi.security.UserIdentity("user", java.util.Set.of(), Map.of(), java.time.Instant.now()));
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return !shouldFail.get(); }
            @Override public void close() {}
        };

        try (AuthenticationManager manager = new AuthenticationManager(List.of(toggleable))) {
            // Drive to DEGRADED
            for (int i = 0; i < AuthenticationManager.DEGRADED_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("toggle")).isEqualTo(AuthenticatorHealthStatus.DEGRADED);

            // One success resets to HEALTHY
            shouldFail.set(false);
            AuthenticationResult result = manager.authenticate(PASSWORD_REQUEST);
            assertThat(result.success()).isTrue();
            assertThat(manager.getHealthStatus("toggle")).isEqualTo(AuthenticatorHealthStatus.HEALTHY);

            AuthenticatorMetrics m = manager.getMetrics().get("toggle");
            assertThat(m.consecutiveFailures()).isZero();
        }
    }

    @Test
    void unhealthyAuthenticatorCannotSelectBypass() {
        Authenticator unhealthyAuth = failingAuthenticator("primary");
        BypassAuthenticator fallback = new BypassAuthenticator();
        fallback.init(Map.of("bypass.enabled", "true"));

        try (AuthenticationManager manager = new AuthenticationManager(List.of(unhealthyAuth, fallback))) {
            // Drive primary to UNHEALTHY
            for (int i = 0; i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("primary")).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);

            // Backend health must not change the authoritative authenticator.
            AuthenticationResult result = manager.authenticate(PASSWORD_REQUEST);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).isEqualTo(AuthenticationManager.GENERIC_FAILURE_MESSAGE);

            AuthenticatorMetrics primaryMetrics = manager.getMetrics().get("primary");
            assertThat(primaryMetrics.failureCount()).isEqualTo(AuthenticationManager.UNHEALTHY_THRESHOLD + 1);
            assertThat(manager.getMetrics().get("bypass").successCount()).isZero();
        }
    }

    @Test
    void degradedAuthenticatorIsNotSkipped() {
        AtomicInteger callCount = new AtomicInteger();
        Authenticator degradedAuth = new Authenticator() {
            @Override public String getName() { return "degraded-auth"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                callCount.incrementAndGet();
                throw new IllegalStateException("backend temporarily unavailable");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return false; }
            @Override public void close() {}
        };

        try (AuthenticationManager manager = new AuthenticationManager(List.of(degradedAuth))) {
            // Drive to DEGRADED (but not UNHEALTHY)
            for (int i = 0; i < AuthenticationManager.DEGRADED_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("degraded-auth")).isEqualTo(AuthenticatorHealthStatus.DEGRADED);

            int countBefore = callCount.get();
            manager.authenticate(PASSWORD_REQUEST);
            // DEGRADED authenticator is still tried
            assertThat(callCount.get()).isEqualTo(countBefore + 1);
        }
    }

    @Test
    void healthCheckRecoversPreviouslyUnhealthyAuthenticator() {
        AtomicBoolean healthy = new AtomicBoolean(false);
        Authenticator recoverableAuth = new Authenticator() {
            @Override public String getName() { return "recoverable"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                throw new RuntimeException("down");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return healthy.get(); }
            @Override public void close() {}
        };

        try (AuthenticationManager manager = new AuthenticationManager(List.of(recoverableAuth))) {
            // Drive to UNHEALTHY
            for (int i = 0; i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
                manager.authenticate(PASSWORD_REQUEST);
            }
            assertThat(manager.getHealthStatus("recoverable")).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);

            // Simulate backend recovery
            healthy.set(true);
            manager.runHealthChecks();

            assertThat(manager.getHealthStatus("recoverable")).isEqualTo(AuthenticatorHealthStatus.HEALTHY);
            AuthenticatorMetrics m = manager.getMetrics().get("recoverable");
            assertThat(m.consecutiveFailures()).isZero();
        }
    }

    @Test
    void healthCheckDrivesStatusToUnhealthy() {
        Authenticator alwaysUnhealthy = new Authenticator() {
            @Override public String getName() { return "always-sick"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                return AuthenticationResult.failure("nope");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return false; }
            @Override public void close() {}
        };

        try (AuthenticationManager manager = new AuthenticationManager(List.of(alwaysUnhealthy))) {
            for (int i = 0; i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
                manager.runHealthChecks();
            }
            assertThat(manager.getHealthStatus("always-sick")).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
        }
    }

    @Test
    void metricsSnapshotIsImmutable() {
        BypassAuthenticator noop = new BypassAuthenticator();
        noop.init(Map.of("bypass.enabled", "true"));
        try (AuthenticationManager manager = new AuthenticationManager(List.of(noop))) {
            manager.authenticate(PASSWORD_REQUEST);

            Map<String, AuthenticatorMetrics> snapshot1 = manager.getMetrics();
            manager.authenticate(PASSWORD_REQUEST);
            Map<String, AuthenticatorMetrics> snapshot2 = manager.getMetrics();

            // First snapshot should not be affected by subsequent operations
            assertThat(snapshot1.get("bypass").successCount()).isEqualTo(1);
            assertThat(snapshot2.get("bypass").successCount()).isEqualTo(2);
        }
    }

    @Test
    void deriveHealthStatusBoundaries() {
        assertThat(AuthenticationManager.deriveHealthStatus(0))
                .isEqualTo(AuthenticatorHealthStatus.HEALTHY);
        assertThat(AuthenticationManager.deriveHealthStatus(AuthenticationManager.DEGRADED_THRESHOLD - 1))
                .isEqualTo(AuthenticatorHealthStatus.HEALTHY);
        assertThat(AuthenticationManager.deriveHealthStatus(AuthenticationManager.DEGRADED_THRESHOLD))
                .isEqualTo(AuthenticatorHealthStatus.DEGRADED);
        assertThat(AuthenticationManager.deriveHealthStatus(AuthenticationManager.UNHEALTHY_THRESHOLD - 1))
                .isEqualTo(AuthenticatorHealthStatus.DEGRADED);
        assertThat(AuthenticationManager.deriveHealthStatus(AuthenticationManager.UNHEALTHY_THRESHOLD))
                .isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
        assertThat(AuthenticationManager.deriveHealthStatus(100))
                .isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
    }

    @Test
    void periodicHealthCheckSchedulerCanBeCreatedAndClosed() {
        BypassAuthenticator noop = new BypassAuthenticator();
        noop.init(Map.of("bypass.enabled", "true"));
        // Create with periodic health check (very long interval so it won't fire during test)
        try (AuthenticationManager manager = new AuthenticationManager(
                List.of(noop), 1, TimeUnit.HOURS)) {
            AuthenticationResult result = manager.authenticate(PASSWORD_REQUEST);
            assertThat(result.success()).isTrue();
        }
        // close() should not throw
    }

    @Test
    void averageLatencyCalculation() {
        AuthenticatorMetrics zeroAttempts = new AuthenticatorMetrics(
                "test", 0, 0, 0, AuthenticatorHealthStatus.HEALTHY, 0);
        assertThat(zeroAttempts.averageLatencyMs()).isZero();

        AuthenticatorMetrics withAttempts = new AuthenticatorMetrics(
                "test", 3, 2, 500, AuthenticatorHealthStatus.HEALTHY, 0);
        assertThat(withAttempts.averageLatencyMs()).isEqualTo(100); // 500 / 5
    }

    @Test
    void authenticationFailureResultDoesNotCountAsException() {
        // An authenticator that returns failure (not throws) should still track failure metrics
        Authenticator returnsFailure = new Authenticator() {
            @Override public String getName() { return "soft-fail"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                return AuthenticationResult.failure("bad credentials");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return true; }
            @Override public void close() {}
        };

        try (AuthenticationManager manager = new AuthenticationManager(List.of(returnsFailure))) {
            for (int i = 0; i < 3; i++) {
                AuthenticationResult result = manager.authenticate(PASSWORD_REQUEST);
                assertThat(result.success()).isFalse();
                // The original failure message is preserved (not replaced with generic)
                assertThat(result.errorMessage()).isEqualTo("bad credentials");
            }

            AuthenticatorMetrics m = manager.getMetrics().get("soft-fail");
            assertThat(m.failureCount()).isEqualTo(3);
            assertThat(m.successCount()).isZero();
            assertThat(m.healthStatus()).isEqualTo(AuthenticatorHealthStatus.HEALTHY);
            assertThat(m.consecutiveFailures()).isZero();
        }
    }

    // --- Helpers ---

    private static Authenticator failingAuthenticator(String name) {
        return new Authenticator() {
            @Override public String getName() { return name; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                throw new RuntimeException("backend unreachable");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return false; }
            @Override public void close() {}
        };
    }
}
