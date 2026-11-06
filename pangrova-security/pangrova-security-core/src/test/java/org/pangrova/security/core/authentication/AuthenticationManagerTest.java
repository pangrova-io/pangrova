package org.pangrova.security.core.authentication;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

import org.pangrova.security.api.identity.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.api.authentication.AuthenticationResult;
import org.pangrova.security.spi.authentication.Authenticator;
import org.pangrova.security.api.authentication.AuthenticatorHealthStatus;
import org.pangrova.security.auth.builtin.BypassAuthenticator;
import org.pangrova.security.auth.ldap.LdapAuthenticator;
import org.pangrova.security.auth.oauth2.OAuth2Authenticator;

/**
 * Validates: Requirements 18.1 — Authentication manager delegates to first matching authenticator.
 */
class AuthenticationManagerTest {

    @Test
    void closedManagerRejectsAuthenticationAndRepeatedCloseIsSafe() {
        BypassAuthenticator bypass = new BypassAuthenticator();
        bypass.init(Map.of("bypass.enabled", "true"));
        AuthenticationManager manager = new AuthenticationManager(List.of(bypass));
        var request = new AuthenticationRequest(null, null, null, null, Map.of(), "127.0.0.1");
        assertThat(manager.authenticate(request).success()).isTrue();
        manager.close();
        manager.close();
        manager.runHealthChecks();
        assertThat(manager.authenticate(request).success()).isFalse();
        assertThat(manager.authenticate(request).errorMessage()).contains("closed");
    }

    @Test
    void delegatesToFirstSupportingAuthenticator() {
        BypassAuthenticator bypass = new BypassAuthenticator();
        bypass.init(Map.of("bypass.enabled", "true"));
        AuthenticationManager manager = new AuthenticationManager(List.of(bypass));

        AuthenticationRequest request = new AuthenticationRequest(
                "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");
        AuthenticationResult result = manager.authenticate(request);

        assertThat(result.success()).isTrue();
        assertThat(result.userIdentity().principal()).isEqualTo("anonymous");
    }

    @Test
    void returnsFailureWhenNoAuthenticatorSupports() {
        // OAuth2 only supports token requests
        OAuth2Authenticator oauth2 = new OAuth2Authenticator();
        AuthenticationManager manager = new AuthenticationManager(List.of(oauth2));

        AuthenticationRequest request = new AuthenticationRequest(
                null, null, null, null, Map.of(), "127.0.0.1");
        AuthenticationResult result = manager.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).contains("No authenticator supports");
    }

    @Test
    void triesAuthenticatorsInOrder() {
        // A matching uninitialized provider must fail closed, without trying bypass.
        OAuth2Authenticator oauth2 = new OAuth2Authenticator();
        BypassAuthenticator bypass = new BypassAuthenticator();
        bypass.init(Map.of("bypass.enabled", "true"));
        AuthenticationManager manager = new AuthenticationManager(List.of(oauth2, bypass));

        AuthenticationRequest request = new AuthenticationRequest(
                null, null, "token", null, Map.of(), "127.0.0.1");
        AuthenticationResult result = manager.authenticate(request);

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).contains("not configured");
    }

    @Test
    void handlesAuthenticatorException() {
        Authenticator failing = new Authenticator() {
            @Override public String getName() { return "failing"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                throw new RuntimeException("boom");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return true; }
            @Override public void close() {}
        };
        AuthenticationManager manager = new AuthenticationManager(List.of(failing));

        AuthenticationResult result = manager.authenticate(
                new AuthenticationRequest(null, null, null, null, Map.of(), "127.0.0.1"));

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).isEqualTo(AuthenticationManager.GENERIC_FAILURE_MESSAGE);
        assertThat(result.errorMessage()).doesNotContain("boom");
    }

    @Test
    void rejectsNullRequest() {
        AuthenticationManager manager = new AuthenticationManager(List.of());
        assertThatThrownBy(() -> manager.authenticate(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullAuthenticatorList() {
        assertThatThrownBy(() -> new AuthenticationManager(null))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * Validates: Requirements 1.33, 1.34, 2.33, 2.34
     * Verifies that authentication failure responses never leak backend implementation details
     * such as LDAP paths, connection strings, stack traces, or internal class names.
     */
    @Test
    void authenticationFailureResponseContainsNoImplementationDetails() {
        List<String> sensitiveMessages = List.of(
                "LDAP connection to ldap://internal-ldap:389 failed",
                "javax.naming.CommunicationException: internal-ldap:389",
                "jdbc:postgresql://db-host:5432/auth_db connection refused",
                "OAuth2 token validation failed: https://idp.internal.corp/token",
                "java.lang.NullPointerException\n\tat org.pangrova.security.auth.ldap.LdapAuthenticator.authenticate",
                "uid=admin,ou=users,dc=example,dc=com",
                "Connection refused: connect to ldap://10.0.0.5:636",
                "SSL handshake failed with ldaps://ldap.internal:636"
        );

        for (String sensitiveMessage : sensitiveMessages) {
            Authenticator leaky = new Authenticator() {
                @Override public String getName() { return "leaky"; }
                @Override public void init(Map<String, String> config) {}
                @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                    throw new RuntimeException(sensitiveMessage);
                }
                @Override public boolean supports(AuthenticationRequest r) { return true; }
                @Override public boolean isHealthy() { return true; }
                @Override public void close() {}
            };
            AuthenticationManager manager = new AuthenticationManager(List.of(leaky));

            AuthenticationResult result = manager.authenticate(
                    new AuthenticationRequest("attacker", "wrong".toCharArray(), null, null, Map.of(), "1.2.3.4"));

            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage())
                    .as("Error message must not contain: %s", sensitiveMessage)
                    .isEqualTo(AuthenticationManager.GENERIC_FAILURE_MESSAGE);
            assertThat(result.errorMessage())
                    .doesNotContain("ldap://", "ldaps://", "jdbc:", "https://",
                            "javax.naming", "NullPointerException", "dc=", "ou=",
                            "connection refused", "SSL handshake", "10.0.0.");
        }
    }

    /**
     * Validates BUG-S01: HealthSnapshot atomicity — consecutiveFailures and healthStatus
     * are packed into a single immutable record, updated via CAS.
     */
    @Test
    void healthSnapshotTransitionsDegradedThenUnhealthy() {
        Authenticator failing = createAlwaysFailingAuthenticator("flaky");
        AuthenticationManager manager = new AuthenticationManager(List.of(failing));
        AuthenticationRequest request = new AuthenticationRequest(
                "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");

        // Initial state is HEALTHY
        assertThat(manager.getHealthStatus("flaky")).isEqualTo(AuthenticatorHealthStatus.HEALTHY);

        // 3 consecutive failures → DEGRADED
        for (int i = 0; i < AuthenticationManager.DEGRADED_THRESHOLD; i++) {
            manager.authenticate(request);
        }
        assertThat(manager.getHealthStatus("flaky")).isEqualTo(AuthenticatorHealthStatus.DEGRADED);

        // 5 consecutive failures → UNHEALTHY
        for (int i = AuthenticationManager.DEGRADED_THRESHOLD; i < AuthenticationManager.UNHEALTHY_THRESHOLD; i++) {
            manager.authenticate(request);
        }
        assertThat(manager.getHealthStatus("flaky")).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
    }

    @Test
    void successResetsHealthToHealthy() {
        AtomicInteger callCount = new AtomicInteger();
        Authenticator intermittent = new Authenticator() {
            @Override public String getName() { return "intermittent"; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                if (callCount.incrementAndGet() <= AuthenticationManager.DEGRADED_THRESHOLD) {
                    throw new RuntimeException("temporary failure");
                }
                return AuthenticationResult.success(new UserIdentity("user", Set.of(), Map.of(), Instant.now()));
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return true; }
            @Override public void close() {}
        };
        AuthenticationManager manager = new AuthenticationManager(List.of(intermittent));
        AuthenticationRequest request = new AuthenticationRequest(
                "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");

        // Drive to DEGRADED
        for (int i = 0; i < AuthenticationManager.DEGRADED_THRESHOLD; i++) {
            manager.authenticate(request);
        }
        assertThat(manager.getHealthStatus("intermittent")).isEqualTo(AuthenticatorHealthStatus.DEGRADED);

        // One success resets to HEALTHY
        manager.authenticate(request);
        assertThat(manager.getHealthStatus("intermittent")).isEqualTo(AuthenticatorHealthStatus.HEALTHY);
    }

    @Test
    void concurrentFailuresProduceConsistentHealthState() throws InterruptedException {
        Authenticator failing = createAlwaysFailingAuthenticator("concurrent-target");
        AuthenticationManager manager = new AuthenticationManager(List.of(failing));
        AuthenticationRequest request = new AuthenticationRequest(
                "user", "pass".toCharArray(), null, null, Map.of(), "127.0.0.1");

        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    manager.authenticate(request);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // After 20 concurrent failures, must be UNHEALTHY (>= 5 threshold)
        assertThat(manager.getHealthStatus("concurrent-target"))
                .isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);

        // Metrics snapshot must have consistent state
        AuthenticatorMetrics metrics = manager.getMetrics().get("concurrent-target");
        assertThat(metrics).isNotNull();
        assertThat(metrics.consecutiveFailures()).isGreaterThanOrEqualTo(AuthenticationManager.UNHEALTHY_THRESHOLD);
        assertThat(metrics.healthStatus()).isEqualTo(AuthenticatorHealthStatus.UNHEALTHY);
    }

    private static Authenticator createAlwaysFailingAuthenticator(String name) {
        return new Authenticator() {
            @Override public String getName() { return name; }
            @Override public void init(Map<String, String> config) {}
            @Override public AuthenticationResult authenticate(AuthenticationRequest r) {
                throw new RuntimeException("backend down");
            }
            @Override public boolean supports(AuthenticationRequest r) { return true; }
            @Override public boolean isHealthy() { return true; }
            @Override public void close() {}
        };
    }
}
