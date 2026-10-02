package org.pangrova.security.authn;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates authentication by delegating to the first matching Authenticator in order.
 *
 * <p>Authentication failures return a generic user-facing message to prevent
 * leaking backend implementation details (LDAP paths, connection strings, etc.).
 * Full exception details are logged to the controlled audit log at WARN level.
 *
 * <h3>Failure Observability</h3>
 * Tracks per-authenticator metrics: success/failure counts, latency, and health status.
 * All auth events are logged in structured key=value format.
 *
 * <p>Health is observable, never an authorization routing decision. A matching
 * authenticator remains authoritative even when its backend is unhealthy; failure
 * must not select a less restrictive authenticator.
 *
 * <h3>Periodic Health Check</h3>
 * Optionally runs periodic health checks against all authenticators to update their
 * health status proactively (rather than only on auth failure).
 *
 */
@ThreadSafe
public class AuthenticationManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationManager.class);

    static final String GENERIC_FAILURE_MESSAGE = "Authentication failed. Please check your credentials.";

    /** Consecutive failure threshold to transition from HEALTHY to DEGRADED. */
    static final int DEGRADED_THRESHOLD = 3;

    /** Consecutive failure threshold to transition from DEGRADED to UNHEALTHY. */
    static final int UNHEALTHY_THRESHOLD = 5;

    private final List<Authenticator> authenticators;
    private final ConcurrentHashMap<String, AuthenticatorState> stateByName;
    private final ScheduledExecutorService healthCheckScheduler;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Immutable snapshot of health state: consecutive failures + derived status.
     * Packed into a single object for atomic CAS updates, eliminating the TOCTOU
     * race between reading consecutiveFailures and healthStatus separately.
     */
    record HealthSnapshot(int consecutiveFailures, AuthenticatorHealthStatus status) {
        static HealthSnapshot healthy() {
            return new HealthSnapshot(0, AuthenticatorHealthStatus.HEALTHY);
        }

        HealthSnapshot withIncrementedFailure() {
            int newFailures = consecutiveFailures + 1;
            return new HealthSnapshot(newFailures, deriveHealthStatus(newFailures));
        }
    }

    /**
     * Mutable per-authenticator tracking state.
     * Counter fields use individual atomics; health state uses packed HealthSnapshot
     * in a single AtomicReference for atomic consecutiveFailures + status updates.
     */
    static final class AuthenticatorState {
        final AtomicLong successCount = new AtomicLong();
        final AtomicLong failureCount = new AtomicLong();
        final AtomicLong totalLatencyMs = new AtomicLong();
        final AtomicReference<HealthSnapshot> health =
                new AtomicReference<>(HealthSnapshot.healthy());

        AuthenticatorMetrics snapshot(String name) {
            HealthSnapshot h = health.get();
            return new AuthenticatorMetrics(
                    name,
                    successCount.get(),
                    failureCount.get(),
                    totalLatencyMs.get(),
                    h.status(),
                    h.consecutiveFailures()
            );
        }
    }

    /**
     * Creates an authentication manager with the given authenticator chain.
     * No periodic health check is started.
     *
     * @param authenticators ordered list of authenticators to try
     * @throws NullPointerException if authenticators is null
     */
    public AuthenticationManager(List<Authenticator> authenticators) {
        this(authenticators, 0, TimeUnit.SECONDS);
    }

    /**
     * Creates an authentication manager with the given authenticator chain
     * and optional periodic health check.
     *
     * @param authenticators      ordered list of authenticators to try
     * @param healthCheckInterval interval between health checks (0 to disable)
     * @param timeUnit            time unit for the interval
     * @throws NullPointerException     if authenticators or timeUnit is null
     * @throws IllegalArgumentException if healthCheckInterval is negative
     */
    public AuthenticationManager(List<Authenticator> authenticators,
                                 long healthCheckInterval,
                                 TimeUnit timeUnit) {
        Objects.requireNonNull(authenticators, "authenticators must not be null");
        Objects.requireNonNull(timeUnit, "timeUnit must not be null");
        if (healthCheckInterval < 0) {
            throw new IllegalArgumentException("healthCheckInterval must not be negative");
        }
        this.authenticators = List.copyOf(authenticators);
        this.stateByName = new ConcurrentHashMap<>();
        for (Authenticator auth : this.authenticators) {
            stateByName.putIfAbsent(auth.getName(), new AuthenticatorState());
        }

        if (healthCheckInterval > 0) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "auth-health-check");
                t.setDaemon(true);
                return t;
            });
            executor.setRemoveOnCancelPolicy(true);
            this.healthCheckScheduler = executor;
            this.healthCheckScheduler.scheduleAtFixedRate(
                    this::runHealthChecks, healthCheckInterval, healthCheckInterval, timeUnit);
        } else {
            this.healthCheckScheduler = null;
        }
    }

    /**
     * Iterates authenticators in order; delegates to the first that supports the request.
     * Health status never changes which matching authenticator is authoritative.
     *
     * @param request the authentication request
     * @return the authentication result
     * @throws NullPointerException if request is null
     */
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (closed.get()) {
            return AuthenticationResult.failure("Authentication manager is closed");
        }
        for (Authenticator auth : authenticators) {
            if (!auth.supports(request)) {
                continue;
            }
            return safeAuthenticate(auth, request);
        }
        return AuthenticationResult.failure("No authenticator supports this request");
    }

    private AuthenticationResult safeAuthenticate(Authenticator auth, AuthenticationRequest request) {
        AuthenticatorState state = stateByName.computeIfAbsent(auth.getName(), k -> new AuthenticatorState());
        long startNanos = System.nanoTime();
        try {
            AuthenticationResult result = auth.authenticate(request);
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            recordOutcome(state, auth.getName(), request.clientIp(), latencyMs, result);
            return result;
        } catch (Exception e) {
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            return handleAuthFailure(state, auth.getName(), request, latencyMs, e);
        }
    }

    private void recordOutcome(AuthenticatorState state, String authName,
                               String clientIp, long latencyMs, AuthenticationResult result) {
        state.totalLatencyMs.addAndGet(latencyMs);
        if (result.success()) {
            state.successCount.incrementAndGet();
            // CAS loop: atomically reset to HEALTHY snapshot
            HealthSnapshot current = state.health.get();
            if (current.consecutiveFailures() > 0 || current.status() != AuthenticatorHealthStatus.HEALTHY) {
                state.health.set(HealthSnapshot.healthy());
            }
            LOG.info("event=authentication_success authenticator={} clientIp={} latencyMs={}",
                    authName, clientIp, latencyMs);
        } else {
            state.failureCount.incrementAndGet();
            LOG.info("event=authentication_denied authenticator={} clientIp={} latencyMs={}",
                    authName, clientIp, latencyMs);
        }
    }

    private AuthenticationResult handleAuthFailure(AuthenticatorState state, String authName,
                                                   AuthenticationRequest request, long latencyMs, Exception e) {
        state.totalLatencyMs.addAndGet(latencyMs);
        recordFailure(state, authName, request.clientIp(), latencyMs);
        LOG.warn("event=authentication_failed authenticator={} clientIp={} username={} latencyMs={} error={}",
                authName, request.clientIp(), request.username(), latencyMs, e.getMessage(), e);
        return AuthenticationResult.failure(GENERIC_FAILURE_MESSAGE);
    }

    private void recordFailure(AuthenticatorState state, String authenticatorName,
                               String clientIp, long latencyMs) {
        state.failureCount.incrementAndGet();
        // CAS loop: atomically increment consecutiveFailures and derive new status
        HealthSnapshot oldSnap;
        HealthSnapshot newSnap;
        do {
            oldSnap = state.health.get();
            newSnap = oldSnap.withIncrementedFailure();
        } while (!state.health.compareAndSet(oldSnap, newSnap));

        if (oldSnap.status() != newSnap.status()) {
            LOG.warn("event=authenticator_health_changed authenticator={} oldStatus={} "
                    + "newStatus={} consecutiveFailures={} clientIp={}",
                    authenticatorName, oldSnap.status(), newSnap.status(), newSnap.consecutiveFailures(), clientIp);
        }
        LOG.info("event=authentication_failure authenticator={} clientIp={} latencyMs={} consecutiveFailures={}",
                authenticatorName, clientIp, latencyMs, newSnap.consecutiveFailures());
    }

    static AuthenticatorHealthStatus deriveHealthStatus(int consecutiveFailures) {
        if (consecutiveFailures >= UNHEALTHY_THRESHOLD) {
            return AuthenticatorHealthStatus.UNHEALTHY;
        }
        if (consecutiveFailures >= DEGRADED_THRESHOLD) {
            return AuthenticatorHealthStatus.DEGRADED;
        }
        return AuthenticatorHealthStatus.HEALTHY;
    }

    /**
     * Runs health checks against all authenticators and updates their status.
     * Called periodically by the health check scheduler, or can be invoked manually.
     */
    public void runHealthChecks() {
        if (closed.get()) {
            return;
        }
        for (Authenticator auth : authenticators) {
            AuthenticatorState state = stateByName.computeIfAbsent(auth.getName(), k -> new AuthenticatorState());
            checkSingleAuthenticator(auth, state);
        }
    }

    private void checkSingleAuthenticator(Authenticator auth, AuthenticatorState state) {
        try {
            boolean healthy = auth.isHealthy();
            updateHealthState(auth, state, healthy);
        } catch (Exception e) {
            HealthSnapshot oldSnap;
            HealthSnapshot newSnap;
            do {
                oldSnap = state.health.get();
                newSnap = oldSnap.withIncrementedFailure();
            } while (!state.health.compareAndSet(oldSnap, newSnap));
            LOG.warn("event=authenticator_health_check_error authenticator={} error={}",
                    auth.getName(), e.getMessage());
        }
    }

    private void updateHealthState(Authenticator auth, AuthenticatorState state, boolean healthy) {
        if (healthy) {
            HealthSnapshot oldSnap = state.health.get();
            if (oldSnap.status() != AuthenticatorHealthStatus.HEALTHY) {
                state.health.set(HealthSnapshot.healthy());
                LOG.info("event=authenticator_health_recovered authenticator={} oldStatus={}",
                        auth.getName(), oldSnap.status());
            }
        } else {
            HealthSnapshot oldSnap;
            HealthSnapshot newSnap;
            do {
                oldSnap = state.health.get();
                newSnap = oldSnap.withIncrementedFailure();
            } while (!state.health.compareAndSet(oldSnap, newSnap));
            if (oldSnap.status() != newSnap.status()) {
                LOG.warn("event=authenticator_health_changed authenticator={} oldStatus={} newStatus={} "
                                + "consecutiveFailures={}",
                        auth.getName(), oldSnap.status(), newSnap.status(), newSnap.consecutiveFailures());
            }
        }
    }

    /**
     * Returns an immutable snapshot of metrics for all authenticators.
     *
     * @return map of authenticator name to metrics snapshot
     */
    public Map<String, AuthenticatorMetrics> getMetrics() {
        return stateByName.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().snapshot(e.getKey())
                ));
    }

    /**
     * Returns the current health status of a specific authenticator.
     *
     * @param authenticatorName the authenticator name
     * @return the health status, or HEALTHY if the authenticator is unknown
     */
    public AuthenticatorHealthStatus getHealthStatus(String authenticatorName) {
        AuthenticatorState state = stateByName.get(authenticatorName);
        return state != null ? state.health.get().status() : AuthenticatorHealthStatus.HEALTHY;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (healthCheckScheduler != null) {
            healthCheckScheduler.shutdownNow();
        }
        RuntimeException failure = null;
        for (Authenticator authenticator : authenticators) {
            try {
                authenticator.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
