package org.pangrova.security.authn;

import java.time.Duration;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-IP brute force protection using a Caffeine cache as the attempt counter.
 *
 * <p>After {@code maxAttempts} failed authentication attempts within the lockout window,
 * subsequent attempts from the same IP are blocked until the window expires.
 * A successful authentication resets the counter for that IP immediately.
 *
 * <p>Thread-safe: Caffeine provides concurrent access guarantees internally.
 *
 * @ThreadSafe
 */
public final class BruteForceGuard {

    private static final Logger LOG = LoggerFactory.getLogger(BruteForceGuard.class);

    static final int DEFAULT_MAX_ATTEMPTS = 5;
    static final Duration DEFAULT_LOCKOUT_DURATION = Duration.ofMinutes(5);

    private final int maxAttempts;
    private final Duration lockoutDuration;
    private final Cache<String, Integer> attemptsByIp;

    /**
     * Creates a guard with default settings: 5 attempts, 5-minute lockout window.
     */
    public BruteForceGuard() {
        this(DEFAULT_MAX_ATTEMPTS, DEFAULT_LOCKOUT_DURATION);
    }

    /**
     * Creates a guard with custom settings.
     *
     * @param maxAttempts     maximum failed attempts before lockout (must be positive)
     * @param lockoutDuration duration of the lockout window (must be positive)
     * @throws IllegalArgumentException if maxAttempts is not positive or lockoutDuration is not positive
     * @throws NullPointerException     if lockoutDuration is null
     */
    public BruteForceGuard(int maxAttempts, Duration lockoutDuration) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive, got: " + maxAttempts);
        }
        Objects.requireNonNull(lockoutDuration, "lockoutDuration must not be null");
        if (lockoutDuration.isNegative() || lockoutDuration.isZero()) {
            throw new IllegalArgumentException("lockoutDuration must be positive, got: " + lockoutDuration);
        }
        this.maxAttempts = maxAttempts;
        this.lockoutDuration = lockoutDuration;
        this.attemptsByIp = Caffeine.newBuilder()
                .expireAfterWrite(lockoutDuration)
                .build();
    }

    /**
     * Checks whether the given IP is currently blocked due to excessive failed attempts.
     *
     * @param ip the client IP address
     * @return true if the IP is blocked, false if attempts are still allowed
     * @throws NullPointerException if ip is null
     */
    public boolean isBlocked(String ip) {
        Objects.requireNonNull(ip, "ip must not be null");
        Integer attempts = attemptsByIp.getIfPresent(ip);
        return attempts != null && attempts >= maxAttempts;
    }

    /**
     * Records a failed authentication attempt for the given IP.
     * If this pushes the IP over the threshold, subsequent calls to {@link #isBlocked(String)}
     * will return true until the lockout window expires.
     *
     * @param ip the client IP address
     * @throws NullPointerException if ip is null
     */
    public void recordFailedAttempt(String ip) {
        Objects.requireNonNull(ip, "ip must not be null");
        Integer current = attemptsByIp.getIfPresent(ip);
        int newCount = (current == null) ? 1 : current + 1;
        attemptsByIp.put(ip, newCount);

        if (newCount >= maxAttempts) {
            LOG.warn("event=brute_force_lockout ip={} attempts={} lockoutDuration={}",
                    ip, newCount, lockoutDuration);
        }
    }

    /**
     * Records a successful authentication, resetting the failure counter for the given IP.
     *
     * @param ip the client IP address
     * @throws NullPointerException if ip is null
     */
    public void recordSuccess(String ip) {
        Objects.requireNonNull(ip, "ip must not be null");
        attemptsByIp.invalidate(ip);
    }

    /**
     * Returns the current failure count for the given IP, or 0 if no failures are recorded.
     * Primarily for observability and testing.
     *
     * @param ip the client IP address
     * @return the current failure count
     */
    public int getFailureCount(String ip) {
        Objects.requireNonNull(ip, "ip must not be null");
        Integer attempts = attemptsByIp.getIfPresent(ip);
        return attempts != null ? attempts : 0;
    }

    /**
     * Returns the configured maximum attempts before lockout.
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the configured lockout duration.
     */
    public Duration lockoutDuration() {
        return lockoutDuration;
    }
}
