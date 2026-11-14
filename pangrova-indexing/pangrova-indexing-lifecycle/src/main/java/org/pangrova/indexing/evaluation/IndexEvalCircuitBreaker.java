package org.pangrova.indexing.evaluation;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-index circuit breaker for evaluation failures.
 *
 * <p>Prevents a consistently failing index from slowing down every query.
 * States: CLOSED (normal) → OPEN (failing) → HALF_OPEN (testing recovery).</p>
 *
 * <p>Supports an optional {@link BreakerStateStore} for distributed state.
 * Shared state is cached locally with a 10-second TTL to avoid excessive
 * DB reads on the hot query path. Local breaker state is always checked
 * first (fast path); shared state acts as a secondary gate.</p>
 *
 * <p>Thread-safe. Uses ConcurrentHashMap with immutable state snapshots
 * and atomic {@code compute} operations.</p>
 */
@ThreadSafe
public final class IndexEvalCircuitBreaker {

    private static final Logger LOG = LoggerFactory.getLogger(IndexEvalCircuitBreaker.class);
    private static final long DEFAULT_SHARED_CACHE_TTL_MS = 10_000L;

    private final int failureThreshold;
    private final Duration openDuration;
    private final BreakerStateStore sharedStore;
    private final long sharedCacheTtlMs;
    private final ConcurrentHashMap<String, BreakerState> breakers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedSharedState> sharedCache = new ConcurrentHashMap<>();

    /** Backward-compatible constructor — no shared store. */
    public IndexEvalCircuitBreaker(int failureThreshold, Duration openDuration) {
        this(failureThreshold, openDuration, BreakerStateStore.noOp());
    }

    /**
     * Creates a circuit breaker with distributed shared state and default cache TTL.
     */
    public IndexEvalCircuitBreaker(int failureThreshold, Duration openDuration,
                                   BreakerStateStore sharedStore) {
        this(failureThreshold, openDuration, sharedStore, DEFAULT_SHARED_CACHE_TTL_MS);
    }

    /**
     * Creates a circuit breaker with distributed shared state and configurable cache TTL.
     *
     * @param failureThreshold minimum failures before opening the circuit
     * @param openDuration     how long the circuit stays open
     * @param sharedStore      distributed state store (use {@link BreakerStateStore#noOp()} for local-only)
     * @param sharedCacheTtlMs TTL for locally cached shared state in milliseconds
     */
    public IndexEvalCircuitBreaker(int failureThreshold, Duration openDuration,
                                   BreakerStateStore sharedStore, long sharedCacheTtlMs) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException(
                    "failureThreshold must be >= 1, got " + failureThreshold);
        }
        if (openDuration == null || openDuration.isNegative() || openDuration.isZero()) {
            throw new IllegalArgumentException("openDuration must be positive");
        }
        if (sharedStore == null) {
            throw new IllegalArgumentException("sharedStore must not be null");
        }
        this.failureThreshold = failureThreshold;
        this.openDuration = openDuration;
        this.sharedStore = sharedStore;
        this.sharedCacheTtlMs = sharedCacheTtlMs;
    }

    /**
     * Returns true if evaluation is permitted for this index.
     *
     * <p>Checks local breaker first (fast path), then shared state with TTL cache.</p>
     *
     * <p>P2-BUG-3 fix: HALF_OPEN probe check and increment are now atomic
     * via {@code compute()}. Previously two concurrent threads could both
     * see {@code halfOpenProbes=0} and both pass, allowing 2 probes instead of 1.</p>
     */
    public boolean isAllowed(String indexId) {
        if (!isLocallyAllowed(indexId)) {
            return false;
        }
        return isSharedAllowed(indexId);
    }

    private boolean isLocallyAllowed(String indexId) {
        BreakerState localState = breakers.get(indexId);
        if (localState == null) {
            return true;
        }
        boolean[] allowed = {true};
        breakers.computeIfPresent(indexId, (k, s) -> {
            if (!s.isAllowed()) {
                allowed[0] = false;
                return s;
            }
            if (s.openUntilEpochMs() > 0
                    && System.currentTimeMillis() > s.openUntilEpochMs()) {
                return s.withProbe();
            }
            return s;
        });
        return allowed[0];
    }

    private boolean isSharedAllowed(String indexId) {
        if (sharedStore == BreakerStateStore.NO_OP) {
            return true;
        }
        CachedSharedState cached = sharedCache.get(indexId);
        long now = System.currentTimeMillis();
        if (cached == null || now - cached.fetchedAtMs() > sharedCacheTtlMs) {
            return refreshSharedState(indexId, now);
        }
        return cached.state().openUntilEpochMs() <= now;
    }

    private boolean refreshSharedState(String indexId, long now) {
        try {
            Optional<SharedBreakerState> shared = sharedStore.getState(indexId);
            shared.ifPresent(s -> sharedCache.put(indexId,
                    new CachedSharedState(s, System.currentTimeMillis())));
            return shared.isEmpty() || shared.get().openUntilEpochMs() <= now;
        } catch (Exception e) {
            LOG.debug("action=shared_breaker_read_failed indexId={} error={}",
                    indexId, e.getMessage());
            return true;
        }
    }

    /** Records a successful evaluation, resetting the breaker to CLOSED. */
    public void recordSuccess(String indexId) {
        breakers.computeIfPresent(indexId, (k, s) -> s.withReset());
        if (sharedStore != BreakerStateStore.NO_OP) {
            try {
                sharedStore.recordSuccess(indexId);
            } catch (Exception e) {
                LOG.debug("action=shared_breaker_success_failed indexId={} error={}",
                        indexId, e.getMessage());
            }
        }
    }

    /** Records a failed evaluation, potentially opening the circuit. */
    public void recordFailure(String indexId) {
        breakers.compute(indexId, (k, s) ->
                s == null
                        ? new BreakerState(1, 0, failureThreshold, openDuration).maybeOpen()
                        : s.withFailure().maybeOpen());
        if (sharedStore != BreakerStateStore.NO_OP) {
            try {
                sharedStore.recordFailure(indexId, Instant.now());
            } catch (Exception e) {
                LOG.debug("action=shared_breaker_failure_failed indexId={} error={}",
                        indexId, e.getMessage());
            }
        }
    }

    /** Locally cached shared state with fetch timestamp for TTL expiry. */
    private record CachedSharedState(SharedBreakerState state, long fetchedAtMs) {}

    /**
     * Immutable breaker state snapshot. Each mutation returns a new instance.
     * Stored in ConcurrentHashMap via compute/computeIfPresent for atomicity.
     */
    record BreakerState(
            int failureCount,
            long openUntilEpochMs,
            int threshold,
            Duration openDuration,
            int halfOpenProbes) {

        BreakerState(int failureCount, long openUntilEpochMs,
                     int threshold, Duration openDuration) {
            this(failureCount, openUntilEpochMs, threshold, openDuration, 0);
        }

        boolean isAllowed() {
            if (openUntilEpochMs == 0) {
                return true; // CLOSED
            }
            // HALF_OPEN if open period expired — allow only 1 probe
            if (System.currentTimeMillis() > openUntilEpochMs) {
                return halfOpenProbes < 1;
            }
            return false; // OPEN
        }

        BreakerState withFailure() {
            return new BreakerState(failureCount + 1, openUntilEpochMs,
                    threshold, openDuration, halfOpenProbes);
        }

        BreakerState maybeOpen() {
            if (failureCount >= threshold) {
                long until = System.currentTimeMillis() + openDuration.toMillis();
                return new BreakerState(failureCount, until, threshold, openDuration, 0);
            }
            return this;
        }

        BreakerState withReset() {
            return new BreakerState(0, 0, threshold, openDuration, 0);
        }

        BreakerState withProbe() {
            return new BreakerState(failureCount, openUntilEpochMs,
                    threshold, openDuration, halfOpenProbes + 1);
        }
    }
}
