package org.pangrova.resilience.breaker;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Thread-safe circuit breaker with count-based sliding window failure tracking.
 *
 * <p>Implements {@link CircuitBreakerPolicy} SPI so that upstream modules
 * (e.g., coordinator) can depend on the stable contract instead of this
 * concrete class. The {@code target} parameter in SPI methods is intentionally
 * ignored — this breaker tracks a single global failure window. Per-target
 * tracking can be layered externally if needed.</p>
 *
 * <p>The sliding window uses a fixed-size ring buffer that only considers the
 * most recent N calls when computing failure rate. Old calls naturally fall out
 * of the window as new calls arrive, ensuring the breaker responds to recent
 * system health rather than cumulative history.</p>
 *
 * <p>Concurrency strategy:
 * <ul>
 *   <li>State + openedAt: Encapsulated in immutable {@link CircuitBreakerState} snapshot,
 *       atomically updated via single {@link AtomicReference}</li>
 *   <li>Half-open probes/successes: Reset atomically with state CAS via snapshot</li>
 *   <li>Sliding window: Uses synchronized ring buffer for consistent failure rate computation</li>
 * </ul>
 * </p>
 */
@ThreadSafe
public class CircuitBreaker implements CircuitBreakerPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State { CLOSED, OPEN, HALF_OPEN }

    /**
     * Immutable snapshot of circuit breaker state.
     * Encapsulates state + openedAt + halfOpenProbes + halfOpenSuccesses for atomic updates.
     */
    record CircuitBreakerState(
            State state,
            Instant openedAt,
            int halfOpenProbes,
            int halfOpenSuccesses
    ) {
        static CircuitBreakerState closed() {
            return new CircuitBreakerState(State.CLOSED, Instant.MIN, 0, 0);
        }

        static CircuitBreakerState open(Instant openedAt) {
            return new CircuitBreakerState(State.OPEN, openedAt, 0, 0);
        }

        CircuitBreakerState toHalfOpen() {
            return new CircuitBreakerState(State.HALF_OPEN, openedAt, 0, 0);
        }

        CircuitBreakerState withIncrementedProbes() {
            return new CircuitBreakerState(state, openedAt, halfOpenProbes + 1, halfOpenSuccesses);
        }

        CircuitBreakerState withIncrementedSuccesses() {
            return new CircuitBreakerState(state, openedAt, halfOpenProbes, halfOpenSuccesses + 1);
        }
    }

    private final double failureRateThreshold;
    private final int minimumCalls;
    private final long waitDurationMs;
    private final int halfOpenMaxCalls;
    private final Clock clock;
    private final MetricsProvider metricsProvider;
    private final LongCounter stateTransitionCounter;
    private final LongCounter requestAllowedCounter;
    private final LongCounter requestRejectedCounter;

    private final AtomicReference<CircuitBreakerState> stateRef =
            new AtomicReference<>(CircuitBreakerState.closed());
    private final SlidingWindowCounter counter;

    public CircuitBreaker() {
        this(CircuitBreakerConfig.defaults());
    }

    /** Primary constructor accepting a configuration parameter object. */
    public CircuitBreaker(CircuitBreakerConfig config) {
        this(config, null);
    }

    /** Constructor with metrics support. */
    public CircuitBreaker(CircuitBreakerConfig config, MetricsProvider metricsProvider) {
        this.failureRateThreshold = config.failureRateThreshold();
        this.minimumCalls = config.minimumCalls();
        this.waitDurationMs = config.waitDurationMs();
        this.halfOpenMaxCalls = config.halfOpenMaxCalls();
        this.clock = config.clock();
        this.counter = new SlidingWindowCounter(config.slidingWindowSize());
        this.metricsProvider = metricsProvider;
        if (metricsProvider != null) {
            this.stateTransitionCounter = metricsProvider.createCounter(
                    "pangrova.resilience.circuit_breaker.transitions",
                    "Circuit breaker state transitions", "{transition}");
            this.requestAllowedCounter = metricsProvider.createCounter(
                    "pangrova.resilience.circuit_breaker.requests.allowed",
                    "Requests allowed by circuit breaker", "{request}");
            this.requestRejectedCounter = metricsProvider.createCounter(
                    "pangrova.resilience.circuit_breaker.requests.rejected",
                    "Requests rejected by circuit breaker", "{request}");
        } else {
            this.stateTransitionCounter = null;
            this.requestAllowedCounter = null;
            this.requestRejectedCounter = null;
        }
    }

    /**
     * Convenience constructor for callers passing individual config values.
     * Delegates to the primary constructor via CircuitBreakerConfig.
     */
    public CircuitBreaker(double failureRateThreshold, int minimumCalls,
                          long waitDurationMs, int halfOpenMaxCalls, Clock clock) {
        this(new CircuitBreakerConfig(failureRateThreshold, minimumCalls,
                waitDurationMs, halfOpenMaxCalls, clock));
    }

    /** Returns true if the request is allowed under the current breaker state. */
    public boolean allowRequest() {
        CircuitBreakerState current = stateRef.get();
        boolean allowed = switch (current.state()) {
            case CLOSED -> true;
            case OPEN -> tryTransitionToHalfOpen();
            case HALF_OPEN -> tryIncrementHalfOpenProbes();
        };
        if (allowed) {
            recordMetric(requestAllowedCounter);
        } else {
            recordMetric(requestRejectedCounter);
        }
        return allowed;
    }

    // BUG-R02 (P2 observation): onSuccess()/onFailure() read state once at the top.
    // A concurrent state transition between the read and the switch body is benign:
    // - handleHalfOpenSuccess() uses CAS retry, so a lost success is retried.
    // - handleClosedFailure() uses counter snapshot, so stale reads are safe.
    // - transitionToOpen() uses CAS, so concurrent attempts are idempotent.
    // No code change needed — the CAS-retry pattern already handles this correctly.

    /** Records a successful call outcome, possibly closing the breaker. */
    public void onSuccess() {
        CircuitBreakerState current = stateRef.get();
        switch (current.state()) {
            case CLOSED -> counter.recordSuccess();
            case HALF_OPEN -> handleHalfOpenSuccess();
            case OPEN -> { /* ignore success in OPEN state */ }
            default -> throw new IllegalStateException("Unhandled breaker state: " + current.state());
        }
    }

    /** Records a failed call outcome, possibly opening the breaker. */
    public void onFailure() {
        CircuitBreakerState current = stateRef.get();
        switch (current.state()) {
            case CLOSED -> handleClosedFailure();
            case HALF_OPEN -> transitionToOpen();
            case OPEN -> { /* ignore failure in OPEN state */ }
            default -> throw new IllegalStateException("Unhandled breaker state: " + current.state());
        }
    }

    // -- CircuitBreakerPolicy SPI --

    @Override
    public boolean allowRequest(String target) {
        return allowRequest();
    }

    @Override
    public void recordSuccess(String target) {
        onSuccess();
    }

    @Override
    public void recordFailure(String target, Throwable cause) {
        onFailure();
    }

    public State getState() {
        return stateRef.get().state();
    }

    /**
     * Returns the timestamp when the circuit breaker entered OPEN state.
     * Returns {@link Instant#MIN} if never opened.
     */
    public Instant getOpenedAt() {
        return stateRef.get().openedAt();
    }

    /**
     * Attempts to transition from OPEN to HALF_OPEN if wait duration has elapsed.
     * Uses CAS to ensure atomic state transition with reset of half-open counters.
     *
     * @return true if request is allowed (transition succeeded or already in HALF_OPEN with capacity)
     */
    private boolean tryTransitionToHalfOpen() {
        while (true) {
            CircuitBreakerState current = stateRef.get();
            if (current.state() != State.OPEN) {
                // State changed by another thread, re-evaluate
                return current.state() == State.CLOSED
                        || (current.state() == State.HALF_OPEN && current.halfOpenProbes() < halfOpenMaxCalls);
            }

            long elapsed = clock.instant().toEpochMilli() - current.openedAt().toEpochMilli();
            if (elapsed < waitDurationMs) {
                return false;
            }

            // Atomically transition to HALF_OPEN with reset counters and first probe
            CircuitBreakerState halfOpen = current.toHalfOpen().withIncrementedProbes();
            if (stateRef.compareAndSet(current, halfOpen)) {
                LOG.info("circuit_breaker_transition state=HALF_OPEN previous=OPEN");
                recordTransition("HALF_OPEN");
                return true;
            }
            // CAS failed, retry
        }
    }

    /**
     * Attempts to increment half-open probes counter atomically.
     *
     * @return true if probe is allowed (under max calls limit)
     */
    private boolean tryIncrementHalfOpenProbes() {
        while (true) {
            CircuitBreakerState current = stateRef.get();
            if (current.state() != State.HALF_OPEN) {
                // State changed, re-evaluate
                return current.state() == State.CLOSED;
            }

            if (current.halfOpenProbes() >= halfOpenMaxCalls) {
                return false;
            }

            CircuitBreakerState updated = current.withIncrementedProbes();
            if (stateRef.compareAndSet(current, updated)) {
                return true;
            }
            // CAS failed, retry
        }
    }

    /**
     * Handles success in HALF_OPEN state.
     * Uses CAS to ensure only one thread triggers HALF_OPEN→CLOSED transition.
     */
    private void handleHalfOpenSuccess() {
        while (true) {
            CircuitBreakerState current = stateRef.get();
            if (current.state() != State.HALF_OPEN) {
                return;
            }
            if (tryTransitionFromHalfOpen(current)) {
                return;
            }
            // CAS failed, retry
        }
    }

    private boolean tryTransitionFromHalfOpen(CircuitBreakerState current) {
        int newSuccesses = current.halfOpenSuccesses() + 1;
        if (newSuccesses >= halfOpenMaxCalls) {
            // BUG-R03: Reset counter BEFORE CAS to CLOSED. This prevents a concurrent
            // onFailure() from reading stale pre-reset counter values after seeing CLOSED.
            counter.reset();
            CircuitBreakerState closed = CircuitBreakerState.closed();
            if (stateRef.compareAndSet(current, closed)) {
                LOG.info("circuit_breaker_transition state=CLOSED previous=HALF_OPEN");
                recordTransition("CLOSED");
                return true;
            }
            // CAS failed — another thread changed state; counter reset is harmless
            // since we're no longer transitioning to CLOSED.
            return false;
        }
        return stateRef.compareAndSet(current, current.withIncrementedSuccesses());
    }

    private void handleClosedFailure() {
        counter.recordFailure();
        SlidingWindowCounter.Snapshot snap = counter.snapshot();
        if (snap.totalCalls() < minimumCalls) {
            return;
        }
        if (snap.failureRate() >= failureRateThreshold) {
            transitionToOpen();
        }
    }

    /**
     * Transitions to OPEN state atomically.
     * Multiple threads may attempt this; only one will succeed.
     */
    private void transitionToOpen() {
        while (true) {
            CircuitBreakerState current = stateRef.get();
            if (current.state() == State.OPEN) {
                return;
            }

            State previous = current.state();
            CircuitBreakerState open = CircuitBreakerState.open(clock.instant());
            if (stateRef.compareAndSet(current, open)) {
                LOG.info("circuit_breaker_transition state=OPEN previous={}", previous);
                recordTransition("OPEN");
                return;
            }
        }
    }

    private void recordTransition(String toState) {
        if (stateTransitionCounter != null) {
            stateTransitionCounter.add(1, MetricAttributes.of("to_state", toState));
        }
    }

    private void recordMetric(LongCounter metric) {
        if (metric != null) {
            metric.add(1);
        }
    }

    /**
     * Count-based sliding window failure counter using a fixed-size ring buffer.
     *
     * <p>Only the most recent {@code windowSize} calls are considered when computing
     * failure rate. Old calls naturally fall out of the window as new calls arrive,
     * ensuring the breaker responds to recent system health rather than cumulative history.</p>
     *
     * <p>Thread safety: all mutations are synchronized on the instance. Reads via
     * {@link #snapshot()} are also synchronized to ensure a consistent view.</p>
     */
    @ThreadSafe
    static final class SlidingWindowCounter {

        private static final byte SUCCESS = 0;
        private static final byte FAILURE = 1;

        private final byte[] ring;
        private final int windowSize;
        private int head;
        private int count;
        private int failures;

        /**
         * Immutable point-in-time snapshot of the sliding window state.
         */
        record Snapshot(int totalCalls, int failures) {
            double failureRate() {
                return totalCalls == 0 ? 0.0 : (double) failures / totalCalls;
            }
        }

        SlidingWindowCounter(int windowSize) {
            if (windowSize <= 0) {
                throw new IllegalArgumentException("windowSize must be positive: " + windowSize);
            }
            this.windowSize = windowSize;
            this.ring = new byte[windowSize];
            this.head = 0;
            this.count = 0;
            this.failures = 0;
        }

        synchronized void recordSuccess() {
            record(SUCCESS);
        }

        synchronized void recordFailure() {
            record(FAILURE);
        }

        /**
         * Returns a consistent snapshot of the current sliding window state.
         */
        synchronized Snapshot snapshot() {
            return new Snapshot(count, failures);
        }

        /**
         * Resets the sliding window, clearing all recorded calls.
         */
        synchronized void reset() {
            head = 0;
            count = 0;
            failures = 0;
        }

        private void record(byte outcome) {
            if (count < windowSize) {
                ring[head] = outcome;
                head = (head + 1) % windowSize;
                count++;
                if (outcome == FAILURE) {
                    failures++;
                }
            } else {
                byte evicted = ring[head];
                if (evicted == FAILURE) {
                    failures--;
                }
                ring[head] = outcome;
                head = (head + 1) % windowSize;
                if (outcome == FAILURE) {
                    failures++;
                }
            }
        }

        @org.pangrova.common.VisibleForTesting
        synchronized int totalCalls() {
            return count;
        }

        @org.pangrova.common.VisibleForTesting
        synchronized int getFailures() {
            return failures;
        }

        @org.pangrova.common.VisibleForTesting
        synchronized int getSuccesses() {
            return count - failures;
        }
    }
}
