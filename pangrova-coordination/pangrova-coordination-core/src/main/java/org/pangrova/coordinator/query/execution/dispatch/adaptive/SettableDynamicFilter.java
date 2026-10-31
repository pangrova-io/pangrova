package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import org.pangrova.connector.spi.metadata.DynamicFilter;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable, thread-safe implementation of {@link DynamicFilter} that supports
 * the full lifecycle state machine (PENDING → COLLECTING → READY → APPLIED → COMPLETED).
 *
 * <p>The coordinator creates one instance per planned dynamic filter and updates
 * it as the build-side stage progresses. Probe-side connectors observe the
 * filter through the {@link DynamicFilter} SPI contract.</p>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 *   // Coordinator creates during query setup
 *   SettableDynamicFilter filter = SettableDynamicFilter.create("order_date", true);
 *
 *   // Build-side stage starts
 *   filter.transitionTo(State.COLLECTING);
 *
 *   // Build-side stage completes — set the filter data
 *   filter.setMinMax(minValue, maxValue);  // automatically transitions to READY
 *
 *   // After propagation to probe-side tasks
 *   filter.transitionTo(State.APPLIED);
 *
 *   // Query completes
 *   filter.transitionTo(State.COMPLETED);
 *
 *   // Or on failure/cancellation
 *   filter.cancel();
 * }</pre>
 *
 * <h3>Thread safety</h3>
 * <p>All state transitions use CAS operations on an {@link AtomicReference}.
 * The blocking future is replaced atomically. Listeners are stored in a
 * {@link CopyOnWriteArrayList} for safe concurrent iteration.</p>
 */
public final class SettableDynamicFilter implements DynamicFilter {

    /**
     * Lifecycle states for a dynamic filter.
     */
    public enum State {
        PENDING, COLLECTING, READY, APPLIED, COMPLETED, CANCELLED
    }

    private final String column;
    private final boolean awaitable;
    private final AtomicReference<State> state;
    private final AtomicReference<CompletableFuture<Void>> blocked;
    private final List<Runnable> readyListeners = new CopyOnWriteArrayList<>();

    // Filter data — set once when transitioning to READY
    private volatile Type filterType = Type.MIN_MAX;
    private volatile Comparable<?> minValue;
    private volatile Comparable<?> maxValue;
    private volatile FilterPredicate predicate;

    /**
     * Creates a new settable dynamic filter in {@link State#PENDING}.
     *
     * @param column    the target column name for this filter
     * @param awaitable whether scan operators should block waiting for this filter
     * @return a new filter in PENDING state
     */
    public static SettableDynamicFilter create(String column, boolean awaitable) {
        return new SettableDynamicFilter(column, awaitable);
    }

    private SettableDynamicFilter(String column, boolean awaitable) {
        this.column = Objects.requireNonNull(column, "column must not be null");
        this.awaitable = awaitable;
        this.state = new AtomicReference<>(State.PENDING);
        this.blocked = new AtomicReference<>(new CompletableFuture<>());
    }

    // ---- State transitions (coordinator side) ----

    /**
     * Transitions the filter to the specified state.
     *
     * <p>Valid transitions:</p>
     * <ul>
     *   <li>PENDING → COLLECTING</li>
     *   <li>COLLECTING → READY (prefer {@link #setMinMax} or {@link #setBloom})</li>
     *   <li>READY → APPLIED</li>
     *   <li>APPLIED → COMPLETED</li>
     *   <li>Any → CANCELLED (use {@link #cancel()})</li>
     * </ul>
     *
     * @param newState the target state
     * @throws IllegalStateException if the transition is invalid
     */
    public void transitionTo(State newState) {
        State current = state.get();
        if (current == State.CANCELLED || current == State.COMPLETED) {
            return; // terminal states — silently ignore
        }
        if (!isValidTransition(current, newState)) {
            throw new IllegalStateException(
                    "Invalid state transition: " + current + " → " + newState);
        }
        state.set(newState);
        if (newState == State.READY) {
            completeBlockedFuture();
            fireReadyListeners();
        }
    }

    /**
     * Sets min/max filter data and transitions to {@link State#READY}.
     *
     * @param min the minimum value from build-side data
     * @param max the maximum value from build-side data
     * @throws IllegalStateException if not in PENDING or COLLECTING state
     */
    public void setMinMax(Comparable<?> min, Comparable<?> max) {
        requireMutableState();
        this.filterType = Type.MIN_MAX;
        this.minValue = min;
        this.maxValue = max;
        this.predicate = new MinMaxPredicate(min, max);
        state.set(State.READY);
        completeBlockedFuture();
        fireReadyListeners();
    }

    /**
     * Sets a bloom-filter-backed predicate and transitions to {@link State#READY}.
     *
     * @param bloomPredicate a predicate that tests membership via bloom filter
     * @throws IllegalStateException if not in PENDING or COLLECTING state
     */
    public void setBloom(FilterPredicate bloomPredicate) {
        requireMutableState();
        this.filterType = Type.BLOOM;
        this.predicate = Objects.requireNonNull(bloomPredicate);
        state.set(State.READY);
        completeBlockedFuture();
        fireReadyListeners();
    }

    /**
     * Configures a timeout: if the filter is not ready within the given duration,
     * the blocked future completes (allowing scans to proceed without the filter).
     *
     * @param timeout   maximum wait duration
     * @param scheduler executor for scheduling the timeout
     */
    public void setTimeout(Duration timeout, ScheduledExecutorService scheduler) {
        CompletableFuture<Void> future = blocked.get();
        if (future.isDone()) {
            return;
        }
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ---- DynamicFilter SPI implementation ----

    @Override
    public Type filterType() {
        return filterType;
    }

    @Override
    public String column() {
        return column;
    }

    @Override
    public Optional<Comparable<?>> min() {
        return Optional.ofNullable(minValue);
    }

    @Override
    public Optional<Comparable<?>> max() {
        return Optional.ofNullable(maxValue);
    }

    @Override
    public boolean mightContain(Object value) {
        State current = state.get();
        if (current == State.CANCELLED || current == State.PENDING
                || current == State.COLLECTING) {
            return true; // no filter data yet — accept all
        }
        if (predicate == null) {
            return true;
        }
        return predicate.test(value);
    }

    public State state() {
        return state.get();
    }

    public boolean isComplete() {
        State current = state.get();
        return current == State.READY || current == State.APPLIED
                || current == State.COMPLETED;
    }

    public CompletableFuture<?> isBlocked() {
        return blocked.get();
    }

    public boolean isAwaitable() {
        return awaitable;
    }

    /** Invokes a listener when the filter reaches a terminal state. */
    public void onReady(Runnable listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        if (isComplete()) {
            listener.run();
        } else {
            readyListeners.add(listener);
            // Double-check after add in case state changed concurrently
            if (isComplete()) {
                fireReadyListeners();
            }
        }
    }

    /** Cancels the pending filter and notifies listeners. */
    public void cancel() {
        State previous = state.getAndSet(State.CANCELLED);
        if (previous != State.CANCELLED && previous != State.COMPLETED) {
            completeBlockedFuture();
        }
    }

    @Override
    public String toString() {
        return "SettableDynamicFilter[column=" + column + ", state=" + state.get()
                + ", type=" + filterType + ", awaitable=" + awaitable + "]";
    }

    // ---- Internal helpers ----

    private void requireMutableState() {
        State current = state.get();
        if (current != State.PENDING && current != State.COLLECTING) {
            throw new IllegalStateException(
                    "Cannot set filter data in state: " + current);
        }
    }

    private void completeBlockedFuture() {
        CompletableFuture<Void> future = blocked.get();
        future.complete(null);
    }

    private void fireReadyListeners() {
        for (Runnable listener : readyListeners) {
            try {
                listener.run();
            } catch (Exception e) {
                // Swallow listener exceptions — lifecycle must not be interrupted
            }
        }
        readyListeners.clear();
    }

    private static boolean isValidTransition(State from, State to) {
        if (to == State.CANCELLED) {
            return true; // any state can transition to CANCELLED
        }
        return switch (from) {
            case PENDING -> to == State.COLLECTING || to == State.READY;
            case COLLECTING -> to == State.READY;
            case READY -> to == State.APPLIED;
            case APPLIED -> to == State.COMPLETED;
            default -> false;
        };
    }

    /**
     * Predicate for evaluating values against collected filter data.
     * Implementations must be thread-safe.
     */
    @FunctionalInterface
    public interface FilterPredicate {
        /**
         * Tests whether a value might pass the filter.
         *
         * @param value the value to test (may be null)
         * @return true if the value might match, false if definitely excluded
         */
        boolean test(Object value);
    }

    /**
     * Min/max range predicate for MIN_MAX filter type.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private record MinMaxPredicate(Comparable min, Comparable max) implements FilterPredicate {
        @Override
        public boolean test(Object value) {
            if (value == null || min == null || max == null) {
                return true;
            }
            try {
                Comparable cv = (Comparable) value;
                return cv.compareTo(min) >= 0 && cv.compareTo(max) <= 0;
            } catch (ClassCastException e) {
                return true;
            }
        }
    }
}
