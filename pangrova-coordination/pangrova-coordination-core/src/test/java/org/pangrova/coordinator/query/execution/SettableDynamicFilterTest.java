package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.execution.dispatch.adaptive.SettableDynamicFilter;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SettableDynamicFilter} lifecycle state machine,
 * blocking behavior, and thread-safety.
 */
class SettableDynamicFilterTest {

    // ---- Initial state ----

    @Test
    @DisplayName("New filter starts in PENDING state")
    void create_startsPending() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.PENDING);
    }

    @Test
    @DisplayName("New filter is not complete")
    void create_isNotComplete() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        assertThat(filter.isComplete()).isFalse();
    }

    @Test
    @DisplayName("New filter accepts all values (no data yet)")
    void create_acceptsAllValues() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        assertThat(filter.mightContain("anything")).isTrue();
        assertThat(filter.mightContain(null)).isTrue();
    }

    @Test
    @DisplayName("Awaitable flag is respected")
    void create_awaitableFlag() {
        SettableDynamicFilter awaitable = SettableDynamicFilter.create("col", true);
        SettableDynamicFilter notAwaitable = SettableDynamicFilter.create("col", false);
        assertThat(awaitable.isAwaitable()).isTrue();
        assertThat(notAwaitable.isAwaitable()).isFalse();
    }

    // ---- State transitions ----

    @Test
    @DisplayName("PENDING -> COLLECTING is valid")
    void transition_pendingToCollecting() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.transitionTo(SettableDynamicFilter.State.COLLECTING);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.COLLECTING);
        assertThat(filter.isComplete()).isFalse();
    }

    @Test
    @DisplayName("COLLECTING -> READY via setMinMax")
    void transition_collectingToReady_viaSetMinMax() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.transitionTo(SettableDynamicFilter.State.COLLECTING);
        filter.setMinMax(10, 100);

        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.READY);
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.filterType()).isEqualTo(DynamicFilter.Type.MIN_MAX);
        assertThat(filter.min()).hasValue(10);
        assertThat(filter.max()).hasValue(100);
    }

    @Test
    @DisplayName("PENDING -> READY via setMinMax (skip COLLECTING)")
    void transition_pendingToReady_viaSetMinMax() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(5, 50);

        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.READY);
        assertThat(filter.isComplete()).isTrue();
    }

    @Test
    @DisplayName("READY -> APPLIED is valid")
    void transition_readyToApplied() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(1, 10);
        filter.transitionTo(SettableDynamicFilter.State.APPLIED);

        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.APPLIED);
        assertThat(filter.isComplete()).isTrue();
    }

    @Test
    @DisplayName("APPLIED -> COMPLETED is valid")
    void transition_appliedToCompleted() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(1, 10);
        filter.transitionTo(SettableDynamicFilter.State.APPLIED);
        filter.transitionTo(SettableDynamicFilter.State.COMPLETED);

        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.COMPLETED);
    }

    @Test
    @DisplayName("Invalid transition throws IllegalStateException")
    void transition_invalidThrows() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        // PENDING -> APPLIED is invalid (must go through COLLECTING/READY first)
        assertThatThrownBy(() -> filter.transitionTo(SettableDynamicFilter.State.APPLIED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PENDING")
                .hasMessageContaining("APPLIED");
    }

    @Test
    @DisplayName("Transitions from terminal states are silently ignored")
    void transition_fromTerminalState_ignored() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.cancel();
        // Should not throw — silently ignored
        filter.transitionTo(SettableDynamicFilter.State.COLLECTING);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.CANCELLED);
    }

    // ---- Cancellation ----

    @Test
    @DisplayName("Cancel transitions to CANCELLED from any state")
    void cancel_fromPending() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.cancel();
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.CANCELLED);
    }

    @Test
    @DisplayName("Cancel from COLLECTING")
    void cancel_fromCollecting() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.transitionTo(SettableDynamicFilter.State.COLLECTING);
        filter.cancel();
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.CANCELLED);
    }

    @Test
    @DisplayName("Cancelled filter accepts all values")
    void cancel_acceptsAllValues() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(10, 20);
        filter.cancel();
        // Even though min/max are set, cancelled filter should accept all
        assertThat(filter.mightContain(5)).isTrue();
        assertThat(filter.mightContain(999)).isTrue();
    }

    @Test
    @DisplayName("Cancel unblocks waiting futures")
    void cancel_unblocksFuture() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", true);
        CompletableFuture<?> blocked = filter.isBlocked();
        assertThat(blocked.isDone()).isFalse();

        filter.cancel();
        assertThat(blocked.isDone()).isTrue();
    }

    // ---- Blocking behavior ----

    @Test
    @DisplayName("PENDING filter has incomplete future when awaitable")
    void blocking_pendingIsBlocked() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", true);
        CompletableFuture<?> blocked = filter.isBlocked();
        assertThat(blocked.isDone()).isFalse();
    }

    @Test
    @DisplayName("setMinMax completes the blocked future")
    void blocking_setMinMaxCompletesBlocked() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", true);
        CompletableFuture<?> blocked = filter.isBlocked();
        assertThat(blocked.isDone()).isFalse();

        filter.setMinMax(1, 10);
        assertThat(blocked.isDone()).isTrue();
    }

    @Test
    @DisplayName("Timeout completes blocked future for non-awaitable filters")
    void blocking_timeout() throws Exception {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", true);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            filter.setTimeout(Duration.ofMillis(50), scheduler);
            CompletableFuture<?> blocked = filter.isBlocked();
            blocked.get(2, TimeUnit.SECONDS);
            assertThat(blocked.isDone()).isTrue();
            // State should still be PENDING — timeout just unblocks, doesn't change state
            assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.PENDING);
        } finally {
            scheduler.shutdown();
        }
    }

    // ---- Filter data ----

    @Test
    @DisplayName("MIN_MAX filter correctly prunes values")
    void filterData_minMaxPruning() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(10, 20);

        assertThat(filter.mightContain(15)).isTrue();
        assertThat(filter.mightContain(10)).isTrue();
        assertThat(filter.mightContain(20)).isTrue();
        assertThat(filter.mightContain(5)).isFalse();
        assertThat(filter.mightContain(25)).isFalse();
    }

    @Test
    @DisplayName("BLOOM filter uses custom predicate")
    void filterData_bloomPredicate() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setBloom(value -> "match".equals(value));

        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.READY);
        assertThat(filter.filterType()).isEqualTo(DynamicFilter.Type.BLOOM);
        assertThat(filter.mightContain("match")).isTrue();
        assertThat(filter.mightContain("other")).isFalse();
    }

    @Test
    @DisplayName("Cannot set filter data after READY state")
    void filterData_immutableAfterReady() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(1, 10);

        assertThatThrownBy(() -> filter.setMinMax(5, 50))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("READY");
    }

    // ---- Listener notification ----

    @Test
    @DisplayName("onReady listener fires when filter becomes ready")
    void listener_firesOnReady() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        AtomicBoolean invoked = new AtomicBoolean(false);
        filter.onReady(() -> invoked.set(true));

        assertThat(invoked).isFalse(); // not yet ready
        filter.setMinMax(1, 10);
        assertThat(invoked).isTrue();
    }

    @Test
    @DisplayName("onReady listener fires immediately if already ready")
    void listener_immediateIfAlreadyReady() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        filter.setMinMax(1, 10);

        AtomicBoolean invoked = new AtomicBoolean(false);
        filter.onReady(() -> invoked.set(true));
        assertThat(invoked).isTrue();
    }

    @Test
    @DisplayName("Multiple onReady listeners all fire")
    void listener_multipleListeners() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        AtomicInteger count = new AtomicInteger(0);
        filter.onReady(count::incrementAndGet);
        filter.onReady(count::incrementAndGet);
        filter.onReady(count::incrementAndGet);

        filter.setMinMax(1, 10);
        assertThat(count.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("Listener exception does not prevent other listeners from firing")
    void listener_exceptionSwallowed() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("col", false);
        AtomicBoolean secondFired = new AtomicBoolean(false);
        filter.onReady(() -> { throw new RuntimeException("boom"); });
        filter.onReady(() -> secondFired.set(true));

        filter.setMinMax(1, 10);
        assertThat(secondFired).isTrue();
    }

    // ---- toString ----

    @Test
    @DisplayName("toString contains relevant state information")
    void toString_containsInfo() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("order_date", true);
        String s = filter.toString();
        assertThat(s).contains("order_date");
        assertThat(s).contains("PENDING");
        assertThat(s).contains("awaitable=true");
    }

    // ---- Full lifecycle walk-through ----

    @Test
    @DisplayName("Full lifecycle: PENDING -> COLLECTING -> READY -> APPLIED -> COMPLETED")
    void fullLifecycle() {
        SettableDynamicFilter filter = SettableDynamicFilter.create("order_id", true);
        AtomicBoolean readyFired = new AtomicBoolean(false);
        filter.onReady(() -> readyFired.set(true));

        // PENDING
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.PENDING);
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isBlocked().isDone()).isFalse();

        // COLLECTING
        filter.transitionTo(SettableDynamicFilter.State.COLLECTING);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.COLLECTING);
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.mightContain(42)).isTrue(); // accepts all during collection

        // READY (via setMinMax)
        filter.setMinMax(100, 500);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.READY);
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isBlocked().isDone()).isTrue();
        assertThat(readyFired).isTrue();
        assertThat(filter.mightContain(200)).isTrue();
        assertThat(filter.mightContain(50)).isFalse();

        // APPLIED
        filter.transitionTo(SettableDynamicFilter.State.APPLIED);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.APPLIED);
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.mightContain(200)).isTrue();
        assertThat(filter.mightContain(50)).isFalse();

        // COMPLETED
        filter.transitionTo(SettableDynamicFilter.State.COMPLETED);
        assertThat(filter.state()).isEqualTo(SettableDynamicFilter.State.COMPLETED);
    }
}
