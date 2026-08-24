package org.pangrova.metadata.property.dynamictable;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link DynamicTableState} immutable transition methods.
 *
 * <p><b>Validates: Requirements 2, 7</b></p>
 */
class DynamicTableStatePropertyTest {

    /**
     * Property 1: withState returns a new instance — original is unchanged.
     */
    @Property(tries = 10)
    void withStatePreservesOriginal(@ForAll("dynamicTableStates") DynamicTableState original) {
        DynamicTableLifecycleState originalState = original.state();
        DynamicTableLifecycleState newState = pickDifferentState(originalState);

        DynamicTableState updated = original.withState(newState);

        assertThat(original.state())
                .as("original state must not change")
                .isEqualTo(originalState);
        assertThat(updated.state()).isEqualTo(newState);
        assertThat(updated.dtName()).isEqualTo(original.dtName());
        assertThat(updated.tenantId()).isEqualTo(original.tenantId());
        assertThat(updated.checkpoint()).isEqualTo(original.checkpoint());
        assertThat(updated.consecutiveFailures()).isEqualTo(original.consecutiveFailures());
        assertThat(updated.totalRefreshCount()).isEqualTo(original.totalRefreshCount());
    }

    /**
     * Property 2: withCheckpoint returns a new instance — original is unchanged,
     * consecutiveFailures is reset to 0, totalRefreshCount is incremented.
     */
    @Property(tries = 10)
    void withCheckpointResetsFailuresAndIncrementsCount(
            @ForAll("dynamicTableStates") DynamicTableState original) {
        String origCheckpoint = original.checkpoint();
        int origFailures = original.consecutiveFailures();
        long origCount = original.totalRefreshCount();

        DynamicTableState updated = original.withCheckpoint("snap-999", "SNAPSHOT");

        // original unchanged
        assertThat(original.checkpoint()).isEqualTo(origCheckpoint);
        assertThat(original.consecutiveFailures()).isEqualTo(origFailures);
        assertThat(original.totalRefreshCount()).isEqualTo(origCount);

        // updated has new checkpoint, reset failures, incremented count
        assertThat(updated.checkpoint()).isEqualTo("snap-999");
        assertThat(updated.checkpointType()).isEqualTo("SNAPSHOT");
        assertThat(updated.consecutiveFailures()).isZero();
        assertThat(updated.totalRefreshCount()).isEqualTo(origCount + 1);
        assertThat(updated.dtName()).isEqualTo(original.dtName());
    }

    /**
     * Property 3: withFailure returns a new instance — original is unchanged,
     * consecutiveFailures is incremented by exactly 1.
     */
    @Property(tries = 10)
    void withFailureIncrementsConsecutiveFailures(
            @ForAll("dynamicTableStates") DynamicTableState original) {
        int origFailures = original.consecutiveFailures();

        DynamicTableState updated = original.withFailure();

        assertThat(original.consecutiveFailures())
                .as("original failures must not change")
                .isEqualTo(origFailures);
        assertThat(updated.consecutiveFailures()).isEqualTo(origFailures + 1);
        assertThat(updated.dtName()).isEqualTo(original.dtName());
        assertThat(updated.checkpoint()).isEqualTo(original.checkpoint());
        assertThat(updated.totalRefreshCount()).isEqualTo(original.totalRefreshCount());
    }

    // ── Generators ──

    @Provide
    Arbitrary<DynamicTableState> dynamicTableStates() {
        Arbitrary<String> names = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> tenants = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10);
        Arbitrary<DynamicTableLifecycleState> states = Arbitraries.of(DynamicTableLifecycleState.values());
        Arbitrary<DynamicTableRefreshMode> modes = Arbitraries.of(DynamicTableRefreshMode.values());
        Arbitrary<Integer> failures = Arbitraries.integers().between(0, 100);
        Arbitrary<Long> counts = Arbitraries.longs().between(0, 10_000);

        return Combinators.combine(names, tenants, states, modes, failures, counts)
                .as((name, tenant, state, mode, fail, count) -> {
                    Instant now = Instant.now();
                    return new DynamicTableState(
                            name, tenant, state, mode,
                            Duration.ofMinutes(5),
                            "snap-" + count, "SNAPSHOT",
                            1, null,
                            "admin", "coord-1", 1L,
                            now, count, fail,
                            now, now);
                });
    }

    private static DynamicTableLifecycleState pickDifferentState(DynamicTableLifecycleState current) {
        for (DynamicTableLifecycleState s : DynamicTableLifecycleState.values()) {
            if (s != current) {
                return s;
            }
        }
        return current;
    }
}
