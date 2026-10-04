package org.pangrova.resource.property.tenant;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.resource.tenant.TenantResourceQuota;
import org.pangrova.resource.tenant.TenantResourceTracker;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual counter admission and refund over the complete positive signed-long domain. */
class TenantResourceAccountingPropertyTest {
    @Property(tries = 200, seed = "20261003")
    void exactAdmissionAndRefundNeverWrap(@ForAll("limits") long limit,
                                          @ForAll long firstCandidate, @ForAll long secondCandidate) {
        var tracker = new TenantResourceTracker();
        tracker.setQuota("tenant", new TenantResourceQuota(limit, -1));
        long first = Math.floorMod(firstCandidate, limit) + 1;
        long second = Math.floorMod(secondCandidate, limit) + 1;
        assertThat(tracker.tryAllocateMemory("tenant", first)).isTrue();
        boolean fits = second <= limit - first;
        assertThat(tracker.tryAllocateMemory("tenant", second)).isEqualTo(fits);
        assertThat(tracker.getMemoryUsage("tenant")).isEqualTo(fits ? first + second : first);
        tracker.releaseMemory("tenant", first);
        if (fits) tracker.releaseMemory("tenant", second);
        assertThat(tracker.getMemoryUsage("tenant")).isZero();
        assertThat(tracker.tryAllocateMemory("tenant", limit)).isTrue();
        assertThat(tracker.tryAllocateMemory("tenant", 1)).isFalse();
        assertThat(tracker.snapshotUsage().get("tenant").memoryBytes()).isEqualTo(limit);
    }

    @Provide
    Arbitrary<Long> limits() { return Arbitraries.longs().between(1, Long.MAX_VALUE); }
}
