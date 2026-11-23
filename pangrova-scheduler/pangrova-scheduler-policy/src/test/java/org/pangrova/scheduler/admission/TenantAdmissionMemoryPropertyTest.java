package org.pangrova.scheduler.admission;

import org.pangrova.testing.testkit.coordination.admission.InMemoryClusterConcurrencySlots;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;

import java.math.BigInteger;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Component accounting oracle; an in-memory slot store is not distributed acceptance evidence. */
class TenantAdmissionMemoryPropertyTest {
    @Property(tries = 300, seed = "20261003")
    void admissionMatchesUnboundedArithmeticAndBalancesRejectedSlots(
            @ForAll @LongRange(min = 0) long limit,
            @ForAll @LongRange(min = 0) long clusterUsed,
            @ForAll @LongRange(min = 0) long estimated,
            @ForAll boolean sharedSlotStore) {
        var quota = new TenantLimits(limit, 1, 0);
        var slots = sharedSlotStore ? new InMemoryClusterConcurrencySlots() : null;
        var controller = new TenantQuotaAdmissionController(new MemoryUsage(clusterUsed), slots);

        var actual = controller.admit(new AdmissionRequest("tenant", "default", "query", quota,
                OptionalLong.of(estimated)));
        boolean fits = BigInteger.valueOf(clusterUsed).add(BigInteger.valueOf(estimated))
                .compareTo(BigInteger.valueOf(limit)) <= 0;

        assertThat(actual.isAdmitted()).isEqualTo(fits);
        assertThat(controller.admittedCount("tenant")).isEqualTo(fits ? 1 : 0);
        if (fits) {
            controller.release("query", "tenant");
            assertThat(controller.admittedCount("tenant")).isZero();
        }
    }

    private record MemoryUsage(long memoryBytes) implements GlobalTenantUsageView {
        @Override public long aggregatedMemoryBytes(String tenantId) { return memoryBytes; }
        @Override public int aggregatedActiveQueries(String tenantId) { return 0; }
        @Override public Set<String> trackedTenants() { return Set.of("tenant"); }
    }
}
