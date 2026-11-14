package org.pangrova.worker.resource.memory;

import org.pangrova.worker.resource.tenant.TenantResourceQuota;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link HeartbeatCrossDomainMemoryCoordinator} and its
 * integration with {@link LargestQueryKiller} for cross-domain arbitration.
 */
class CrossDomainMemoryCoordinatorTest {

    private TenantResourceTracker tenantTracker;
    private RecordingNativeQuotaSender quotaSender;
    private Clock fixedClock;
    private HeartbeatCrossDomainMemoryCoordinator coordinator;

    @BeforeEach
    void setUp() {
        tenantTracker = new TenantResourceTracker();
        quotaSender = new RecordingNativeQuotaSender();
        fixedClock = Clock.fixed(Instant.ofEpochMilli(100_000), ZoneId.of("UTC"));
        coordinator = new HeartbeatCrossDomainMemoryCoordinator(
                tenantTracker, quotaSender, fixedClock, 10_000);
    }

    @Test
    void publishQuotaChangePushesToNative() {
        coordinator.publishQuotaChange("tenant-a", 1024 * 1024);

        assertThat(quotaSender.calls).hasSize(1);
        assertThat(quotaSender.calls.get(0).tenantId()).isEqualTo("tenant-a");
        assertThat(quotaSender.calls.get(0).bytes()).isEqualTo(1024 * 1024);
    }

    @Test
    void publishQuotaChangeRejectsNullTenant() {
        assertThatThrownBy(() -> coordinator.publishQuotaChange(null, 1024))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publishQuotaChangeRejectsNonPositiveBytes() {
        assertThatThrownBy(() -> coordinator.publishQuotaChange("t", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> coordinator.publishQuotaChange("t", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publishQuotaChangePropagatesNativeException() {
        quotaSender.failOnNext = true;
        assertThatThrownBy(() -> coordinator.publishQuotaChange("t", 1024))
                .isInstanceOf(NativeQuotaSender.NativeQuotaSendException.class);
    }

    @Test
    void onNativeEventUpdatesUsage() {
        var event = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 5000, 99_000);
        coordinator.onNativeEvent(event);

        assertThat(coordinator.nativeUsage("tenant-a")).isEqualTo(5000);
    }

    @Test
    void onNativeEventRejectsNull() {
        assertThatThrownBy(() -> coordinator.onNativeEvent(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void combinedUsageReturnsJavaPlusNative() {
        tenantTracker.setQuota("tenant-a", new TenantResourceQuota(10_000_000, 10));
        tenantTracker.tryAllocateMemory("tenant-a", 3000);

        var event = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 7000, 95_000);
        coordinator.onNativeEvent(event);

        assertThat(coordinator.combinedUsage("tenant-a")).isEqualTo(3000 + 7000);
    }

    @Test
    void combinedUsageReturnsJavaOnlyWhenNativeStale() {
        tenantTracker.setQuota("tenant-a", new TenantResourceQuota(10_000_000, 10));
        tenantTracker.tryAllocateMemory("tenant-a", 3000);

        // Event timestamp is 80_000, clock is at 100_000 → elapsed = 20_000 > timeout 10_000
        var staleEvent = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 7000, 80_000);
        coordinator.onNativeEvent(staleEvent);

        long usage = coordinator.combinedUsage("tenant-a");
        assertThat(usage).isEqualTo(3000); // Java only, native stale
        assertThat(coordinator.staleStatCount()).isEqualTo(1);
    }

    @Test
    void combinedUsageReturnsJavaOnlyWhenNoNativeEvent() {
        tenantTracker.setQuota("tenant-a", new TenantResourceQuota(10_000_000, 10));
        tenantTracker.tryAllocateMemory("tenant-a", 3000);

        // No native event received → stale
        assertThat(coordinator.combinedUsage("tenant-a")).isEqualTo(3000);
        assertThat(coordinator.isNativeStatStale("tenant-a")).isTrue();
    }

    @Test
    void isNativeStatStaleFreshHeartbeat() {
        // Event at 95_000, clock at 100_000 → elapsed = 5_000 < timeout 10_000
        var event = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 1000, 95_000);
        coordinator.onNativeEvent(event);

        assertThat(coordinator.isNativeStatStale("tenant-a")).isFalse();
    }

    @Test
    void isNativeStatStaleExpiredHeartbeat() {
        // Event at 85_000, clock at 100_000 → elapsed = 15_000 > timeout 10_000
        var event = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 1000, 85_000);
        coordinator.onNativeEvent(event);

        assertThat(coordinator.isNativeStatStale("tenant-a")).isTrue();
    }

    @Test
    void spillEventUpdatesUsage() {
        var event = new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.SPILL, 2000, 99_000);
        coordinator.onNativeEvent(event);

        assertThat(coordinator.nativeUsage("tenant-a")).isEqualTo(2000);
        assertThat(coordinator.isNativeStatStale("tenant-a")).isFalse();
    }

    @Test
    void largestQueryKillerUsesCombinedUsageForArbitration() {
        tenantTracker.setQuota("tenant-a", new TenantResourceQuota(10_000_000, 10));
        tenantTracker.tryAllocateMemory("tenant-a", 1000);
        tenantTracker.setQuota("tenant-b", new TenantResourceQuota(10_000_000, 10));
        tenantTracker.tryAllocateMemory("tenant-b", 5000);

        // tenant-a has 1000 Java + 9000 Native = 10000 combined
        // tenant-b has 5000 Java + 1000 Native = 6000 combined
        coordinator.onNativeEvent(new NativeMemoryEvent("tenant-a", NativeMemoryEvent.Type.STAT, 9000, 99_000));
        coordinator.onNativeEvent(new NativeMemoryEvent("tenant-b", NativeMemoryEvent.Type.STAT, 1000, 99_000));

        var killer = new LargestQueryKiller(t -> true, coordinator::combinedUsage);

        var root = new RootMemoryPool("root", 100_000);
        NodeMemoryPool node = root.createNodePool("node", 100_000);
        QueryMemoryPool qA = node.createQueryPool("q-a", 50_000, "tenant-a");
        qA.tryReserve(1000);
        QueryMemoryPool qB = node.createQueryPool("q-b", 50_000, "tenant-b");
        qB.tryReserve(5000);

        Optional<QueryMemoryPool> victim = killer.chooseQueryToKill(List.of(qA, qB));

        // tenant-a has higher combined usage (10000 > 6000), so q-a is killed
        assertThat(victim).isPresent().containsSame(qA);
    }

    @Test
    void largestQueryKillerFallsBackToPoolBytesWithoutCoordinator() {
        var killer = new LargestQueryKiller(t -> false);

        var root = new RootMemoryPool("root", 100_000);
        NodeMemoryPool node = root.createNodePool("node", 100_000);
        QueryMemoryPool q1 = node.createQueryPool("q-1", 50_000, "tenant-a");
        q1.tryReserve(1000);
        QueryMemoryPool q2 = node.createQueryPool("q-2", 50_000, "tenant-b");
        q2.tryReserve(5000);

        Optional<QueryMemoryPool> victim = killer.chooseQueryToKill(List.of(q1, q2));
        // q2 has more pool bytes (5000 > 1000)
        assertThat(victim).isPresent().containsSame(q2);
    }

    @Test
    void constructorRejectsNullDependencies() {
        assertThatThrownBy(() -> new HeartbeatCrossDomainMemoryCoordinator(null, quotaSender))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HeartbeatCrossDomainMemoryCoordinator(tenantTracker, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HeartbeatCrossDomainMemoryCoordinator(
                tenantTracker, quotaSender, null, 10_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HeartbeatCrossDomainMemoryCoordinator(
                tenantTracker, quotaSender, fixedClock, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- Test helper: records quota push calls ---

    private static final class RecordingNativeQuotaSender implements NativeQuotaSender {
        final List<QuotaCall> calls = new ArrayList<>();
        boolean failOnNext = false;

        @Override
        public void setTenantQuota(String tenantId, long bytes) {
            if (failOnNext) {
                throw new NativeQuotaSendException(tenantId, bytes, new RuntimeException("simulated"));
            }
            calls.add(new QuotaCall(tenantId, bytes));
        }

        record QuotaCall(String tenantId, long bytes) {}
    }
}
