package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.query.store.memory.InMemoryExecutionAnchorStore;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.query.api.coordination.ExecutionAnchorStore;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves bulk renewal count changes do not fence live queries during concurrent admission. */
class QueryOwnershipRegistryRenewalTest {

    private static final String OWNER = "coordinator-1";
    private static final String INCARNATION = "incarnation-1";

    @Test
    void acquisitionDuringRenewalDoesNotDemoteTheCoordinator() {
        var store = new HookedAnchorStore();
        var registry = new QueryOwnershipRegistry(10, Duration.ofSeconds(30), store);
        assertThat(registry.acquire("first", OWNER, INCARNATION)).isTrue();
        store.beforeRenew = () -> assertThat(registry.acquire("second", OWNER, INCARNATION)).isTrue();

        assertThat(registry.renewOwned(OWNER, INCARNATION)).isTrue();
        assertThat(registry.localAnchor("first")).isPresent();
        assertThat(registry.localAnchor("second")).isPresent();
    }

    @Test
    void completedQueryDuringRenewalDoesNotDemoteTheCoordinator() {
        var store = new HookedAnchorStore();
        var registry = new QueryOwnershipRegistry(10, Duration.ofSeconds(30), store);
        assertThat(registry.acquire("finished", OWNER, INCARNATION)).isTrue();
        store.beforeRenew = () -> assertThat(registry.release("finished", OWNER, INCARNATION)).isTrue();

        assertThat(registry.renewOwned(OWNER, INCARNATION)).isTrue();
        assertThat(registry.localAnchor("finished")).isEmpty();
    }

    @Test
    void genuinelyLostAnchorStillDemotesTheCoordinator() {
        var store = new HookedAnchorStore();
        var registry = new QueryOwnershipRegistry(10, Duration.ofSeconds(30), store);
        assertThat(registry.acquire("stolen", OWNER, INCARNATION)).isTrue();
        ExecutionAnchor anchor = registry.localAnchor("stolen").orElseThrow();
        store.beforeRenew = () -> assertThat(store.delegate.yield(anchor)).isTrue();

        assertThat(registry.renewOwned(OWNER, INCARNATION)).isFalse();
        assertThat(registry.localAnchor("stolen")).isEmpty();
    }

    @Test
    void retiringFenceIsExcludedWhileDurableReleaseReturns() throws Exception {
        var store = new HookedAnchorStore();
        var registry = new QueryOwnershipRegistry(10, Duration.ofSeconds(30), store);
        assertThat(registry.acquire("finishing", OWNER, INCARNATION)).isTrue();
        var durableReleaseDone = new CountDownLatch(1);
        var allowReleaseReturn = new CountDownLatch(1);
        var renewalStarted = new CountDownLatch(1);
        store.afterRelease = () -> {
            durableReleaseDone.countDown();
            await(allowReleaseReturn);
        };

        var executor = Executors.newFixedThreadPool(2);
        try {
            var release = executor.submit(() -> registry.release("finishing", OWNER, INCARNATION));
            await(durableReleaseDone);
            var renewal = executor.submit(() -> {
                renewalStarted.countDown();
                return registry.renewOwned(OWNER, INCARNATION);
            });
            await(renewalStarted);
            assertThat(renewal.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(registry.localAnchor("finishing")).isEmpty();
            assertThat(store.current("finishing")).isEmpty();
            allowReleaseReturn.countDown();
            assertThat(release.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(renewal.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            allowReleaseReturn.countDown();
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for renewal interleaving", e);
        }
    }

    private static final class HookedAnchorStore implements ExecutionAnchorStore {
        private final InMemoryExecutionAnchorStore delegate = new InMemoryExecutionAnchorStore();
        private Runnable beforeRenew = () -> { };
        private Runnable afterRelease = () -> { };

        @Override
        public Optional<ExecutionAnchor> tryAcquire(
                String executionId, String ownerId, String ownerIncarnation, Duration leaseDuration) {
            return delegate.tryAcquire(executionId, ownerId, ownerIncarnation, leaseDuration);
        }

        @Override
        public List<ExecutionAnchor> renewOwned(String ownerId, String ownerIncarnation, Duration leaseDuration) {
            beforeRenew.run();
            return delegate.renewOwned(ownerId, ownerIncarnation, leaseDuration);
        }

        @Override
        public boolean release(ExecutionAnchor anchor) {
            boolean released = delegate.release(anchor);
            afterRelease.run();
            return released;
        }

        @Override
        public boolean yield(ExecutionAnchor anchor) {
            return delegate.yield(anchor);
        }

        @Override
        public int yieldOwned(String ownerId, String ownerIncarnation) {
            return delegate.yieldOwned(ownerId, ownerIncarnation);
        }

        @Override
        public List<ExecutionAnchor> claimExpired(
                String newOwnerId, String newOwnerIncarnation, Duration leaseDuration, int maxBatchSize) {
            return delegate.claimExpired(newOwnerId, newOwnerIncarnation, leaseDuration, maxBatchSize);
        }

        @Override
        public Optional<ExecutionAnchor> current(String executionId) {
            return delegate.current(executionId);
        }
    }
}
