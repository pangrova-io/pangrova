package org.pangrova.coordinator.bootstrap;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryDurableRecoveryWiringTest {

    @Test
    void nonRemoteWiringDoesNotStartCleanupScheduler() {
        QueryDurableRecoveryWiring wiring = new QueryDurableRecoveryWiring(
                CoordinatorBootstrapConfig.withDefaults("coordinator-1", 0), null, null);
        assertThatThrownBy(() -> wiring.resolveShuffleOwner("query-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not initialized");
        try (var service = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease())) {
            wiring.wireHighAvailability(service,
                    () -> {
                        throw new AssertionError("non-remote mode must not start cleanup loop");
                    }, Runnable::run, 1_000L);
            assertThat(wiring.recover(List.of(), service, null)).isEmpty();
            wiring.close();
        }
    }

    @Test
    void slowBackloggedShuffleCleanupDoesNotBlockSharedHealthTimer() throws Exception {
        Set<String> pending = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 270; i++) {
            pending.add("q-" + i);
        }
        CountDownLatch rpcStarted = new CountDownLatch(1);
        CountDownLatch releaseRpc = new CountDownLatch(1);
        CountDownLatch allCleaned = new CountDownLatch(pending.size());
        AtomicInteger activeRpc = new AtomicInteger();
        AtomicInteger maxActiveRpc = new AtomicInteger();
        QueryDurableRecoveryWiring wiring = QueryDurableRecoveryWiring.withCleanupSource(
                CoordinatorBootstrapConfig.withDefaults("coordinator-1", 0),
                () -> pending.stream().sorted().limit(256).toList(), queryId -> {
                    int current = activeRpc.incrementAndGet();
                    maxActiveRpc.accumulateAndGet(current, Math::max);
                    try {
                        rpcStarted.countDown();
                        await(releaseRpc);
                        if (pending.remove(queryId)) {
                            allCleaned.countDown();
                        }
                    } finally {
                        activeRpc.decrementAndGet();
                    }
                });
        var healthTimer = Executors.newSingleThreadScheduledExecutor();
        var cleanupPool = ManagedExecutorFactory.newBoundedThreadPool(
                "shuffle-cleanup-test", 2, 2);
        try (var service = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease())) {
            wiring.wireHighAvailability(service, () -> healthTimer, cleanupPool, 10L);
            assertThat(rpcStarted.await(3, TimeUnit.SECONDS)).isTrue();
            CountDownLatch healthTick = new CountDownLatch(1);
            healthTimer.schedule(healthTick::countDown, 0, TimeUnit.MILLISECONDS);
            assertThat(healthTick.await(1, TimeUnit.SECONDS))
                    .as("health timer remains responsive while cleanup RPC is blocked")
                    .isTrue();

            releaseRpc.countDown();
            assertThat(allCleaned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(pending).isEmpty();
            assertThat(maxActiveRpc.get()).isLessThanOrEqualTo(2);
        } finally {
            releaseRpc.countDown();
            wiring.close();
            cleanupPool.shutdownNow();
            healthTimer.shutdownNow();
        }
    }

    @Test
    void shutdownStopsNewCleanupAndRejectsRewiring() throws Exception {
        CountDownLatch rpcStarted = new CountDownLatch(1);
        CountDownLatch releaseRpc = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        QueryDurableRecoveryWiring wiring = QueryDurableRecoveryWiring.withCleanupSource(
                CoordinatorBootstrapConfig.withDefaults("coordinator-1", 0),
                () -> List.of("q-shutdown"), ignored -> {
                    attempts.incrementAndGet();
                    rpcStarted.countDown();
                    await(releaseRpc);
                });
        var healthTimer = Executors.newSingleThreadScheduledExecutor();
        var cleanupPool = ManagedExecutorFactory.newBoundedThreadPool(
                "shuffle-close-test", 2, 2);
        try (var service = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease())) {
            wiring.wireHighAvailability(service, () -> healthTimer, cleanupPool, 10L);
            assertThat(rpcStarted.await(3, TimeUnit.SECONDS)).isTrue();
            wiring.close();
            releaseRpc.countDown();
            healthTimer.schedule(() -> { }, 50, TimeUnit.MILLISECONDS)
                    .get(3, TimeUnit.SECONDS);
            assertThat(attempts.get()).isEqualTo(1);
            assertThatThrownBy(() -> wiring.wireHighAvailability(
                    service, () -> healthTimer, cleanupPool, 10L))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            releaseRpc.countDown();
            wiring.close();
            cleanupPool.shutdownNow();
            healthTimer.shutdownNow();
        }
    }

    @Test
    void failedShuffleRpcKeepsDurableWorkEligibleForRetry() throws Exception {
        Set<String> pending = ConcurrentHashMap.newKeySet();
        pending.add("q-fail-once");
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        QueryDurableRecoveryWiring wiring = QueryDurableRecoveryWiring.withCleanupSource(
                CoordinatorBootstrapConfig.withDefaults("coordinator-1", 0),
                () -> List.copyOf(pending), queryId -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("remote shuffle unavailable");
                    }
                    pending.remove(queryId);
                    recovered.countDown();
                });
        var healthTimer = Executors.newSingleThreadScheduledExecutor();
        var cleanupPool = ManagedExecutorFactory.newBoundedThreadPool(
                "shuffle-fail-once-test", 2, 2);
        try (var service = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease())) {
            wiring.wireHighAvailability(service, () -> healthTimer, cleanupPool, 10L);
            assertThat(recovered.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(pending).isEmpty();
        } finally {
            wiring.close();
            cleanupPool.shutdownNow();
            healthTimer.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for test barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
