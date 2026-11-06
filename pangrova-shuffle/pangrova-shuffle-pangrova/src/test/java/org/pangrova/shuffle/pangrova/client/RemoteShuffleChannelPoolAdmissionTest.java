package org.pangrova.shuffle.pangrova.client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual ManagedChannel owners and joins; no RPC/server or Native execution claims. */
class RemoteShuffleChannelPoolAdmissionTest {
    @Test void actualConcurrentCreationPublicationKeepsOneCachedOwnerAndDrainsLosingCandidates() throws Exception {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(32, 32));
        var start = new CountDownLatch(1);
        var borrows = new ArrayList<CompletableFuture<RemoteShuffleClientLease>>();
        var retained = new ArrayList<RemoteShuffleClientLease>();
        try (var pool = pool(admission)) {
            try {
                for (int index = 0; index < 16; index++) borrows.add(CompletableFuture.supplyAsync(() -> {
                    try { start.await(); }
                    catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                    return pool.borrow(endpoint(9400));
                }));
                start.countDown();
                for (var borrow : borrows) retained.add(borrow.get(5, TimeUnit.SECONDS));
                var cached = retained.get(0).client();
                assertThat(retained).allSatisfy(lease -> assertThat(lease.client()).isSameAs(cached));
                assertThat(pool.activeConnectionCount()).isEqualTo(1);
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                assertThatThrownBy(pool::close).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("active producer/reader leases");
                assertThat(cached.isTerminated()).isFalse();
            } finally { for (var lease : retained) lease.close(); }
            pool.close();
            admission.requireDrained();
        }
        admission.requireDrained();
    }

    @Test void twoPoolsShareOneWorkerTotalAndCacheReuseDoesNotDoubleCharge() {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(1, 1));
        try (var producer = pool(admission); var consumer = pool(admission)) {
            try (var first = producer.borrow(endpoint(9400)); var reused = producer.borrow(endpoint(9400))) {
                assertThat(reused.client()).isSameAs(first.client());
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                assertThatThrownBy(() -> consumer.borrow(endpoint(9401)))
                        .isInstanceOf(RemoteShuffleAdmissionException.class).hasMessageContaining("scope=TOTAL");
                assertThat(consumer.activeConnectionCount()).isZero();
            }
            producer.close();
            admission.requireDrained();
            try (var admitted = consumer.borrow(endpoint(9401))) {
                assertThat(admitted.client().isTerminated()).isFalse();
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
            }
        }
        admission.requireDrained();
    }

    @Test void retiredBorrowedChannelStillOccupiesEndpointCredit() {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(2, 1));
        var options = new RemoteShuffleClientPool.PoolOptions(20, TimeUnit.MILLISECONDS, 100, false, null, null);
        try (var pool = new RemoteShuffleClientPool(5_000, 5_000, options, admission)) {
            var retained = pool.borrow(endpoint(9400));
            var actual = retained.client();
            try {
                await().pollDelay(Duration.ofMillis(60)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThatThrownBy(() -> pool.borrow(endpoint(9400)))
                                .isInstanceOf(RemoteShuffleAdmissionException.class).hasMessageContaining("scope=ENDPOINT"));
                assertThat(actual.isTerminated()).isFalse();
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
            } finally { retained.close(); }
            assertThat(actual.isTerminated()).isTrue();
            admission.requireDrained();
            try (var replacement = pool.borrow(endpoint(9400))) {
                assertThat(replacement.client()).isNotSameAs(actual);
            }
        }
        admission.requireDrained();
    }

    @Test void failedActualChannelJoinKeepsSharedCreditUntilSuccessfulRetry() {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(1, 1));
        try (var first = pool(admission); var second = pool(admission)) {
            var retained = first.borrow(endpoint(9400));
            var actual = retained.client();
            assertThatThrownBy(first::close).isInstanceOf(IllegalStateException.class);
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(retained::close).isInstanceOf(RemoteShuffleClientException.class)
                        .hasMessageContaining("interrupted");
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                assertThatThrownBy(retained::client).isInstanceOf(IllegalStateException.class);
            } finally { Thread.interrupted(); }
            assertThatThrownBy(() -> second.borrow(endpoint(9401))).isInstanceOf(RemoteShuffleAdmissionException.class);
            retained.close();
            assertThat(actual.isTerminated()).isTrue();
            admission.requireDrained();
            try (var replacement = second.borrow(endpoint(9401))) {
                assertThat(replacement.client().isTerminated()).isFalse();
            }
        }
        admission.requireDrained();
    }

    private static RemoteShuffleClientPool pool(RemoteShuffleChannelAdmission admission) {
        return new RemoteShuffleClientPool(5_000, 5_000, RemoteShuffleClientPool.PoolOptions.defaults(), admission);
    }

    private static ShuffleServerEndpoint endpoint(int port) {
        return new ShuffleServerEndpoint("localhost", port, 0, 1, "");
    }
}
