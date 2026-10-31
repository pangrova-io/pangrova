package org.pangrova.testing.integration.e2e.shuffle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.worker.exchange.remote.RemoteShuffleClient;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientException;
import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual mTLS channel eviction/drain, with real RPC callback and resource ownership boundaries. */
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class RemoteShuffleClientLifecycleIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test void closedActualRpcWakesBlockedConsumerWithoutReportingCleanEof() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.clientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            service.wire(0).pauseReadOutput = true;
            var lease = pool.borrow(binding.selectedEndpoint());
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                var started = new java.util.concurrent.CountDownLatch(1);
                var actualReader = new java.util.concurrent.atomic.AtomicReference<Thread>();
                var read = CompletableFuture.supplyAsync(() -> {
                    actualReader.set(Thread.currentThread());
                    started.countDown();
                    return source.nextVerifiedChunk();
                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                await().atMost(Duration.ofSeconds(5)).until(() ->
                        actualReader.get().getState() == Thread.State.WAITING);
                assertThat(read.isDone()).isFalse();
                source.close();
                assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RemoteShuffleClientException.class);
                assertThat(source.isFinished()).isFalse();
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(service.wire(0).cancelled).contains("ReadPartition"));
            }
            pool.close();
            assertThat(pool.activeConnectionCount()).isZero();
        }
    }

    @Test void actualEmptyCommittedPartitionPublishesVerifiedTerminalAndCleanRpcCompletion()
            throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1)) {
            var producer = service.registerProducer();
            var request = producer.request();
            try (var egress = new org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress(
                    service.clientPool())) {
                assertThat(egress.installTask(request)).isTrue();
                egress.onTaskCompleted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId());
            }
            service.finalizeProducer(producer);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            try (var pool = service.clientPool();
                 var source = new RemoteShuffleExchangeSource(binding, pool.borrow(binding.selectedEndpoint()))) {
                assertThat(source.nextVerifiedChunk()).isNull();
                assertThat(source.isFinished()).isTrue();
                assertThat(source.nextVerifiedChunk()).isNull();
                var actualTerminals = service.wire(0).responses.stream()
                        .map(ShuffleWireRelay.Frame::message)
                        .filter(org.pangrova.protocol.v2.shuffle.ReadBatchResponse.class::isInstance)
                        .map(org.pangrova.protocol.v2.shuffle.ReadBatchResponse.class::cast)
                        .filter(org.pangrova.protocol.v2.shuffle.ReadBatchResponse::hasTerminal).toList();
                assertThat(actualTerminals).singleElement().satisfies(frame -> {
                    assertThat(frame.getTerminal().getTotalRows()).isZero();
                    assertThat(frame.getTerminal().getTotalBytes()).isZero();
                    assertThat(frame.getTerminal().getCommittedProducerCount()).isEqualTo(1);
                });
            }
        }
    }

    @Test void idleEvictionDoesNotCancelBorrowedActualPartitionRpc() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.shortIdleClientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var consumer = ActualExternalShuffleTask.consumer(producer);
            var binding = service.delegateRead(producer, consumer);
            service.wire(0).pauseReadOutput = true;
            var lease = pool.borrow(binding.selectedEndpoint());
            RemoteShuffleClient actualChannel = lease.client();
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                    pool.cleanUp();
                    assertThat(pool.activeConnectionCount()).isEqualTo(1);
                    assertThat(actualChannel.isTerminated()).isFalse();
                    assertThat(service.wire(0).cancelled).doesNotContain("ReadPartition");
                });
                service.wire(0).resume.countDown();
                assertThat(source.nextVerifiedChunk().getData().toStringUtf8()).isEqualTo("actual-partition");
                assertThat(source.nextVerifiedChunk()).isNull();
                assertThat(source.isFinished()).isTrue();
            }
            pool.cleanUp();
            assertThat(pool.activeConnectionCount()).isZero();
            assertThat(actualChannel.isTerminated()).isTrue();
        }
    }

    @Test void closeFailureRetainsActiveRpcUntilExplicitSourceDrainThenRetry() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.clientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            service.wire(0).pauseReadOutput = true;
            var lease = pool.borrow(binding.selectedEndpoint());
            var actualChannel = lease.client();
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(pool::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("active");
                assertThat(actualChannel.isTerminated()).isFalse();
                assertThat(pool.activeConnectionCount()).isEqualTo(1);
                assertThatThrownBy(() -> pool.borrow(binding.selectedEndpoint())).isInstanceOf(IllegalStateException.class);
                source.close();
            }
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.wire(0).cancelled).contains("ReadPartition"));
            pool.close();
            assertThat(actualChannel.isTerminated()).isTrue();
            assertThat(pool.activeConnectionCount()).isZero();
        }
    }

    @Test void interruptedActualChannelJoinRetainsOwnerForARealRetry() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.clientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            var lease = pool.borrow(binding.selectedEndpoint());
            var actualChannel = lease.client();
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(source.nextVerifiedChunk()).isNotNull();
                assertThat(source.nextVerifiedChunk()).isNull();
            }
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(pool::close).isInstanceOf(RemoteShuffleClientException.class)
                        .hasMessageContaining("interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(pool.activeConnectionCount()).isEqualTo(1);
            } finally {
                Thread.interrupted();
            }
            pool.close();
            assertThat(actualChannel.isTerminated()).isTrue();
            assertThat(pool.activeConnectionCount()).isZero();
        }
    }

    @Test void concurrentPoolShutdownCannotCloseAStillBorrowedActualRpc() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.clientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            service.wire(0).pauseReadOutput = true;
            var lease = pool.borrow(binding.selectedEndpoint());
            var actualChannel = lease.client();
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                var first = CompletableFuture.runAsync(pool::close);
                var second = CompletableFuture.runAsync(pool::close);
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
                assertThat(actualChannel.isTerminated()).isFalse();
                source.close();
            }
            pool.close();
            assertThat(actualChannel.isTerminated()).isTrue();
            assertThat(pool.activeConnectionCount()).isZero();
        }
    }

    @Test void interruptedLeaseReturnRejectsFurtherUseButAllowsActualChannelDrainRetry() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var pool = service.clientPool()) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            var lease = pool.borrow(binding.selectedEndpoint());
            var actualChannel = lease.client();
            try (var source = new RemoteShuffleExchangeSource(binding, lease)) {
                assertThat(source.nextVerifiedChunk().getData().toStringUtf8()).isEqualTo("actual-partition");
                assertThat(source.nextVerifiedChunk()).isNull();
                assertThat(source.isFinished()).isTrue();
                assertThatThrownBy(pool::close).isInstanceOf(IllegalStateException.class);
                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(lease::close).isInstanceOf(RemoteShuffleClientException.class)
                            .hasMessageContaining("interrupted");
                    assertThat(Thread.currentThread().isInterrupted()).isTrue();
                    assertThat(pool.activeConnectionCount()).isEqualTo(1);
                    assertThatThrownBy(lease::client).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("released");
                } finally {
                    Thread.interrupted();
                }
                lease.close();
                assertThat(actualChannel.isTerminated()).isTrue();
                assertThat(pool.activeConnectionCount()).isZero();
            }
            pool.close();
        }
    }

}
