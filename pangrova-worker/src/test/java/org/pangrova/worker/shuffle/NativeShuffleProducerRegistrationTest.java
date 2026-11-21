package org.pangrova.worker.shuffle;

import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JNI server/ticket registration; task-scoped control tests, not SQL E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeShuffleProducerRegistrationTest {
    @Test
    void registerProducerReturnsSignedPartitionTickets() {
        try (var runtime = new ActualNativeTaskFixture()) {
            long handle = runtime.createPending(ActualNativeShuffleRequests.pendingProducer(4));
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, "test-host", "worker-1.internal");
            var server = transport.registerProducer(handle, 4);
            assertThat(server.host()).isEqualTo("test-host");
            assertThat(server.port()).isPositive();
            assertThat(server.tlsIdentity()).isEqualTo("worker-1.internal");
            assertThat(server.partitionTickets()).hasSize(4).allSatisfy(ticket -> {
                assertThat(ticket.signedTicket()).isNotEmpty();
                assertThat(ticket.expiresAtEpochMs()).isGreaterThan(Instant.now().toEpochMilli());
            });
            transport.unregisterProducer(handle);
        }
    }

    @Test
    void repeatedRegistrationReturnsTheSameServer() {
        try (var runtime = new ActualNativeTaskFixture()) {
            long handle = runtime.createPending(ActualNativeShuffleRequests.pendingProducer(4));
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, "127.0.0.1", "");
            var first = transport.registerProducer(handle, 4);
            var second = transport.registerProducer(handle, 4);
            assertThat(second.port()).isEqualTo(first.port()).isPositive();
            assertThat(second.partitionTickets()).extracting(org.pangrova.shuffle.api.ShufflePartitionTicket::partitionId)
                    .containsExactly(0, 1, 2, 3);
            transport.unregisterProducer(handle);
        }
    }

    @Test
    void registerProducerRejectsZeroHandle() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, "127.0.0.1", "");
            assertThatThrownBy(() -> transport.registerProducer(0, 4))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("executorHandle must not be zero");
        }
    }

    @Test
    void registerProducerRejectsNonPositivePartitions() {
        try (var runtime = new ActualNativeTaskFixture()) {
            long handle = runtime.createPending(ActualNativeShuffleRequests.pendingProducer(4));
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, "127.0.0.1", "");
            for (int partitions : new int[]{0, -1}) {
                assertThatThrownBy(() -> transport.registerProducer(handle, partitions))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("numPartitions must be positive");
            }
        }
    }

    @Test
    void unregisterProducerIsIdempotentAndLeavesExecutorReleaseToTaskOwner() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, "127.0.0.1", "");
            long handle = runtime.createPending(ActualNativeShuffleRequests.pendingProducer(1));
            transport.registerProducer(handle, 1);
            transport.unregisterProducer(handle);
            transport.unregisterProducer(handle);
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            assertThat(runtime.executor().releaseTask(handle)).isEmpty();
        }
    }

    @Test
    void defaultHost_usesLocalhost() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var transport = new NativeShuffleProducerRegistration(runtime.executor()::executeCommand, null, null);
            long handle = runtime.createPending(ActualNativeShuffleRequests.pendingProducer(1));
            var server = transport.registerProducer(handle, 1);
            assertThat(server.host()).isEqualTo("localhost");
            assertThat(server.tlsIdentity()).isEmpty();
            assertThat(server.port()).isPositive();
            transport.unregisterProducer(handle);
        }
    }
}
