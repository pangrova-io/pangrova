package org.pangrova.testing.e2e.shuffle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual clock, signed mTLS delegation and production renewal; not a simulated service/clock. */
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class RemoteShuffleReadRenewalTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test void renewsAndResumesAtNextVerifiedBatchWithoutRepeatingBytes() throws Exception {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        try (var service = new ActualShuffleService(directory, 1, 1)) {
            var producer = service.registerProducer();
            var request = producer.request();
            try (var egress = new RemoteShuffleByteEgress(service.clientPool())) {
                assertThat(egress.installTask(request)).isTrue();
                egress.onPartitionBatch(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(),
                        0, 1, ByteBuffer.wrap(first));
                egress.onPartitionBatch(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(),
                        0, 1, ByteBuffer.wrap(second));
                egress.onTaskCompleted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId());
            }
            service.finalizeProducer(producer);
            var consumer = ActualExternalShuffleTask.consumer(producer);
            var actual = service.delegateRead(producer, consumer);
            var shortGrant = service.shortReadGrant(producer, consumer);
            var binding = new ExternalShuffleTransport(actual.shuffleId(), actual.endpoints(), actual.commitAddress(),
                    actual.outputPartitionId(), actual.partitionCount(), actual.planEpoch(), actual.executionOwner(),
                    actual.consumerAttempt(), actual.deadlineEpochMs(), new ShuffleAccess(shortGrant.getToken().toByteArray(),
                    shortGrant.getExpiresAtEpochMs()), actual.expectedProducerPartitions(), actual.expectedProducerSetChecksum());
            verifyRollover(service, binding, first, second, shortGrant);
        }
    }

    private static void verifyRollover(ActualShuffleService service, ExternalShuffleTransport binding,
            byte[] first, byte[] second, org.pangrova.protocol.v2.ShuffleAccessCapability shortGrant) {
        try (var pool = service.clientPool();
             var source = new RemoteShuffleExchangeSource(binding, pool.borrow(binding.selectedEndpoint()))) {
            assertThat(source.nextVerifiedChunk().getData().toByteArray()).isEqualTo(first);
            assertThat(source.lastCompletedSequence()).isZero();
            await().atMost(Duration.ofSeconds(20)).until(() ->
                    shortGrant.getExpiresAtEpochMs() - System.currentTimeMillis() <= 30_000);
            assertThat(source.nextVerifiedChunk().getData().toByteArray()).isEqualTo(second);
            assertThat(source.lastCompletedSequence()).isEqualTo(1);
            assertThat(source.nextVerifiedChunk()).isNull();
            assertThat(source.isFinished()).isTrue();
            var reads = service.wire(0).requests.stream().map(ShuffleWireRelay.Frame::message)
                    .filter(ReadPartitionRequest.class::isInstance).map(ReadPartitionRequest.class::cast).toList();
            assertThat(reads).extracting(ReadPartitionRequest::getStartSequence).containsExactly(0L, 1L);
            var renewals = service.wire(0).requests.stream().map(ShuffleWireRelay.Frame::message)
                    .filter(RenewAccessRequest.class::isInstance).map(RenewAccessRequest.class::cast).toList();
            assertThat(renewals).hasSize(1);
            assertThat(renewals.get(0).getOperation()).isEqualTo(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ);
            assertThat(renewals.get(0).getPreviousCapability()).isEqualTo(shortGrant);
            assertThat(reads.get(0).getAccessCapability()).isEqualTo(shortGrant);
            assertThat(reads.get(1).getAccessCapability().getExpiresAtEpochMs()).isGreaterThan(shortGrant.getExpiresAtEpochMs());
            assertThat(reads.get(1).getAccessCapability().getToken()).isNotEqualTo(shortGrant.getToken());
            assertThat(reads.get(1).getConsumerAttempt()).isEqualTo(reads.get(0).getConsumerAttempt());
        }
    }
}
