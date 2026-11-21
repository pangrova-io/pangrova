package org.pangrova.testing.integration.e2e.shuffle;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.shuffle.nativewire.exception.ShuffleChannelAdmissionException;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission;
import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleExchangeSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real durable-service mTLS reader leases and shared channel credits; not distributed SQL E2E. */
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class RemoteShuffleChannelAdmissionIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test void twoPoolsCannotExceedSharedWorkerCreditWhileAnActualReadRpcIsPaused() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1)) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            var admission = new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(1, 1));
            try (var first = service.clientPool(admission); var second = service.clientPool(admission)) {
                service.wire(0).pauseReadOutput = true;
                var lease = first.borrow(binding.selectedEndpoint());
                var actual = lease.client();
                try (var source = new RemoteShuffleExchangeSource(ActualShuffleService.partitionRead(binding), binding.readAccess(), lease)) {
                    assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(() -> second.borrow(binding.selectedEndpoint()))
                            .isInstanceOf(ShuffleChannelAdmissionException.class).hasMessageContaining("scope=TOTAL");
                    assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                    assertThat(actual.isTerminated()).isFalse();
                    assertThat(service.wire(0).opened).filteredOn("ReadPartition"::equals).hasSize(1);
                    service.wire(0).resume.countDown();
                    assertThat(source.nextVerifiedChunk().getData().toStringUtf8()).isEqualTo("actual-partition");
                    assertThat(source.nextVerifiedChunk()).isNull();
                }
                first.close();
                admission.requireDrained();
                try (var replacement = second.borrow(binding.selectedEndpoint())) {
                    assertThat(replacement.client().isTerminated()).isFalse();
                }
            }
            admission.requireDrained();
        }
    }

    @Test void retiredActualReaderChannelStillOccupiesItsEndpointLimit() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1)) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            var admission = new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(2, 1));
            try (var pool = service.shortIdleClientPool(admission)) {
                service.wire(0).pauseReadOutput = true;
                var lease = pool.borrow(binding.selectedEndpoint());
                var actual = lease.client();
                try (var source = new RemoteShuffleExchangeSource(ActualShuffleService.partitionRead(binding), binding.readAccess(), lease)) {
                    assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
                    await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                        pool.cleanUp();
                        assertThat(actual.isTerminated()).isFalse();
                        assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                    });
                    assertThatThrownBy(() -> pool.borrow(binding.selectedEndpoint()))
                            .isInstanceOf(ShuffleChannelAdmissionException.class).hasMessageContaining("scope=ENDPOINT");
                    assertThat(service.wire(0).opened).filteredOn("ReadPartition"::equals).hasSize(1);
                    service.wire(0).resume.countDown();
                    assertThat(source.nextVerifiedChunk()).isNotNull();
                    assertThat(source.nextVerifiedChunk()).isNull();
                }
                assertThat(actual.isTerminated()).isTrue();
                admission.requireDrained();
            }
        }
    }

    @Test void failedRealReaderChannelJoinRetainsSharedQuotaUntilRetry() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1)) {
            var producer = ActualShufflePublication.publish(service);
            var binding = service.delegateRead(producer, ActualExternalShuffleTask.consumer(producer));
            var admission = new RemoteShuffleChannelAdmission(new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(1, 1));
            try (var first = service.clientPool(admission); var second = service.clientPool(admission)) {
                var lease = first.borrow(binding.selectedEndpoint());
                var actual = lease.client();
                try (var source = new RemoteShuffleExchangeSource(ActualShuffleService.partitionRead(binding), binding.readAccess(), lease)) {
                    assertThat(source.nextVerifiedChunk()).isNotNull();
                    assertThat(source.nextVerifiedChunk()).isNull();
                    assertThatThrownBy(first::close).isInstanceOf(IllegalStateException.class);
                    Thread.currentThread().interrupt();
                    try {
                        assertThatThrownBy(lease::close).isInstanceOf(RemoteShuffleClientException.class);
                        assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
                    } finally { Thread.interrupted(); }
                    assertThatThrownBy(() -> second.borrow(binding.selectedEndpoint()))
                            .isInstanceOf(ShuffleChannelAdmissionException.class);
                    lease.close();
                    assertThat(actual.isTerminated()).isTrue();
                    admission.requireDrained();
                }
            }
        }
    }
}
