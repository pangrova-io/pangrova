package org.pangrova.testing.integration.e2e.shuffle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;

import static org.assertj.core.api.Assertions.assertThat;

/** Publishes actual opaque component-test data using the production producer and commit RPC. */
final class ActualShufflePublication {
    private ActualShufflePublication() {}

    static ActualShuffleService.AdmittedProducer publish(ActualShuffleService service) {
        var producer = service.registerProducer();
        var request = producer.request();
        try (var egress = new RemoteShuffleByteEgress(service.clientPool())) {
            assertThat(egress.installTask(request)).isTrue();
            egress.onPartitionBatch(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(),
                    0, 1, ByteBuffer.wrap("actual-partition".getBytes(StandardCharsets.UTF_8)));
            egress.onTaskCompleted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId());
        }
        service.finalizeProducer(producer);
        return producer;
    }
}
