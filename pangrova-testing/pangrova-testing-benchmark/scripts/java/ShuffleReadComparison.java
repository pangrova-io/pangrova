package org.pangrova.testing.integration.e2e.shuffle;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;
import org.pangrova.shuffle.pangrova.client.BaselinePangrovaPartitionRead;
import org.pangrova.shuffle.pangrova.client.BaselineRemoteShuffleExchangeSource;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleByteEgress;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleExchangeSource;

public final class ShuffleReadComparison {
    private static final int ROWS = 137_000;
    private static final int BATCHES = 4;

    private ShuffleReadComparison() {}

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        int warmups = Integer.parseInt(args[1]);
        int pairs = Integer.parseInt(args[2]);
        try (ActualShuffleService service = new ActualShuffleService(directory, 1, 1)) {
            byte[] payload = ActualExternalShuffleTask.arrowBatch(ROWS);
            var producer = service.registerProducer();
            var attempt = producer.request().getAttempt();
            try (var egress = new RemoteShuffleByteEgress(service.clientPool())) {
                if (!egress.installTask(producer.request())) {
                    throw new IllegalStateException("Producer admission failed");
                }
                for (int i = 0; i < BATCHES; i++) {
                    egress.onPartitionBatch(attempt.getQueryOrJobId(), attempt.getTaskId(), 0, ROWS, ByteBuffer.wrap(payload));
                }
                egress.onTaskCompleted(attempt.getQueryOrJobId(), attempt.getTaskId());
            }
            var receipt = service.finalizeProducer(producer).partitions().get(0);
            if (receipt.rows() != (long) ROWS * BATCHES || receipt.bytes() != (long) payload.length * BATCHES
                    || receipt.lastSequence() != BATCHES - 1) {
                throw new IllegalStateException("Incomplete publication");
            }
            System.out.println("PAYLOAD_SHA256," + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload)));
            var consumer = ActualExternalShuffleTask.consumer(producer).toBuilder()
                    .setDeadlineEpochMs(System.currentTimeMillis() + 240_000L).build();
            var binding = service.delegateRead(producer, consumer);
            try (var pool = service.clientPool()) {
                for (int pair = -warmups; pair < pairs; pair++) {
                    for (int order = 0; order < 2; order++) {
                        boolean baseline = ((pair + order) & 1) == 0;
                        readSample(service, pool, binding, baseline, pair, order, receipt.bytes());
                    }
                }
            }
        }
    }

    private interface Input {
        BatchChunk next();
    }

    private static void readSample(ActualShuffleService service, RemoteShuffleClientPool pool,
            ExternalShuffleTransport binding, boolean baseline, int pair, int order, long expectedBytes) {
        var relay = service.wire(0);
        relay.requests.clear();
        relay.responses.clear();
        var published = ActualShuffleService.partitionRead(binding);
        var oldRead = new BaselinePangrovaPartitionRead(published.shuffleId(), published.context(),
                published.planEpoch(), published.expectedProducerPartitions(), published.expectedProducerSetChecksum());
        long start = System.nanoTime();
        long received;
        if (baseline) {
            try (var reader = new BaselineRemoteShuffleExchangeSource(oldRead, binding.readAccess(),
                    pool.borrow(binding.selectedEndpoint()))) {
                received = drain(reader::nextVerifiedChunk);
                if (!reader.isFinished()) {
                    throw new IllegalStateException("Missing baseline terminal");
                }
            }
        } else {
            try (var reader = new RemoteShuffleExchangeSource(published, binding.readAccess(),
                    pool.borrow(binding.selectedEndpoint()))) {
                received = drain(reader::nextVerifiedChunk);
                if (!reader.isFinished()) {
                    throw new IllegalStateException("Missing candidate terminal");
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        var responses = relay.responses.stream().map(ShuffleWireRelay.Frame::message)
                .filter(ReadBatchResponse.class::isInstance).map(ReadBatchResponse.class::cast).toList();
        long wireBytes = responses.stream().mapToLong(ReadBatchResponse::getSerializedSize).sum();
        if (received != expectedBytes || responses.isEmpty() || !responses.get(responses.size() - 1).hasTerminal()) {
            throw new IllegalStateException("Observed bytes or terminal differ from durable publication");
        }
        if (pair >= 0) {
            System.out.printf("SAMPLE,%d,%d,%s,%d,%d,%d,%d%n", pair, order, baseline ? "baseline" : "candidate",
                    elapsed, received, wireBytes, responses.size());
        }
    }

    private static long drain(Input input) {
        long bytes = 0;
        for (BatchChunk chunk; (chunk = input.next()) != null;) {
            bytes += chunk.getData().size();
        }
        return bytes;
    }
}
