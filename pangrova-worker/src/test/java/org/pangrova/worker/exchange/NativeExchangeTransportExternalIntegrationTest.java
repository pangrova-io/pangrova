package org.pangrova.worker.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.FinishExchangeSourceRequest;
import org.pangrova.protocol.v2.PushExchangeBatchRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ReadPartitionTerminal;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShufflePartitionManifestChecksum;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientPool;

class NativeExchangeTransportExternalIntegrationTest {

    private NativeExchangeTransport transport;
    private Server server;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    @Test
    void forwardsVerifiedChunksAndTerminalUsingTaskAttempt() throws Exception {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 7];
        Arrays.fill(batch, (byte) 7);
        RecordingReadService service = RecordingReadService.complete(batch);
        start(service);
        CapturingExecutor executor = new CapturingExecutor();
        transport = transport(executor);

        transport.bindInputs(1L, attempt(), List.of(input(external())));

        assertThat(executor.finished.await(5, TimeUnit.SECONDS)).isTrue();
        List<RuntimeCommand> commands = executor.commands();
        assertThat(commands).allSatisfy(command ->
                assertThat(command.getAttempt().getTaskId()).isEqualTo(attempt().taskId()));
        assertThat(commands.stream().filter(command -> command.getCommandType()
                        == RuntimeCommandType.RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH))
                .hasSize(2);
        List<PushExchangeBatchRequest> pushed = commands.stream()
                .filter(command -> command.getCommandType()
                        == RuntimeCommandType.RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH)
                .map(command -> unpack(command, PushExchangeBatchRequest.class))
                .toList();
        assertThat(pushed).extracting(value -> value.getChunk().getChunkIndex())
                .containsExactly(0, 1);
        assertThat(pushed).extracting(value -> value.getChunk().getData().size())
                .containsExactly(CanonicalBatchChunks.MAX_CHUNK_BYTES, 7);
        assertThat(service.request.getAccessCapability().getToken().toStringUtf8())
                .isEqualTo("read-token");
        assertThat(finish(commands).hasFailure())
                .as(finish(commands).getFailure().getMessage()).isFalse();
        assertThat(finish(commands).getLastSequence()).isZero();
    }

    @Test
    void reportsCorruptionWhenRpcCompletesBeforeTerminal() throws Exception {
        byte[] batch = "batch".getBytes(StandardCharsets.UTF_8);
        RecordingReadService service = RecordingReadService.eofBeforeTerminal(batch);
        start(service);
        CapturingExecutor executor = new CapturingExecutor();
        transport = transport(executor);

        transport.bindInputs(1L, attempt(), List.of(input(external())));

        assertThat(executor.finished.await(5, TimeUnit.SECONDS)).isTrue();
        FinishExchangeSourceRequest finish = finish(executor.commands());
        assertThat(finish.getFailure().getErrorClass())
                .isEqualTo(ErrorClass.ERROR_CLASS_DATA_CORRUPTION);
        assertThat(finish.getFailure().getRetryable()).isFalse();
    }

    @Test
    void releaseInputsCancelsTheActivePartitionRpc() throws Exception {
        RecordingReadService service = RecordingReadService.holdOpen();
        start(service);
        CapturingExecutor executor = new CapturingExecutor();
        transport = transport(executor);

        transport.bindInputs(1L, attempt(), List.of(input(external())));
        assertThat(service.started.await(5, TimeUnit.SECONDS)).isTrue();

        transport.releaseInputs(1L);

        assertThat(service.cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.commands().stream().noneMatch(command -> command.getCommandType()
                == RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE)).isTrue();
    }

    private void start(RecordingReadService service) throws IOException {
        server = ServerBuilder.forPort(0).addService(service).build().start();
    }

    private NativeExchangeTransport transport(CapturingExecutor executor) {
        return new NativeExchangeTransport(
                executor, new RemoteShuffleClientPool(5_000, 5_000, 2), 1);
    }

    private ExternalShuffleTransport external() throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        return new ExternalShuffleTransport(
                "shuffle-1",
                List.of(new ShuffleServerEndpoint(
                        "127.0.0.1", server.getPort(), 0, 1, "")),
                new ShuffleServiceAddress("127.0.0.1", server.getPort(), ""),
                0, 1, attempt().fence().planEpoch(), attempt().executionOwner(), attempt(),
                deadline, new ShuffleAccess(bytes("read-token"), deadline + 120_000L),
                1, producerSetChecksum());
    }

    private static ExchangeInput input(ExternalShuffleTransport external) {
        return new ExchangeInput("exchange-node", "exchange-1", 1, external);
    }

    private static ExchangeTaskAttempt attempt() {
        return NativeExchangeTransportTest.attempt();
    }

    private static FinishExchangeSourceRequest finish(List<RuntimeCommand> commands) {
        return commands.stream()
                .filter(command -> command.getCommandType()
                        == RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE)
                .map(command -> unpack(command, FinishExchangeSourceRequest.class))
                .findFirst().orElseThrow();
    }

    private static <T extends com.google.protobuf.Message> T unpack(
            RuntimeCommand command, Class<T> type) {
        try {
            return command.getPayload().unpack(type);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError(failure);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] producerSetChecksum() throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes("producer-set"));
    }

    private static Checksum checksum(byte[] value) {
        return CanonicalTaskRequests.sha256(value);
    }

    private static final class CapturingExecutor implements TaskExecutor {
        private final List<byte[]> commandBytes = new CopyOnWriteArrayList<>();
        private final CountDownLatch finished = new CountDownLatch(1);

        @Override public long createTask(byte[] request) { return 1L; }
        @Override public void startTask(long handle) {}
        @Override public void releaseTask(long handle) {}
        @Override public void cancelTask(long handle) {}

        @Override
        public byte[] executeCommand(long handle, byte[] command) {
            commandBytes.add(command);
            try {
                if (RuntimeCommand.parseFrom(command).getCommandType()
                        == RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE) {
                    finished.countDown();
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new AssertionError(failure);
            }
            return RuntimeCommandResponse.newBuilder().setSuccess(true).build().toByteArray();
        }

        private List<RuntimeCommand> commands() {
            return commandBytes.stream().map(value -> {
                try {
                    return RuntimeCommand.parseFrom(value);
                } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                    throw new AssertionError(failure);
                }
            }).toList();
        }
    }

    private static final class RecordingReadService
            extends ShuffleServiceGrpc.ShuffleServiceImplBase {
        private final byte[] batch;
        private final boolean sendTerminal;
        private final boolean holdOpen;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private volatile ReadPartitionRequest request;

        private RecordingReadService(byte[] batch, boolean sendTerminal, boolean holdOpen) {
            this.batch = batch;
            this.sendTerminal = sendTerminal;
            this.holdOpen = holdOpen;
        }

        private static RecordingReadService complete(byte[] batch) {
            return new RecordingReadService(batch, true, false);
        }

        private static RecordingReadService eofBeforeTerminal(byte[] batch) {
            return new RecordingReadService(batch, false, false);
        }

        private static RecordingReadService holdOpen() {
            return new RecordingReadService(null, false, true);
        }

        @Override
        public void readPartition(
                ReadPartitionRequest value,
                StreamObserver<ReadBatchResponse> responseObserver) {
            request = value;
            started.countDown();
            ((ServerCallStreamObserver<ReadBatchResponse>) responseObserver)
                    .setOnCancelHandler(cancelled::countDown);
            if (holdOpen) {
                return;
            }
            Checksum batchChecksum = CanonicalBatchChunks.sha256(batch);
            int chunkCount = CanonicalBatchChunks.chunkCount(batch);
            for (int index = 0; index < chunkCount; index++) {
                BatchChunk chunk = CanonicalBatchChunks.chunk(
                        batch, 0, index, chunkCount, batchChecksum);
                responseObserver.onNext(ReadBatchResponse.newBuilder()
                        .setChunk(chunk).setRowCount(1L).build());
            }
            if (sendTerminal) {
                responseObserver.onNext(ReadBatchResponse.newBuilder()
                        .setTerminal(ReadPartitionTerminal.newBuilder()
                                .setCommittedLastSequence(0)
                                .setTotalBytes(batch.length)
                                .setTotalRows(1)
                                .setGenerationPlanEpoch(attempt().fence().planEpoch())
                                .setCommittedProducerCount(1)
                                .setProducerSetChecksum(checksumUnchecked())
                                .setOutputChecksum(Checksum.newBuilder()
                                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                                        .setDigest(ByteString.copyFrom(
                                                ShufflePartitionManifestChecksum.accumulator()
                                                        .add(0L, batch.length, 1L,
                                                                batchChecksum.getDigest()
                                                                        .toByteArray())
                                                        .finish()))))
                        .build());
            }
            responseObserver.onCompleted();
        }

        private static Checksum checksumUnchecked() {
            try {
                return Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(producerSetChecksum()))
                        .build();
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        }
    }
}
