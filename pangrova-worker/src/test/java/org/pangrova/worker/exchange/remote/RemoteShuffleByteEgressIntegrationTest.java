package org.pangrova.worker.exchange.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.ShuffleServiceAddress;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CleanupResponse;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.PartitionWriteAck;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.protocol.v2.shuffle.WriteBatchResponse;
import org.pangrova.worker.exchange.CanonicalBatchChunks;
import org.pangrova.shuffle.ShufflePartitionManifestChecksum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

class RemoteShuffleByteEgressIntegrationTest {

    private RecordingShuffleService service;
    private Map<Integer, List<WriteBatchRequest>> allBatches;
    private Server server;
    private RemoteShuffleByteEgress egress;

    @BeforeEach
    void setUp() throws IOException {
        allBatches = new ConcurrentHashMap<>();
        service = new RecordingShuffleService(allBatches);
        server = ServerBuilder.forPort(0).addService(service).build().start();
        egress = new RemoteShuffleByteEgress(
                new RemoteShuffleClientPool(5_000, 5_000, 2));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        egress.close();
        server.shutdownNow();
        server.awaitTermination();
    }

    @Test
    void multiplexesPartitionsPerEndpointAndCommitsProducerOnceWithoutEmptySummaries()
            throws Exception {
        TaskRequest request = taskRequest(server.getPort());
        assertThat(egress.installTask(request)).isTrue();

        egress.onPartitionBatch("query-1", "task-1", 0, 2L,
                ByteBuffer.wrap(bytes("first")));
        egress.onPartitionBatch("query-1", "task-1", 0, 3L,
                ByteBuffer.wrap(bytes("second")));
        egress.onPartitionBatch("query-1", "task-1", 1, 4L,
                ByteBuffer.wrap(bytes("third")));
        egress.onTaskCompleted("query-1", "task-1");

        assertThat(service.writeStreams.get()).isEqualTo(1);
        assertThat(service.streamsByPartition.get(0)).hasSize(1);
        assertThat(service.streamsByPartition.get(1)).hasSize(1);
        assertThat(service.batches.get(0)).extracting(
                        WriteBatchRequest::getStreamSequence)
                .containsExactly(0L, 1L);
        assertThat(service.batches.get(0)).extracting(
                        (WriteBatchRequest chunkRequest) ->
                                chunkRequest.getChunk().getBatchSequence())
                .containsExactly(0L, 1L);
        assertThat(service.batches.get(1)).extracting(
                        WriteBatchRequest::getStreamSequence)
                .containsExactly(2L);
        assertThat(service.batches.get(1)).extracting(
                        (WriteBatchRequest chunkRequest) ->
                                chunkRequest.getChunk().getBatchSequence())
                .containsExactly(0L);
        assertThat(service.batches.get(0)).extracting(
                        requestBatch -> requestBatch.getChunk().getData().toStringUtf8())
                .containsExactly("first", "second");
        assertThat(service.commitCalls.get()).isEqualTo(1);
        assertThat(service.commit.getPartitionsList())
                .extracting(summary -> summary.getPartitionId())
                .containsExactly(0, 1);
        assertThat(service.commit.getPartitions(0).getExpectedLastSequence()).isEqualTo(1L);
        assertThat(service.commit.getPartitions(1).getExpectedLastSequence()).isZero();
        assertThat(service.commit.getPartitions(0).getTotalRows()).isEqualTo(5L);
        assertThat(service.commit.getPartitions(1).getTotalRows()).isEqualTo(4L);
        assertThat(service.registerCalls.get()).isZero();
        assertThat(service.cleanupCalls.get()).isZero();
    }

    @Test
    void chunksBorrowedDirectBufferWithoutWholeBatchCopyOrPositionMutation() {
        assertThat(egress.installTask(taskRequest(server.getPort()))).isTrue();
        int batchSize = CanonicalBatchChunks.MAX_CHUNK_BYTES + 13;
        ByteBuffer batch = ByteBuffer.allocateDirect(batchSize + 3);
        batch.position(3);
        for (int index = 0; index < batchSize; index++) {
            batch.put((byte) (index * 17));
        }
        batch.flip().position(3);
        int originalPosition = batch.position();
        int originalLimit = batch.limit();

        egress.onPartitionBatch("query-1", "task-1", 0, 37L, batch);
        egress.onTaskCompleted("query-1", "task-1");

        assertThat(batch.position()).isEqualTo(originalPosition);
        assertThat(batch.limit()).isEqualTo(originalLimit);
        assertThat(service.batches.get(0)).extracting(
                        request -> request.getChunk().getChunkIndex())
                .containsExactly(0, 1);
        assertThat(service.batches.get(0)).extracting(
                        request -> request.getChunk().getData().size())
                .containsExactly(CanonicalBatchChunks.MAX_CHUNK_BYTES, 13);
        assertThat(service.batches.get(0)).extracting(WriteBatchRequest::getRowCount)
                .containsOnly(37L);
        assertThat(service.commit.getPartitions(0).getTotalRows()).isEqualTo(37L);
    }

    @Test
    void rejectsCompressionThatTheWorkerDidNotApply() {
        TaskRequest request = taskRequest(server.getPort()).toBuilder()
                .setExecutionOptions(TaskExecutionOptions.newBuilder()
                        .setShuffle(taskRequest(server.getPort())
                                .getExecutionOptions().getShuffle().toBuilder()
                                .setCompressionCodec("ZSTD")))
                .build();

        assertThatThrownBy(() -> egress.installTask(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires compression codec NONE");
    }

    @Test
    void rejectsNonPositiveBatchRowCountBeforeWriting() {
        assertThat(egress.installTask(taskRequest(server.getPort()))).isTrue();

        assertThatThrownBy(() -> egress.onPartitionBatch(
                "query-1", "task-1", 0, 0L, ByteBuffer.wrap(bytes("payload"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be positive");

        assertThat(service.writeStreams.get()).isZero();
    }

    @Test
    void commitsEmptyProducerWithoutOpeningPartitionStreams() {
        assertThat(egress.installTask(taskRequest(server.getPort()))).isTrue();

        egress.onTaskCompleted("query-1", "task-1");

        assertThat(service.writeStreams.get()).isZero();
        assertThat(service.commitCalls.get()).isEqualTo(1);
        assertThat(service.commit.getPartitionsList()).isEmpty();
    }

    @Test
    void renewsNearExpiryWriteCapabilityBeforeBatchAndCommit() {
        TaskRequest valid = taskRequest(server.getPort());
        ShuffleExecutionBinding shortLived = valid.getExecutionOptions().getShuffle()
                .getExecutionBinding().toBuilder()
                .setWriteCapability(capability(
                        "signed-write-token", System.currentTimeMillis() + 10_000L))
                .build();
        assertThat(egress.installTask(withBinding(valid, shortLived))).isTrue();

        egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                ByteBuffer.wrap(bytes("renewed-batch")));
        egress.onTaskCompleted("query-1", "task-1");

        assertThat(service.renewCalls.get()).isEqualTo(1);
        assertThat(service.renewRequest.getOperation())
                .isEqualTo(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
        assertThat(service.renewRequest.getTaskAttempt())
                .isEqualTo(valid.getAttempt());
        assertThat(service.batches.get(0).get(0).getAccessCapability().getToken().toStringUtf8())
                .isEqualTo("signed-write-token-renewed");
        assertThat(service.commit.getAccessCapability().getToken().toStringUtf8())
                .isEqualTo("signed-write-token-renewed");
    }

    @Test
    void rejectsEndpointCoverageGapsBeforeOpeningAStream() {
        TaskRequest valid = taskRequest(server.getPort());
        ShuffleExecutionBinding binding = valid.getExecutionOptions().getShuffle()
                .getExecutionBinding().toBuilder()
                .clearEndpoints()
                .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                        .setHost("127.0.0.1")
                        .setPort(server.getPort())
                        .setPartitionStart(1)
                        .setPartitionEnd(3))
                .build();

        assertThatThrownBy(() -> egress.installTask(withBinding(valid, binding)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly cover");
        assertThat(service.writeStreams.get()).isZero();
    }

    @Test
    void routesEachPartitionToItsOwningEndpointAndCommitsAtExplicitAddress()
            throws Exception {
        RecordingShuffleService secondService = new RecordingShuffleService(allBatches);
        Server secondServer = ServerBuilder.forPort(0)
                .addService(secondService).build().start();
        try {
            TaskRequest valid = taskRequest(server.getPort());
            ShuffleExecutionBinding binding = valid.getExecutionOptions().getShuffle()
                    .getExecutionBinding().toBuilder()
                    .clearEndpoints()
                    .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                            .setHost("127.0.0.1").setPort(server.getPort())
                            .setPartitionStart(0).setPartitionEnd(1))
                    .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                            .setHost("127.0.0.1").setPort(secondServer.getPort())
                            .setPartitionStart(1).setPartitionEnd(3))
                    .build();
            assertThat(egress.installTask(withBinding(valid, binding))).isTrue();

            egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                    ByteBuffer.wrap(bytes("zero")));
            egress.onPartitionBatch("query-1", "task-1", 2, 1L,
                    ByteBuffer.wrap(bytes("two")));
            egress.onTaskCompleted("query-1", "task-1");

            assertThat(service.batches).containsOnlyKeys(0);
            assertThat(secondService.batches).containsOnlyKeys(2);
            assertThat(service.writeStreams.get()).isEqualTo(1);
            assertThat(secondService.writeStreams.get()).isEqualTo(1);
            assertThat(service.commitCalls.get()).isEqualTo(1);
            assertThat(secondService.commitCalls.get()).isZero();
        } finally {
            secondServer.shutdownNow();
            secondServer.awaitTermination();
        }
    }

    @Test
    void rejectsMissingAndExpiredWriteCapabilitiesBeforeOpeningAStream() {
        TaskRequest valid = taskRequest(server.getPort());
        var binding = valid.getExecutionOptions().getShuffle()
                .getExecutionBinding().toBuilder();
        TaskRequest missing = withBinding(valid, binding.clearWriteCapability().build());
        TaskRequest expired = withBinding(valid, binding.setWriteCapability(
                capability("signed-write-token", System.currentTimeMillis() - 1L)).build());

        assertThatThrownBy(() -> egress.installTask(missing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing or expired");
        assertThatThrownBy(() -> egress.installTask(expired))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing or expired");
        assertThat(service.writeStreams.get()).isZero();
    }

    @Test
    void propagatesServiceRejectionForWrongCapabilityScope() {
        TaskRequest valid = taskRequest(server.getPort());
        var binding = valid.getExecutionOptions().getShuffle()
                .getExecutionBinding().toBuilder()
                .setWriteCapability(capability(
                        "wrong-scope", System.currentTimeMillis() + 120_000L))
                .build();
        TaskRequest wrongScope = withBinding(valid, binding);
        assertThat(egress.installTask(wrongScope)).isTrue();

        egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                ByteBuffer.wrap(bytes("payload")));
        assertThatThrownBy(() -> egress.onTaskCompleted("query-1", "task-1"))
                .isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("stream failed");
    }

    @Test
    void rejectsAcknowledgementThatDoesNotCoverEverySentBatch() {
        service.incorrectAcknowledgement = true;
        assertThat(egress.installTask(taskRequest(server.getPort()))).isTrue();
        egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                ByteBuffer.wrap(bytes("first")));
        egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                ByteBuffer.wrap(bytes("second")));

        assertThatThrownBy(() -> egress.onTaskCompleted("query-1", "task-1"))
                .isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("acknowledgement");
        assertThat(service.commitCalls.get()).isZero();
    }

    @Test
    void abortCancelsAProducerBlockedDuringStreamCompletion() throws Exception {
        service.stallCompletion = true;
        assertThat(egress.installTask(taskRequest(server.getPort()))).isTrue();
        egress.onPartitionBatch("query-1", "task-1", 0, 1L,
                ByteBuffer.wrap(bytes("payload")));
        AtomicReference<Throwable> completionFailure = new AtomicReference<>();
        CountDownLatch completionReturned = new CountDownLatch(1);
        Thread completion = new Thread(() -> {
            try {
                egress.onTaskCompleted("query-1", "task-1");
            } catch (Throwable failure) {
                completionFailure.set(failure);
            } finally {
                completionReturned.countDown();
            }
        }, "shuffle-egress-completion-test");
        completion.start();
        assertThat(service.completionStalled.await(5, TimeUnit.SECONDS)).isTrue();

        egress.onTaskAborted("query-1", "task-1", "test cancellation");

        assertThat(service.writeCancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(completionReturned.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(completionFailure.get())
                .isInstanceOf(RemoteShuffleClientException.class);
    }

    @Test
    void blockedEndpointDoesNotHeadOfLineBlockAnotherEndpoint() throws Exception {
        service.blockInbound = true;
        RecordingShuffleService healthy = new RecordingShuffleService(allBatches);
        Server healthyServer = ServerBuilder.forPort(0).addService(healthy).build().start();
        try {
            TaskRequest valid = taskRequest(server.getPort());
            ShuffleExecutionBinding binding = valid.getExecutionOptions().getShuffle()
                    .getExecutionBinding().toBuilder()
                    .clearEndpoints()
                    .setCommitAddress(ShuffleServiceAddress.newBuilder()
                            .setHost("127.0.0.1").setPort(healthyServer.getPort()))
                    .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                            .setHost("127.0.0.1").setPort(server.getPort())
                            .setPartitionStart(0).setPartitionEnd(1))
                    .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                            .setHost("127.0.0.1").setPort(healthyServer.getPort())
                            .setPartitionStart(1).setPartitionEnd(3))
                    .build();
            assertThat(egress.installTask(withBinding(valid, binding))).isTrue();
            ByteBuffer wide = ByteBuffer.allocateDirect(
                    CanonicalBatchChunks.MAX_CHUNK_BYTES * 8);
            while (wide.hasRemaining()) {
                wide.put((byte) 3);
            }
            wide.flip();
            CountDownLatch blockedDone = new CountDownLatch(1);
            AtomicReference<Throwable> blockedFailure = new AtomicReference<>();
            Thread blocked = new Thread(() -> {
                try {
                    egress.onPartitionBatch("query-1", "task-1", 0, 1L, wide);
                } catch (Throwable failure) {
                    blockedFailure.set(failure);
                } finally {
                    blockedDone.countDown();
                }
            }, "blocked-shuffle-endpoint-test");
            blocked.start();
            assertThat(service.writeOpened.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(blockedDone.await(200, TimeUnit.MILLISECONDS)).isFalse();

            egress.onPartitionBatch("query-1", "task-1", 2, 1L,
                    ByteBuffer.wrap(bytes("healthy")));

            assertThat(healthy.batchReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(healthy.batches.get(2)).hasSize(1);
            assertThat(blockedDone.getCount()).isEqualTo(1L);
            egress.onTaskAborted("query-1", "task-1", "test cancellation");
            assertThat(blockedDone.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(blockedFailure.get())
                    .isInstanceOf(RemoteShuffleClientException.class);
        } finally {
            healthyServer.shutdownNow();
            healthyServer.awaitTermination();
        }
    }

    private static TaskRequest taskRequest(int port) {
        ExecutionOwnerIdentity owner = ExecutionOwnerIdentity.newBuilder()
                .setOwnerId("coordinator-1")
                .setOwnerIncarnation("incarnation-1")
                .setFencingToken(17L)
                .build();
        TaskAttemptId attempt = TaskAttemptId.newBuilder()
                .setQueryOrJobId("query-1")
                .setStageId(3)
                .setPartitionId(4)
                .setPlanEpoch(11L)
                .setAttemptId(2)
                .setFencingToken(23L)
                .setTaskId("task-1")
                .setControlEpoch(29L)
                .setExecutionOwner(owner)
                .build();
        ShuffleExecutionBinding binding = ShuffleExecutionBinding.newBuilder()
                .setShuffleId("shuffle-1")
                .setPlanEpoch(11L)
                .setExecutionOwner(owner)
                .setWriteCapability(capability(
                        "signed-write-token", System.currentTimeMillis() + 120_000L))
                .setCommitAddress(ShuffleServiceAddress.newBuilder()
                        .setHost("127.0.0.1")
                        .setPort(port))
                .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                        .setHost("127.0.0.1")
                        .setPort(port)
                        .setPartitionStart(0)
                        .setPartitionEnd(3))
                .build();
        ShuffleTaskOptions shuffle = ShuffleTaskOptions.newBuilder()
                .setSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH)
                .setCompressionCodec("NONE")
                .setBatchSize(1024)
                .setPersistenceMode(
                        ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE)
                .setPartitionCount(3)
                .setExecutionBinding(binding)
                .build();
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(attempt)
                .setDeadlineEpochMs(System.currentTimeMillis() + 30_000L)
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setShuffle(shuffle))
                .build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ShuffleAccessCapability capability(String token, long expiry) {
        return ShuffleAccessCapability.newBuilder()
                .setToken(com.google.protobuf.ByteString.copyFromUtf8(token))
                .setExpiresAtEpochMs(expiry)
                .build();
    }

    private static TaskRequest withBinding(
            TaskRequest request, ShuffleExecutionBinding binding) {
        return request.toBuilder()
                .setExecutionOptions(request.getExecutionOptions().toBuilder()
                        .setShuffle(request.getExecutionOptions().getShuffle().toBuilder()
                                .setExecutionBinding(binding)))
                .build();
    }

    private static final class RecordingShuffleService
            extends ShuffleServiceGrpc.ShuffleServiceImplBase {

        private final AtomicInteger writeStreams = new AtomicInteger();
        private final AtomicInteger registerCalls = new AtomicInteger();
        private final AtomicInteger cleanupCalls = new AtomicInteger();
        private final AtomicInteger nextStreamId = new AtomicInteger();
        private final Map<Integer, List<WriteBatchRequest>> batches;
        private final Map<Integer, List<WriteBatchRequest>> allBatches;
        private final Map<Integer, java.util.Set<Integer>> streamsByPartition =
                new ConcurrentHashMap<>();
        private final AtomicInteger commitCalls = new AtomicInteger();
        private final AtomicInteger renewCalls = new AtomicInteger();
        private final CountDownLatch completionStalled = new CountDownLatch(1);
        private final CountDownLatch writeCancelled = new CountDownLatch(1);
        private final CountDownLatch writeOpened = new CountDownLatch(1);
        private final CountDownLatch batchReceived = new CountDownLatch(1);
        private volatile CommitProducerAttemptRequest commit;
        private volatile RenewAccessRequest renewRequest;
        private volatile boolean incorrectAcknowledgement;
        private volatile boolean stallCompletion;
        private volatile boolean blockInbound;

        private RecordingShuffleService(
                Map<Integer, List<WriteBatchRequest>> allBatches) {
            this.batches = new ConcurrentHashMap<>();
            this.allBatches = allBatches;
        }

        @Override
        public StreamObserver<WriteBatchRequest> writeBatch(
                StreamObserver<WriteBatchResponse> responseObserver) {
            writeStreams.incrementAndGet();
            int streamId = nextStreamId.incrementAndGet();
            List<WriteBatchRequest> streamBatches = new ArrayList<>();
            java.util.concurrent.atomic.AtomicBoolean rejected =
                    new java.util.concurrent.atomic.AtomicBoolean();
            ServerCallStreamObserver<WriteBatchResponse> serverObserver =
                    (ServerCallStreamObserver<WriteBatchResponse>) responseObserver;
            serverObserver.setOnCancelHandler(writeCancelled::countDown);
            if (blockInbound) {
                serverObserver.disableAutoRequest();
            }
            writeOpened.countDown();
            return new StreamObserver<>() {
                @Override
                public void onNext(WriteBatchRequest request) {
                    if (!java.util.Set.of("signed-write-token", "signed-write-token-renewed")
                            .contains(request.getAccessCapability().getToken().toStringUtf8())) {
                        rejected.set(true);
                        responseObserver.onError(Status.PERMISSION_DENIED
                                .withDescription("capability scope rejected")
                                .asRuntimeException());
                        return;
                    }
                    assertThat(request.getChunk().getBatchChecksum().getDigest().size())
                            .isEqualTo(32);
                    assertThat(request.getChunk().getChunkChecksum()).isEqualTo(
                            CanonicalTaskRequests.sha256(
                                    request.getChunk().getData().toByteArray()));
                    streamBatches.add(request);
                    batches.computeIfAbsent(
                            request.getPartitionId(), ignored ->
                                    new java.util.concurrent.CopyOnWriteArrayList<>())
                            .add(request);
                    if (allBatches != batches) {
                        allBatches.computeIfAbsent(
                                request.getPartitionId(), ignored ->
                                        new java.util.concurrent.CopyOnWriteArrayList<>())
                                .add(request);
                    }
                    streamsByPartition.computeIfAbsent(
                            request.getPartitionId(), ignored ->
                                    ConcurrentHashMap.newKeySet()).add(streamId);
                    batchReceived.countDown();
                }

                @Override
                public void onError(Throwable failure) {
                    responseObserver.onError(failure);
                }

                @Override
                public void onCompleted() {
                    if (rejected.get()) {
                        return;
                    }
                    if (stallCompletion) {
                        completionStalled.countDown();
                        return;
                    }
                    long bytes = streamBatches.stream()
                            .mapToLong(value -> value.getChunk().getData().size())
                            .sum();
                    long acceptedBatches = streamBatches.stream()
                            .map(value -> value.getPartitionId() + ":"
                                    + value.getChunk().getBatchSequence())
                            .distinct().count();
                    WriteBatchResponse.Builder response = WriteBatchResponse.newBuilder()
                            .setTotalBatchesAccepted(incorrectAcknowledgement
                                    ? Math.max(0, acceptedBatches - 1)
                                    : acceptedBatches)
                            .setTotalBytesAccepted(bytes);
                    streamBatches.stream()
                            .collect(java.util.stream.Collectors.groupingBy(
                                    WriteBatchRequest::getPartitionId))
                            .forEach((partitionId, partitionBatches) ->
                                    response.addPartitions(PartitionWriteAck.newBuilder()
                                            .setPartitionId(partitionId)
                                            .setBatchesAccepted(partitionBatches.stream()
                                                    .map(batch -> batch.getChunk()
                                                            .getBatchSequence())
                                                    .distinct().count())
                                            .setBytesAccepted(partitionBatches.stream()
                                                    .mapToLong(batch -> batch
                                                            .getChunk().getData().size())
                                                    .sum())
                                            .setLastPartitionSequence(partitionBatches.stream()
                                                    .mapToLong(batch -> batch.getChunk()
                                                            .getBatchSequence())
                                                    .max().orElseThrow())));
                    responseObserver.onNext(response.build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void commitProducerAttempt(
                CommitProducerAttemptRequest request,
                StreamObserver<CommitProducerAttemptResponse> responseObserver) {
            commitCalls.incrementAndGet();
            commit = request;
            assertThat(request.getAccessCapability().getToken().toStringUtf8())
                    .isIn("signed-write-token", "signed-write-token-renewed");
            request.getPartitionsList().forEach(summary ->
                    assertThat(summary.getOutputChecksum().getDigest().toByteArray())
                            .isEqualTo(outputDigest(summary.getPartitionId())));
            responseObserver.onNext(CommitProducerAttemptResponse.newBuilder()
                    .setAcceptedPartitions(request.getPartitionsCount())
                    .setTotalBytes(request.getPartitionsList().stream()
                            .mapToLong(summary -> summary.getTotalBytes()).sum())
                    .setCommittedAttemptFencingToken(
                            request.getProducerAttempt().getFencingToken())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void renewAccess(
                RenewAccessRequest request,
                StreamObserver<RenewAccessResponse> responseObserver) {
            renewCalls.incrementAndGet();
            renewRequest = request;
            if (request.getOperation()
                    != ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE
                    || !request.hasTaskAttempt()
                    || !request.getExecutionOwner().equals(
                            request.getTaskAttempt().getExecutionOwner())
                    || !request.getPreviousCapability().getToken().toStringUtf8()
                            .equals("signed-write-token")
                    || request.getPreviousCapability().getExpiresAtEpochMs()
                            <= System.currentTimeMillis()) {
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("invalid renewal attempt").asRuntimeException());
                return;
            }
            responseObserver.onNext(RenewAccessResponse.newBuilder()
                    .setAccessCapability(capability("signed-write-token-renewed",
                            System.currentTimeMillis() + 120_000L)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void registerShuffle(
                RegisterShuffleRequest request,
                StreamObserver<RegisterShuffleResponse> responseObserver) {
            registerCalls.incrementAndGet();
            responseObserver.onError(new AssertionError("Worker must not register shuffle"));
        }

        @Override
        public void cleanup(
                CleanupRequest request,
                StreamObserver<CleanupResponse> responseObserver) {
            cleanupCalls.incrementAndGet();
            responseObserver.onError(new AssertionError("Worker must not clean shuffle"));
        }

        private byte[] outputDigest(int partitionId) {
            var manifest = ShufflePartitionManifestChecksum.accumulator();
            allBatches.getOrDefault(partitionId, List.of()).stream()
                    .filter(batch -> batch.getChunk().getChunkIndex() == 0)
                    .sorted(java.util.Comparator.comparingLong(
                            batch -> batch.getChunk().getBatchSequence()))
                    .forEach(batch -> manifest.add(
                            batch.getChunk().getBatchSequence(),
                            batch.getChunk().getBatchSizeBytes(),
                            batch.getRowCount(),
                            batch.getChunk().getBatchChecksum()
                                    .getDigest().toByteArray()));
            return manifest.finishNonEmpty();
        }
    }
}
