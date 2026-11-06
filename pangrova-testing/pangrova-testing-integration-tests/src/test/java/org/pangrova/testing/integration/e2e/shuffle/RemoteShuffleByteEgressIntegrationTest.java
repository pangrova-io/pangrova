package org.pangrova.testing.integration.e2e.shuffle;

import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.protocol.shuffle.codec.CanonicalBatchChunks;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleByteEgress;
import org.pangrova.shuffle.spi.worker.ShuffleByteEgress;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual durable service RPCs with wire-only observation/faults; not a SQL/Native task E2E. */
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class RemoteShuffleByteEgressIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;
    private ActualShuffleService service;
    private RemoteShuffleByteEgress egress;
    private ShuffleByteEgress callbacks;
    private ActualShuffleService.AdmittedProducer producer;
    private TaskRequest request;

    @BeforeEach void start() throws Exception { configure(3, 1, directory); }

    private void configure(int partitions, int endpoints, Path path) throws Exception {
        service = new ActualShuffleService(path, partitions, endpoints);
        producer = service.registerProducer();
        request = producer.request();
        egress = new RemoteShuffleByteEgress(service.clientPool());
        callbacks = egress;
    }

    @AfterEach void close() throws Exception {
        Exception failure = null;
        if (service != null) {
            for (int index = 0; index < service.endpoints().size(); index++) service.wire(index).resume.countDown();
        }
        for (AutoCloseable owner : new AutoCloseable[]{egress, service}) {
            if (owner == null) continue;
            try { owner.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) throw failure;
    }

    @Test void multiplexesPartitionsPerEndpointAndCommitsProducerOnceWithoutEmptySummaries() {
        install();
        write(0, 2, buffer("first"));
        write(0, 3, buffer("second"));
        write(1, 4, buffer("third"));
        complete();
        assertThat(service.wire(0).opened).filteredOn("WriteBatch"::equals).hasSize(1);
        var batches = calls(0, WriteBatchRequest.class);
        assertThat(batches).extracting(WriteBatchRequest::getStreamSequence).containsExactly(0L, 1L, 2L);
        assertThat(batches).extracting(frame -> frame.getChunk().getBatchSequence()).containsExactly(0L, 1L, 0L);
        assertThat(batches).extracting(frame -> frame.getChunk().getData().toStringUtf8())
                .containsExactly("first", "second", "third");
        var commits = calls(0, CommitProducerAttemptRequest.class);
        assertThat(commits).hasSize(1);
        assertThat(commits.get(0).getPartitionsList()).extracting(partition -> partition.getPartitionId())
                .containsExactly(0, 1);
        assertThat(commits.get(0).getPartitions(0).getExpectedLastSequence()).isEqualTo(1);
        assertThat(commits.get(0).getPartitions(1).getExpectedLastSequence()).isZero();
        assertThat(commits.get(0).getPartitions(0).getTotalRows()).isEqualTo(5);
        assertThat(commits.get(0).getPartitions(1).getTotalRows()).isEqualTo(4);
        assertThat(service.wire(0).opened).doesNotContain("RegisterShuffle", "Cleanup");
        assertDurableRows(3, 1);
    }

    @Test void chunksBorrowedDirectBufferWithoutWholeBatchCopyOrPositionMutation() {
        install();
        int size = CanonicalBatchChunks.MAX_CHUNK_BYTES + 13;
        var data = ByteBuffer.allocateDirect(size + 3).position(3);
        for (int index = 0; index < size; index++) data.put((byte) (index * 17));
        data.flip().position(3);
        int position = data.position();
        int limit = data.limit();
        write(0, 37, data);
        complete();
        assertThat(data.position()).isEqualTo(position);
        assertThat(data.limit()).isEqualTo(limit);
        var frames = calls(0, WriteBatchRequest.class);
        assertThat(frames).extracting(frame -> frame.getChunk().getChunkIndex()).containsExactly(0, 1);
        assertThat(frames).extracting(frame -> frame.getChunk().getData().size())
                .containsExactly(CanonicalBatchChunks.MAX_CHUNK_BYTES, 13);
        assertThat(frames).extracting(WriteBatchRequest::getRowCount).containsOnly(37L);
        assertThat(calls(0, CommitProducerAttemptRequest.class).get(0).getPartitions(0).getTotalRows()).isEqualTo(37);
        byte[] received = new byte[size];
        frames.get(0).getChunk().getData().copyTo(received, 0);
        frames.get(1).getChunk().getData().copyTo(received, CanonicalBatchChunks.MAX_CHUNK_BYTES);
        for (int index = 0; index < size; index++) assertThat(received[index]).isEqualTo((byte) (index * 17));
        assertDurableRows(1, 1);
    }

    @Test void rejectsCompressionThatTheWorkerDidNotApply() {
        for (String codec : List.of("", "NONE", "ZSTD", " none", "none ", "gzip")) {
            var invalid = withCodec(codec);
            assertThatThrownBy(() -> egress.installTask(invalid)).as("codec=%s", codec)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown shuffle stream codec");
        }
        assertThat(service.wire(0).opened).isEmpty();
    }

    @Test void transportsSelfDescribedStreamBytesWithoutApplyingOuterCompression() throws Exception {
        // These official stream bytes contain their own encoding metadata. The
        // task text is admission metadata, not permission to re-encode payloads.
        for (String codec : List.of("none", "lz4", "zstd")) {
            close();
            configure(1, 1, directory.resolve(codec));
            request = withCodec(codec);
            byte[] bytes = ActualExternalShuffleTask.arrowBatch(3);
            install();
            write(0, 3, ByteBuffer.wrap(bytes));
            complete();
            assertThat(calls(0, WriteBatchRequest.class)).singleElement().satisfies(frame -> {
                assertThat(frame.getCodec()).isEqualTo(
                        org.pangrova.protocol.v2.shuffle.CompressionCodec.COMPRESSION_CODEC_NONE);
                assertThat(frame.getChunk().getData().toByteArray()).containsExactly(bytes);
                assertThat(frame.getChunk().getBatchChecksum()).isEqualTo(CanonicalBatchChunks.sha256(bytes));
                assertThat(frame.getChunk().getChunkChecksum()).isEqualTo(CanonicalBatchChunks.sha256(bytes));
                assertThat(frame.getRowCount()).isEqualTo(3);
                assertThat(frame.getStreamSequence()).isZero();
                assertThat(frame.getChunk().getBatchSequence()).isZero();
            });
            assertThat(calls(0, CommitProducerAttemptRequest.class)).singleElement()
                    .satisfies(commit -> assertThat(commit.getPartitions(0).getTotalRows()).isEqualTo(3));
            assertDurableRows(1, 1);
        }
    }

    @Test void rejectsNonPositiveBatchRowCountBeforeWriting() {
        install();
        assertThatThrownBy(() -> write(0, 0, buffer("payload"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be positive");
        assertThat(service.wire(0).opened).isEmpty();
    }

    @Test void commitsEmptyProducerWithoutOpeningPartitionStreams() {
        install();
        complete();
        assertThat(service.wire(0).opened).filteredOn("WriteBatch"::equals).isEmpty();
        assertThat(calls(0, CommitProducerAttemptRequest.class)).singleElement()
                .satisfies(commit -> assertThat(commit.getPartitionsList()).isEmpty());
        assertDurableRows(0, 1);
    }

    @Test void duplicateAdmissionReturnsOnlyTheCandidateLeaseAndPreservesActualProducer() {
        install();
        write(0, 2, buffer("before-duplicate"));
        assertThat(egress.installTask(request)).isTrue();
        write(0, 3, buffer("after-duplicate"));
        complete();
        assertThat(service.wire(0).opened).filteredOn("WriteBatch"::equals).hasSize(1);
        assertThat(calls(0, WriteBatchRequest.class)).extracting(WriteBatchRequest::getStreamSequence)
                .containsExactly(0L, 1L);
        assertThat(calls(0, CommitProducerAttemptRequest.class)).singleElement()
                .satisfies(commit -> assertThat(commit.getPartitions(0).getTotalRows()).isEqualTo(5));
        assertDurableRows(2, 1);
    }

    @Test void conflictingAdmissionDrainsCandidateWithoutReplacingTheActualProducer() {
        install();
        write(0, 2, buffer("before-conflict"));
        var conflict = request.toBuilder().setDeadlineEpochMs(request.getDeadlineEpochMs() + 1).build();
        assertThatThrownBy(() -> egress.installTask(conflict)).isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("Conflicting external shuffle task binding");
        // The failed creation transaction may retry its cleanup. It must
        // never abort the already installed request sharing these raw IDs.
        egress.releaseTask(conflict);
        write(0, 3, buffer("after-conflict"));
        complete();
        assertThat(service.wire(0).opened).filteredOn("WriteBatch"::equals).hasSize(1);
        assertThat(calls(0, WriteBatchRequest.class)).extracting(frame -> frame.getChunk().getData().toStringUtf8())
                .containsExactly("before-conflict", "after-conflict");
        assertThat(calls(0, CommitProducerAttemptRequest.class)).hasSize(1);
        assertDurableRows(2, 1);
    }

    @Test void exactRequestReleaseJoinsItsProducerAndRejectsSubsequentWrites() {
        install();
        write(0, 2, buffer("before-exact-release"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertDurableRows(1, 0));
        egress.releaseTask(request);
        egress.releaseTask(request);
        assertThatThrownBy(() -> write(0, 1, buffer("after-exact-release")))
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("No installed external shuffle task");
        assertThat(calls(0, CommitProducerAttemptRequest.class)).isEmpty();
        assertDurableProducerCommits(0);
    }

    @Test void creationRollbackForConflictingBindingDoesNotAbortTheExistingRealProducer() {
        install();
        write(0, 2, buffer("before-creation-rollback"));
        var conflict = request.toBuilder().setSecurity(request.getSecurity().toBuilder().setPrincipal("another-subject")).build();
        // This actual production creation fails at external admission, before
        // JNI. It is not proof that a Native task was created or completed.
        var executor = new org.pangrova.worker.execution.NativeTaskExecutor(egress);
        try {
            assertThatThrownBy(() -> executor.createTask(conflict.toByteArray()))
                    .isInstanceOf(org.pangrova.worker.execution.NativeTaskCreationFailure.class)
                    .satisfies(failure -> assertThat(((org.pangrova.worker.execution.NativeTaskCreationFailure) failure)
                            .withoutNativeOwner()).isTrue());
            write(0, 3, buffer("after-creation-rollback"));
            complete();
            assertThat(calls(0, WriteBatchRequest.class)).extracting(frame -> frame.getChunk().getData().toStringUtf8())
                    .containsExactly("before-creation-rollback", "after-creation-rollback");
            assertThat(calls(0, CommitProducerAttemptRequest.class)).hasSize(1);
            assertDurableRows(2, 1);
        } finally {
            executor.close();
        }
    }

    @Test void renewsNearExpiryWriteCapabilityBeforeBatchAndCommit() throws Exception {
        var shortAccess = service.shortWriteGrant(producer);
        assertThat(shortAccess.getExpiresAtEpochMs()).isLessThan(System.currentTimeMillis() + 30_000);
        request = withBinding(binding().toBuilder().setWriteCapability(shortAccess).build());
        install();
        write(0, 1, buffer("renewed-batch"));
        complete();
        assertThat(calls(0, RenewAccessRequest.class)).singleElement().satisfies(renewal -> {
            assertThat(renewal.getOperation()).isEqualTo(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            assertThat(renewal.getTaskAttempt()).isEqualTo(request.getAttempt());
            assertThat(renewal.getExecutionOwner()).isEqualTo(request.getAttempt().getExecutionOwner());
            assertThat(renewal.getPreviousCapability()).isEqualTo(shortAccess);
        });
        var used = calls(0, WriteBatchRequest.class).get(0).getAccessCapability();
        assertThat(used.getToken()).isNotEqualTo(shortAccess.getToken());
        assertThat(used.getExpiresAtEpochMs()).isGreaterThan(shortAccess.getExpiresAtEpochMs());
        assertThat(calls(0, CommitProducerAttemptRequest.class).get(0).getAccessCapability()).isEqualTo(used);
        assertDurableRows(1, 1);
    }

    @Test void rejectsEndpointCoverageGapsBeforeOpeningAStream() {
        var invalid = binding().toBuilder().clearEndpoints()
                .addEndpoints(binding().getEndpoints(0).toBuilder().setPartitionStart(1)).build();
        assertThatThrownBy(() -> egress.installTask(withBinding(invalid))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly cover");
        assertThat(service.wire(0).opened).isEmpty();
    }

    @Test void routesEachPartitionToItsOwningEndpointAndCommitsAtExplicitAddress() throws Exception {
        close();
        configure(3, 2, directory.resolve("routes"));
        install();
        write(0, 1, buffer("first-server"));
        write(2, 1, buffer("second-server"));
        complete();
        assertThat(calls(0, WriteBatchRequest.class)).extracting(WriteBatchRequest::getPartitionId).containsOnly(0);
        assertThat(calls(1, WriteBatchRequest.class)).extracting(WriteBatchRequest::getPartitionId).containsOnly(2);
        assertThat(service.wire(0).opened).filteredOn("WriteBatch"::equals).hasSize(1);
        assertThat(service.wire(1).opened).filteredOn("WriteBatch"::equals).hasSize(1);
        assertThat(calls(0, CommitProducerAttemptRequest.class)).hasSize(1);
        assertThat(calls(1, CommitProducerAttemptRequest.class)).isEmpty();
        assertDurableRows(2, 1);
    }

    @Test void rejectsMissingAndExpiredWriteCapabilitiesBeforeOpeningAStream() {
        assertThatThrownBy(() -> egress.installTask(withBinding(binding().toBuilder().clearWriteCapability().build())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing or expired");
        var expired = binding().toBuilder().setWriteCapability(binding().getWriteCapability().toBuilder()
                .setExpiresAtEpochMs(System.currentTimeMillis() - 1)).build();
        assertThatThrownBy(() -> egress.installTask(withBinding(expired)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing or expired");
        assertThat(service.wire(0).opened).isEmpty();
    }

    @Test void propagatesServiceRejectionForWrongCapabilityScope() {
        // This is the actual service-issued READ grant, not a fabricated signed token.
        var wrong = binding().toBuilder().setWriteCapability(producer.registration().getReadCapability()).build();
        request = withBinding(wrong);
        install();
        write(0, 1, buffer("payload"));
        assertThatThrownBy(this::complete).isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("stream failed");
        assertDurableRows(0, 0);
    }

    @Test void rejectsAcknowledgementThatDoesNotCoverEverySentBatch() {
        service.wire(0).corruptWriteCoverage = true;
        install();
        write(0, 1, buffer("first"));
        write(0, 1, buffer("second"));
        assertThatThrownBy(this::complete).isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("acknowledgement");
        assertThat(calls(0, CommitProducerAttemptRequest.class)).isEmpty();
        assertDurableRows(2, 0);
    }

    @Test void abortCancelsAProducerBlockedDuringStreamCompletion() throws Exception {
        service.wire(0).pauseWriteResponse = true;
        install();
        write(0, 1, buffer("payload"));
        var completion = CompletableFuture.runAsync(this::complete);
        assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(completion.isDone()).isFalse();
        abort();
        await().atMost(Duration.ofSeconds(5)).until(() -> service.wire(0).cancelled.contains("WriteBatch"));
        assertThatThrownBy(() -> completion.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(RemoteShuffleClientException.class);
        assertThat(calls(0, CommitProducerAttemptRequest.class)).isEmpty();
        assertDurableProducerCommits(0);
    }

    @Test void blockedEndpointDoesNotHeadOfLineBlockAnotherEndpoint() throws Exception {
        close();
        configure(4, 2, directory.resolve("head-of-line"));
        service.wire(0).pauseWriteInput = true;
        install();
        var wide = ByteBuffer.allocateDirect(16 * 1024 * 1024);
        while (wide.hasRemaining()) wide.put((byte) 3);
        wide.flip();
        var blocked = CompletableFuture.runAsync(() -> write(0, 1, wide));
        assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(blocked.isDone()).isFalse();
        write(2, 1, buffer("healthy"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(calls(1, WriteBatchRequest.class)).hasSize(1));
        assertThat(blocked.isDone()).isFalse();
        abort();
        assertThatThrownBy(() -> blocked.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(RemoteShuffleClientException.class);
    }

    @Test void deliveryDoesNotCommitProducerAndReturnedBufferCanBeReused() {
        install();
        byte[] expected = "borrowed-payload".getBytes(StandardCharsets.UTF_8);
        ByteBuffer borrowed = ByteBuffer.allocateDirect(expected.length);
        borrowed.put(expected).flip();
        write(0, 11, borrowed);
        // A completed callback owns no view of the caller's memory. Reuse it before finish.
        for (int index = 0; index < borrowed.limit(); index++) borrowed.put(index, (byte) 0);
        assertThat(calls(0, CommitProducerAttemptRequest.class)).isEmpty();
        assertDurableProducerCommits(0);
        complete();
        assertThat(calls(0, WriteBatchRequest.class)).singleElement().satisfies(frame -> {
            assertThat(frame.getChunk().getData().toByteArray()).containsExactly(expected);
            assertThat(frame.getRowCount()).isEqualTo(11);
            assertThat(frame.getChunk().getBatchSequence()).isZero();
        });
        assertDurableRows(1, 1);
    }

    @Test void absentOrForeignBindingCannotWriteOrComplete() {
        assertThatThrownBy(() -> write(0, 1, buffer("unbound")))
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("No installed");
        assertThatThrownBy(this::complete)
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("No installed");
        install();
        String taskId = request.getAttempt().getTaskId();
        assertThatThrownBy(() -> callbacks.onPartitionBatch("foreign-query", taskId, 0, 1, buffer("foreign")))
                .isInstanceOf(RemoteShuffleClientException.class);
        assertThatThrownBy(() -> callbacks.onTaskCompleted("foreign-query", taskId))
                .isInstanceOf(RemoteShuffleClientException.class);
        assertThatThrownBy(() -> callbacks.onTaskAborted("foreign-query", taskId, "foreign abort"))
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("identity mismatch");
        assertThat(service.wire(0).opened).isEmpty();
        write(0, 1, buffer("authorized"));
        complete();
        assertDurableRows(1, 1);
    }

    @Test void completionRetiresBindingAndLateAbortDoesNotRemovePublishedData() {
        install();
        write(0, 2, buffer("committed"));
        complete();
        assertThatThrownBy(this::complete)
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("No installed");
        assertThatThrownBy(() -> write(0, 2, buffer("late")))
                .isInstanceOf(RemoteShuffleClientException.class).hasMessageContaining("No installed");
        abort();
        abort();
        assertThat(calls(0, CommitProducerAttemptRequest.class)).hasSize(1);
        assertThat(service.wire(0).opened).doesNotContain("Cleanup");
        assertDurableRows(1, 1);
    }

    @Test void invalidPartitionAndEmptyBatchFailBeforeDataPlaneWrite() {
        install();
        assertThatThrownBy(() -> write(-1, 1, buffer("invalid")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> write(3, 1, buffer("invalid")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> write(0, 1, ByteBuffer.allocate(0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not be empty");
        assertThat(service.wire(0).opened).isEmpty();
        complete();
        assertDurableRows(0, 1);
    }

    private void install() { assertThat(egress.installTask(request)).isTrue(); }
    private void write(int partition, long rows, ByteBuffer bytes) {
        callbacks.onPartitionBatch(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(),
                partition, rows, bytes);
    }
    private void complete() { callbacks.onTaskCompleted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId()); }
    private void abort() { callbacks.onTaskAborted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(), "wire cancellation"); }
    private ShuffleExecutionBinding binding() { return request.getExecutionOptions().getShuffle().getExecutionBinding(); }
    private TaskRequest withBinding(ShuffleExecutionBinding binding) {
        return request.toBuilder().setExecutionOptions(request.getExecutionOptions().toBuilder()
                .setShuffle(request.getExecutionOptions().getShuffle().toBuilder().setExecutionBinding(binding))).build();
    }
    private TaskRequest withCodec(String codec) {
        return request.toBuilder().setExecutionOptions(request.getExecutionOptions().toBuilder()
                .setShuffle(request.getExecutionOptions().getShuffle().toBuilder().setCompressionCodec(codec))).build();
    }
    private static ByteBuffer buffer(String value) { return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)); }
    private <T extends Message> List<T> calls(int endpoint, Class<T> type) {
        return service.wire(endpoint).requests.stream().map(ShuffleWireRelay.Frame::message)
                .filter(type::isInstance).map(type::cast).toList();
    }
    private void assertDurableProducerCommits(int commits) {
        try (var connection = service.backend().jdbc().getConnection();
             var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT count(*) FROM shuffle.shuffle_producer_commit")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(commits);
        } catch (java.sql.SQLException failure) {
            throw new IllegalStateException("Actual producer commit state read failed", failure);
        }
    }

    private void assertDurableRows(int batches, int commits) {
        try (var connection = service.backend().jdbc().getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT count(*) FROM shuffle.shuffle_batch")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(batches);
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM shuffle.shuffle_producer_commit")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(commits);
            }
        } catch (java.sql.SQLException failure) { throw new IllegalStateException("Actual durable state read failed", failure); }
    }
}
