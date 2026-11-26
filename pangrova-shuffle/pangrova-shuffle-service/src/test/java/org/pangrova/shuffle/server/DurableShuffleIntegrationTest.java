package org.pangrova.shuffle.server;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CompressionCodec;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.protocol.v2.shuffle.WriteBatchResponse;
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.shuffle.transport.CompleteWriteBatch;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationShape;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

class DurableShuffleIntegrationTest extends ContainerTestBase {

    private static final String ACCESS_KEY = "pangrova-test";
    private static final String SECRET_KEY = "pangrova-test-secret";
    private static final String BUCKET = "shuffle-test";
    private static final byte[] SECRET = new byte[32];
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements");
    private static final GenericContainer<?> MINIO = org.pangrova.testing.testkit.MinioContainer.create()
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server", "/data")
            .withExposedPorts(9000);
    private static Path passwordFile;

    @BeforeAll
    static void startBackends() throws Exception {
        POSTGRES.start();
        createExecutionAuthority(POSTGRES.getJdbcUrl());
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                    POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        }
        seedAnchor("query-1", owner());
        seedAnchor("ttl-query", owner());
        seedAnchor("large-cleanup-query", owner());
        MINIO.start();
        passwordFile = Files.createTempFile("shuffle-postgres", ".password");
        Files.writeString(passwordFile, POSTGRES.getPassword());
        try (S3Client client = s3Client()) {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    @AfterAll
    static void stopBackends() throws Exception {
        MINIO.stop();
        POSTGRES.stop();
        Files.deleteIfExists(passwordFile);
    }

    @Test
    void survivesRestartFencesRetriesDetectsCorruptionAndCleansOrphans() throws Exception {
        Harness first = openHarness();
        var registration = first.engine.register(registration());
        String shuffleId = registration.getShuffleId();
        var emptyCommit = first.engine.commit(CommitProducerAttemptRequest.newBuilder()
                .setShuffleId(shuffleId).setProducerAttempt(attempt(0, 5))
                .setAccessCapability(taskGrant(first, registration,
                        attempt(0, 5), org.pangrova.protocol.v2.shuffle
                                .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE))
                .build());
        assertThat(emptyCommit.getAcceptedPartitions()).isZero();
        assertThat(emptyCommit.getTotalBytes()).isZero();
        byte[] firstPayload = "attempt-zero".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        accept(first, registration, attempt(1, 10), firstPayload);
        assertThatThrownBy(() -> first.engine.commit(CommitProducerAttemptRequest.newBuilder()
                .setShuffleId(shuffleId).setProducerAttempt(attempt(1, 10))
                .setAccessCapability(taskGrant(first, registration,
                        attempt(1, 10), org.pangrova.protocol.v2.shuffle
                                .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE))
                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exact durable partition coverage");
        first.engine.commit(commitRequest(first, registration,
                attempt(1, 10), firstPayload, 7));
        assertThatThrownBy(() -> first.engine.commit(commitRequest(
                first, registration, attempt(1, 10), firstPayload, 8)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("manifest checksum");

        byte[] winnerPayload = "attempt-one-wins".getBytes(
                java.nio.charset.StandardCharsets.UTF_8);
        acceptAndCommit(first, registration, attempt(2, 20), winnerPayload);
        var finalized = first.engine.finalizeShuffle(FinalizeShuffleRequest.newBuilder()
                .setShuffleId(shuffleId).setPlanEpoch(2).setExecutionOwner(owner())
                .setExpectedProducerPartitions(1).setExpectedProducerSetChecksum(
                        ShuffleChecksums.protocol(producerChecksum()))
                .setAccessCapability(registration.getFinalizeCapability()).build());
        assertThat(finalized.getTotalRows()).isEqualTo(7);
        assertThat(finalized.getPartitions(0).getOutputChecksum().getDigest().toByteArray())
                .isEqualTo(manifest(winnerPayload, 7));
        first.engine.close();

        Harness restarted = openHarness();
        var readRequest = ReadPartitionRequest.newBuilder()
                .setShuffleId(shuffleId).setPartitionId(0).setStartSequence(0)
                .setConsumerAttempt(consumerAttempt()).setDeadlineEpochMs(
                        System.currentTimeMillis() + 60_000)
                .setAccessCapability(taskGrant(restarted, registration,
                        consumerAttempt(), org.pangrova.protocol.v2.shuffle
                                .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ))
                .build();
        try (var transport = new CanonicalShuffleGrpcServer(restarted.engine, config().limits())) {
            var server = io.grpc.ServerBuilder.forPort(0).addService(transport).build().start();
            var channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext().build();
            try {
                var stub = org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS);
                var frames = stub.readPartition(readRequest);
                assertThat(frames.next().getChunk().getData().toByteArray()).isEqualTo(winnerPayload);
                assertThat(frames.next().getTerminal().getCommittedLastSequence()).isZero();
                assertThat(frames.hasNext()).isFalse();
                // Starting immediately after the final batch yields only the committed terminal.
                var tail = stub.readPartition(readRequest.toBuilder().setStartSequence(1).build());
                assertThat(tail.next().hasTerminal()).isTrue();
                assertThat(tail.hasNext()).isFalse();
            } finally {
                channel.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
        long grantsBefore = performedReadWindows();
        var statementsBefore = shuffleStatements();
        var read = restarted.engine.read(readRequest);
        assertShuffleStatementCount(statementsBefore, 5);
        assertThat(read.committedProducerCount()).isEqualTo(1);
        assertThat(read.planEpoch()).isEqualTo(2);
        assertThat(read.producerSetChecksum()).isEqualTo(producerChecksum());
        for (int frame = 0; frame < 128; frame++) restarted.engine.requireReadAuthority(read);
        assertThat(performedReadWindows()).isEqualTo(grantsBefore + 1);
        assertThat(protectedReaders("query-1")).isEqualTo(1);

        var page = restarted.engine.readPage(read, 0);
        assertThat(page).hasSize(1);
        assertThat(restarted.engine.readPayload(read, page.get(0))).isEqualTo(winnerPayload);

        try (S3Client client = s3Client()) {
            String key = "integration/" + shuffleId
                    + "/producer-0/attempt-2/output-0/batch-0.bin";
            byte[] changed = winnerPayload.clone();
            changed[0] ^= 1;
            client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
                    RequestBody.fromBytes(changed));
        }
        assertThatThrownBy(() -> restarted.engine.readPayload(read, page.get(0)))
                .isInstanceOf(org.pangrova.shuffle.server.exception.ShuffleServiceException.class)
                .hasMessageContaining("checksum");
        try (S3Client client = s3Client()) {
            client.putObject(PutObjectRequest.builder().bucket(BUCKET)
                            .key("integration/" + page.get(0).objectKey()).build(),
                    RequestBody.fromBytes(new byte[winnerPayload.length + 1]));
        }
        assertThatThrownBy(() -> restarted.engine.readPayload(read, page.get(0)))
                .isInstanceOf(org.pangrova.shuffle.server.exception.ShuffleServiceException.class)
                .hasMessageContaining("unexpected length");

        Harness concurrent = openHarness();
        try {
            assertThatThrownBy(() -> concurrent.engine.cleanup(CleanupRequest.newBuilder()
                    .setQueryId("query-1").setExecutionOwner(owner())
                    .setAccessCapability(registration.getCleanupCapability()).build()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("reads are still in progress");
        } finally {
            concurrent.engine.close();
        }
        read.close();
        assertThat(protectedReaders("query-1")).isZero();
        assertThatThrownBy(() -> restarted.engine.readPayload(read, page.get(0)))
                .isInstanceOf(SecurityException.class).hasMessageContaining("closed or expired");
        var successor = owner().toBuilder().setOwnerId("recovery-coordinator")
                .setOwnerIncarnation("recovery-process").setFencingToken(2).build();
        replaceAnchor("query-1", successor);
        var renew = org.pangrova.protocol.v2.shuffle.RenewAccessRequest.newBuilder()
                .setOperation(org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP)
                .setQueryId("query-1").setExecutionOwner(successor).build();
        try (var database = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword()); var proof = database.prepareStatement(
                        "UPDATE public.execution_anchor SET recovery_drain_state=? WHERE execution_id='query-1'")) {
            proof.setInt(1, 1);
            assertThat(proof.executeUpdate()).isOne();
            assertThatThrownBy(() -> restarted.engine.renewAccess(renew, true)).isInstanceOf(SecurityException.class);
            proof.setInt(1, 2);
            assertThat(proof.executeUpdate()).isOne();
        }
        var permit = restarted.engine.renewAccess(renew, true).getAccessCapability();
        var cleanupRequest = CleanupRequest.newBuilder().setQueryId("query-1").setExecutionOwner(successor)
                .setAccessCapability(permit).build();
        assertThat(restarted.engine.cleanup(cleanupRequest).getBytesFreed()).isPositive();
        assertThat(restarted.engine.cleanup(cleanupRequest).getBytesFreed()).isZero();
        restarted.engine.close();
    }

    @Test
    void publishedManifestResumesAfterRestartWithoutBatchMetadataQueries() throws Exception {
        String query = "remote-manifest-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        var first = openHarness();
        int pageSize = ShufflePartitionManifest.INLINE_BATCHES;
        int count = pageSize + 5;
        int lastPartition = 16;
        ReadPartitionRequest readRequest;
        byte[] publishedDigest;
        try (var engine = first.engine) {
            var registrationRequest = registration().toBuilder()
                    .setQueryId(query).setShuffleId(shuffle).setNumPartitions(lastPartition + 1)
                    .setExpectedProducerSetChecksum(
                            ShuffleChecksums.protocol(ShuffleProducerSetChecksum.forContiguousPartitions(
                                    query, 1, 2, 1))).build();
            var registration = engine.register(registrationRequest);
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var grant = taskGrant(first, registration, producer, org.pangrova.protocol.v2.shuffle
                    .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            var checksum = ShufflePartitionManifestChecksum.accumulator();
            byte[] lastPayload = sequencePayload(lastPartition);
            byte[] lastDigest = ShuffleChecksums.sha256(lastPayload);
            try (var writer = engine.writeSession(() -> false)) {
                for (int sequence = 0; sequence < count; sequence++) {
                    byte[] payload = sequencePayload(sequence);
                    byte[] digest = ShuffleChecksums.sha256(payload);
                    writer.accept(new CompleteWriteBatch(shuffle, 0, sequence, 1, producer, grant,
                            ByteString.copyFrom(payload), digest));
                    checksum.add(sequence, Long.BYTES, 1, digest);
                }
                writer.accept(new CompleteWriteBatch(shuffle, lastPartition, 0, 1, producer, grant,
                        ByteString.copyFrom(lastPayload), lastDigest));
                writer.finish();
            }
            engine.commit(CommitProducerAttemptRequest.newBuilder().setShuffleId(shuffle)
                    .setProducerAttempt(producer).setAccessCapability(grant)
                    .addPartitions(PartitionCommitSummary.newBuilder().setPartitionId(0)
                            .setExpectedLastSequence(count - 1L).setTotalRows(count)
                            .setTotalBytes((long) count * Long.BYTES).setTotalBatches(count)
                            .setOutputChecksum(ShuffleChecksums.protocol(checksum.finishNonEmpty())))
                    .addPartitions(PartitionCommitSummary.newBuilder().setPartitionId(lastPartition)
                            .setExpectedLastSequence(0).setTotalRows(1)
                            .setTotalBytes(Long.BYTES).setTotalBatches(1)
                            .setOutputChecksum(ShuffleChecksums.protocol(
                                    ShufflePartitionManifestChecksum.accumulator()
                                            .add(0, Long.BYTES, 1, lastDigest).finishNonEmpty()))).build());
            var finalize = FinalizeShuffleRequest.newBuilder().setShuffleId(shuffle).setPlanEpoch(2)
                    .setExecutionOwner(owner()).setExpectedProducerPartitions(1)
                    .setExpectedProducerSetChecksum(registrationRequest.getExpectedProducerSetChecksum())
                    .setAccessCapability(registration.getFinalizeCapability()).build();
            var beforePublish = shuffleStatements();
            var finalized = engine.finalizeShuffle(finalize);
            assertThat(finalized.getPartitions(0).getCommittedLastSequence()).isEqualTo(count - 1L);
            assertThat(finalized.getPartitions(1).getCommittedLastSequence()).isEqualTo(-1);
            assertThat(finalized.getPartitions(lastPartition).getCommittedLastSequence()).isZero();
            publishedDigest = finalized.getPartitions(0).getOutputChecksum().getDigest().toByteArray();
            assertThat(shuffleStatementDelta(beforePublish).entrySet())
                    .filteredOn(entry -> entry.getKey().contains(
                            "ORDER BY b.output_partition_id, b.producer_partition_id, b.batch_sequence"))
                    .singleElement().satisfies(entry -> assertThat(entry.getValue()).isEqualTo(1));
            var beforeReplay = shuffleStatements();
            assertThat(engine.finalizeShuffle(finalize)).isEqualTo(finalized);
            assertThat(shuffleStatementDelta(beforeReplay).keySet())
                    .noneMatch(sql -> sql.contains("shuffle_batch"));
            var consumer = consumerAttempt().toBuilder().setQueryOrJobId(query).build();
            readRequest = ReadPartitionRequest.newBuilder().setShuffleId(shuffle).setPartitionId(0)
                    .setStartSequence(1).setConsumerAttempt(consumer)
                    .setDeadlineEpochMs(System.currentTimeMillis() + 60_000)
                    .setAccessCapability(taskGrant(first, registration, consumer, org.pangrova.protocol.v2.shuffle
                            .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)).build();
        }

        try (S3Client client = s3Client(); var engine = openHarness().engine) {
            assertThat(client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET)
                    .prefix("integration/" + shuffle + "/manifest/").build()).contents()).hasSize(1);
            try (var read = engine.read(readRequest)) {
                var before = shuffleStatements();
                var firstPage = engine.readPage(read, read.startSequence());
                assertThat(firstPage).hasSize(pageSize);
                assertThat(engine.readPayload(read, firstPage.get(pageSize - 1))).isEqualTo(sequencePayload(pageSize));
                var page = engine.readPage(read, read.startSequence() + firstPage.size());
                assertThat(page).hasSize(4);
                assertThat(engine.readPayload(read, page.get(page.size() - 1))).isEqualTo(sequencePayload(count - 1L));
                assertThat(shuffleStatementDelta(before).keySet()).allMatch(sql ->
                        sql.contains("public.execution_anchor") || sql.contains("shuffle_query_scope")
                                || sql.startsWith("WITH pinned AS MATERIALIZED"));
            }
            assertResumedFrames(engine, readRequest, count, publishedDigest);
            assertIndexAdmissionReleasesPin(engine, readRequest, query);
            var cleanup = engine.renewAccess(org.pangrova.protocol.v2.shuffle.RenewAccessRequest.newBuilder()
                    .setOperation(org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP)
                    .setQueryId(query).setExecutionOwner(owner()).build(), true);
            engine.cleanup(CleanupRequest.newBuilder().setQueryId(query).setExecutionOwner(owner())
                    .setAccessCapability(cleanup.getAccessCapability()).build());
            assertThat(client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET)
                    .prefix("integration/" + shuffle + "/").build()).contents()).isEmpty();
        }
    }

    private static void assertResumedFrames(
            ShuffleServiceEngine engine, ReadPartitionRequest request, int count, byte[] digest) throws Exception {
        try (var transport = new CanonicalShuffleGrpcServer(engine, config().limits())) {
            var server = io.grpc.ServerBuilder.forPort(0).addService(transport).build().start();
            var channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext().build();
            try {
                var frames = org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(10, TimeUnit.SECONDS).readPartition(request);
                for (long sequence = request.getStartSequence(); sequence < count; sequence++) {
                    var chunk = frames.next().getChunk();
                    assertThat(chunk.getBatchSequence()).isEqualTo(sequence);
                    assertThat(chunk.getData().toByteArray()).isEqualTo(sequencePayload(sequence));
                }
                var terminal = frames.next().getTerminal();
                assertThat(terminal.getCommittedLastSequence()).isEqualTo(count - 1L);
                assertThat(terminal.getOutputChecksum().getDigest().toByteArray()).isEqualTo(digest);
                assertThat(frames.hasNext()).isFalse();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertIndexAdmissionReleasesPin(
            ShuffleServiceEngine engine, ReadPartitionRequest request, String query) throws Exception {
        var limits = new ShuffleServiceConfig.Limits(1024, 1024, 3 * 1024, 1, 1, 1);
        try (var transport = new CanonicalShuffleGrpcServer(engine, limits)) {
            var server = io.grpc.ServerBuilder.forPort(0).addService(transport).build().start();
            var channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext().build();
            try {
                var frames = org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(10, TimeUnit.SECONDS).readPartition(request);
                assertThatThrownBy(frames::hasNext).isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(
                                io.grpc.Status.Code.RESOURCE_EXHAUSTED));
                assertThat(protectedReaders(query)).isZero();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void winnerChangeFencesAStaleManifestUploadAndPublication() throws Exception {
        String query = "manifest-version-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        try (var store = PostgresShuffleStore.create(config().database())) {
            store.register(registerCommand(query, shuffle, 1, System.currentTimeMillis() + 60_000));
            var first = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            store.batchStore().commitAttempt(new ShuffleServiceModel.CommitCommand(shuffle, first, List.of()));
            var selected = store.findAllocation(shuffle).orElseThrow();
            var newer = attempt(1, 2).toBuilder().setQueryOrJobId(query).build();
            store.batchStore().commitAttempt(new ShuffleServiceModel.CommitCommand(shuffle, newer, List.of()));

            String key = ShufflePartitionManifest.objectKey(shuffle, new byte[32]);
            assertThatThrownBy(() -> store.beginManifestWrite(selected, key, 30_000))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("winner set changed");
            var fence = new ShuffleServiceModel.FinalizeFence(2, owner(), 1,
                    selected.shape().producerSetChecksum(), selected.metadataVersion());
            assertThatThrownBy(() -> store.finalizeShuffle(
                    new ShuffleServiceModel.FinalizeCommand(shuffle, fence, List.of())))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("changed during finalization");
        }
    }

    @Test
    void manifestUploadReservationProtectsCleanupAndSettlesByExactObject() throws Exception {
        String query = "manifest-writer-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        try (var store = PostgresShuffleStore.create(config().database())) {
            var allocation = store.register(registerCommand(query, shuffle, 1, System.currentTimeMillis() + 60_000));
            String key = ShufflePartitionManifest.objectKey(shuffle, new byte[32]);
            String reservation = store.beginManifestWrite(allocation, key, 60_000);
            var command = new CleanupCommand(query, owner());
            var lease = new CleanupLease("manifest-cleaner", 100, 200);
            assertThatThrownBy(() -> store.beginCleanup(command, lease))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("writes are still in progress");
            assertThatThrownBy(() -> store.settleManifestWrite(reservation, shuffle, key + ".other"))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("reservation");
            assertThatThrownBy(() -> store.beginManifestWrite(allocation, key, 60_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not active");
            store.settleManifestWrite(reservation, shuffle, key);
            store.completeCleanup(store.beginCleanup(command, lease).orElseThrow());
            assertThat(store.findAllocation(shuffle)).isEmpty();
        }
    }

    @Test
    void abandonedReaderBlocksCleanupAcrossRestartOnlyUntilItsDrainWindowEnds() throws Exception {
        String query = "reader-crash-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        long until = System.currentTimeMillis() + 60_000;
        UUID pin;
        try (var store = PostgresShuffleStore.create(config().database())) {
            finalizeEmptyShuffle(store, query, shuffle, until);
            pin = store.openRead(query, owner(), shuffle, 0, 0, until, 30_000).pin().id();
        }
        var command = new CleanupCommand(query, owner());
        var lease = new CleanupLease("reader-test", 100, 200);
        try (var successor = PostgresShuffleStore.create(config().database());
                var database = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword());
                var age = database.prepareStatement("UPDATE shuffle.shuffle_read_pin "
                        + "SET read_until_ms = 1, protect_until_ms = ? WHERE pin_id = ?")) {
            age.setObject(2, pin);
            age.setLong(1, until);
            assertThat(age.executeUpdate()).isEqualTo(1);
            assertThatThrownBy(() -> successor.grantReadWindow(query, owner(), pin))
                    .isInstanceOf(SecurityException.class);
            assertThatThrownBy(() -> successor.beginCleanup(command, lease))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("reads are still in progress");
            assertThatThrownBy(() -> successor.openRead(query, owner(), shuffle, 0, 0, until, 30_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not active");
            assertThat(successor.beginExpiredCleanup(lease)).isEmpty();

            age.setLong(1, 2);
            assertThat(age.executeUpdate()).isEqualTo(1);
            var plan = successor.beginCleanup(command, lease).orElseThrow();
            assertThatThrownBy(() -> successor.openRead(query, owner(), shuffle, 0, 0, until, 30_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not active");
            successor.completeCleanup(plan);
            successor.releaseRead(pin);
        }
    }

    @Test
    void wallElapsedTimeEndsLocalAdmissionAfterEarlyOwnerRevocation() throws Exception {
        String query = "read-clock-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        try (var store = PostgresShuffleStore.create(config().database());
             var database = DriverManager.getConnection(
                     POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var revoke = database.prepareStatement(
                     "UPDATE public.execution_anchor SET expires_at = clock_timestamp() WHERE execution_id = ?")) {
            var clock = java.time.Clock.systemUTC();
            long now = clock.millis();
            long until = now + 60_000;
            finalizeEmptyShuffle(store, query, shuffle, until);
            long started = System.nanoTime();
            var pin = store.openRead(query, owner(), shuffle, 0, 0, until, 30_000).pin();
            try (var lease = new ShuffleReadLease(store, query, owner(), pin, clock, started, now)) {
                revoke.setString(1, query);
                assertThat(revoke.executeUpdate()).isEqualTo(1);
                assertThatThrownBy(() -> lease.requireLive(now + ShuffleReadLease.WINDOW_MILLIS))
                        .isInstanceOf(SecurityException.class)
                        .hasMessageContaining("not the live durable anchor");
            }
            assertThat(protectedReaders(query)).isZero();
        }
    }

    @Test
    void terminalIntentFencesNewWritersAndReadWindowsBeforeCleanupIsDispatched() throws Exception {
        String query = "read-terminal-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        String writableShuffle = "writer-" + query;
        seedAnchor(query, owner());
        try (var store = PostgresShuffleStore.create(config().database());
             var database = DriverManager.getConnection(
                     POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var intent = database.prepareStatement("INSERT INTO shuffle.shuffle_cleanup_intent (query_id) VALUES (?)")) {
            long until = System.currentTimeMillis() + 60_000;
            finalizeEmptyShuffle(store, query, shuffle, until);
            store.register(registerCommand(query, writableShuffle, 2, until));
            var pin = store.openRead(query, owner(), shuffle, 0, 0, until, 30_000).pin();
            intent.setString(1, query);
            assertThat(intent.executeUpdate()).isEqualTo(1);
            assertThatThrownBy(() -> store.grantReadWindow(query, owner(), pin.id()))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("terminally fenced");
            assertThatThrownBy(() -> store.openRead(query, owner(), shuffle, 0, 0, until, 30_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("terminally fenced");
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var writer = producer.toBuilder().setStageId(2).build();
            assertThatThrownBy(() -> store.batchStore().openWriteWindow(writableShuffle, writer, 30_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("terminally fenced");
            store.releaseRead(pin.id());
            var plan = store.beginCleanup(new CleanupCommand(query, owner()),
                    new CleanupLease("terminal-reader", 100, 200)).orElseThrow();
            store.completeCleanup(plan);
        }
    }

    @Test
    void exactTaskGrantChecksTheDurableAllocationAndOwnerOnce() throws Exception {
        String query = "task-grant-statements-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        var harness = openHarness();
        try (var engine = harness.engine) {
            var registration = engine.register(registration().toBuilder().setQueryId(query).setShuffleId(shuffle)
                    .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(
                            ShuffleProducerSetChecksum.forContiguousPartitions(query, 1, 2, 1))).build());
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var consumer = consumerAttempt().toBuilder().setQueryOrJobId(query).build();
            var write = org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE;
            var read = org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ;

            var before = shuffleStatements();
            var writeGrant = taskGrant(harness, registration, producer, write);
            assertExactTaskGrantStatements(before);
            assertThat(writeGrant.getToken()).isNotEmpty();
            var readGrant = taskGrant(harness, registration, consumer, read);
            before = shuffleStatements();
            var renewal = org.pangrova.protocol.v2.shuffle.RenewAccessRequest.newBuilder()
                    .setOperation(read).setShuffleId(shuffle).setExecutionOwner(owner()).setTaskAttempt(consumer)
                    .setPreviousCapability(readGrant).build();
            var renewed = engine.renewAccess(renewal, false);
            assertExactTaskGrantStatements(before);
            assertThat(renewed.getAccessCapability().getToken()).isNotEmpty();
            before = shuffleStatements();
            assertThatThrownBy(() -> engine.renewAccess(renewal.toBuilder()
                    .setTaskAttempt(consumer.toBuilder().setTaskId("different-consumer")).build(), false))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("capability scope");
            assertShuffleStatementCount(before, 0);
            assertThatThrownBy(() -> taskGrant(harness, registration, producer.toBuilder().setPlanEpoch(1).build(), write))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("producer attempt");
        }
    }

    private static void assertExactTaskGrantStatements(Map<String, Long> before) throws Exception {
        var changes = shuffleStatementDelta(before);
        assertThat(changes.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(5);
        assertThat(changes.entrySet().stream().filter(entry -> entry.getKey().startsWith("SELECT"))
                .filter(entry -> entry.getKey().contains("FROM public.execution_anchor"))
                .mapToLong(Map.Entry::getValue).sum()).isOne();
        assertThat(changes.entrySet().stream().filter(entry -> entry.getKey().startsWith("SELECT"))
                .filter(entry -> entry.getKey().contains("FROM shuffle_allocation a"))
                .mapToLong(Map.Entry::getValue).sum()).isOne();
    }

    @Test
    void acknowledgesOneWriteWindowForThirtyTwoUploadedBatches() throws Exception {
        String query = "batch-statements-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        var harness = openHarness();
        try (var engine = harness.engine) {
            var registration = engine.register(registration().toBuilder()
                    .setQueryId(query).setShuffleId(shuffle).setExpectedProducerSetChecksum(
                            ShuffleChecksums.protocol(ShuffleProducerSetChecksum.forContiguousPartitions(
                                    query, 1, 2, 1))).build());
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var grant = taskGrant(harness, registration, producer, org.pangrova.protocol.v2.shuffle
                    .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            byte[] payload = {1, 2, 3, 4};
            byte[] checksum = ShuffleChecksums.sha256(payload);
            ByteString canonical = ByteString.copyFrom(payload);
            int batchCount = 32;
            var before = shuffleStatements();
            try (var rpc = WriteRpc.open(engine)) {
                for (int sequence = 0; sequence < batchCount; sequence++) {
                    rpc.send(new CompleteWriteBatch(shuffle, 0, sequence, 1, producer, grant, canonical, checksum));
                }
                var acknowledgement = rpc.finish();
                assertThat(acknowledgement.getTotalBatchesAccepted()).isEqualTo(batchCount);
                assertThat(acknowledgement.getTotalBytesAccepted()).isEqualTo((long) batchCount * payload.length);
                assertThat(acknowledgement.getPartitions(0).getLastPartitionSequence()).isEqualTo(batchCount - 1L);
            }
            assertShuffleStatementCount(before, 8);

            var manifest = ShufflePartitionManifestChecksum.accumulator();
            for (int sequence = 0; sequence < batchCount; sequence++) {
                manifest.add(sequence, payload.length, 1, checksum);
            }
            try (var store = PostgresShuffleStore.create(config().database())) {
                assertThat(store.findAllocation(shuffle).orElseThrow().metadataVersion()).isZero();
                var commit = new ShuffleServiceModel.CommitCommand(shuffle, producer,
                        List.of(new ShuffleServiceModel.PartitionExpectation(0, batchCount - 1,
                                batchCount * payload.length, batchCount, batchCount, manifest.finishNonEmpty())));
                store.batchStore().commitAttempt(commit);
                store.batchStore().commitAttempt(commit);
                assertThat(store.findAllocation(shuffle).orElseThrow().metadataVersion()).isEqualTo(1);
                var replay = new CompleteWriteBatch(shuffle, 0, 0, 1, producer, grant, canonical, checksum);
                acceptSingle(engine, replay);
                assertThatThrownBy(() -> acceptSingle(engine, new CompleteWriteBatch(
                        shuffle, 0, 0, 2, producer, grant, canonical, checksum)))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("batch identity conflicts");
                assertThatThrownBy(() -> acceptSingle(engine, new CompleteWriteBatch(
                        shuffle, 0, batchCount, 1, producer, grant, canonical, checksum)))
                        .isInstanceOf(SecurityException.class)
                        .hasMessageContaining("fenced by committed winner");
                var challenger = attempt(1, 2).toBuilder().setQueryOrJobId(query).build();
                var challengerGrant = taskGrant(harness, registration, challenger, org.pangrova.protocol.v2.shuffle
                        .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
                assertThatThrownBy(() -> acceptSingle(engine, new CompleteWriteBatch(
                        shuffle, 0, 1, 1, challenger, challengerGrant, canonical, checksum)))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("sequence is not contiguous");
                acceptSingle(engine, new CompleteWriteBatch(
                        shuffle, 0, 0, 1, challenger, challengerGrant, canonical, checksum));
                assertThat(store.batchStore().readCommittedPage(
                        store.findAllocation(shuffle).orElseThrow(), null, batchCount + 1))
                        .hasSize(batchCount);
                assertThat(store.findAllocation(shuffle).orElseThrow().metadataVersion()).isEqualTo(1);
            }
        }
    }

    @Test
    void cancelsKnownUploadedObjectsWithoutPublishingOrLeavingTheReservation() throws Exception {
        String query = "cancel-write-window-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        var harness = openHarness();
        try (var engine = harness.engine; var objects = s3Client();
             var store = PostgresShuffleStore.create(config().database()); var rpc = WriteRpc.open(engine)) {
            var registration = engine.register(registration().toBuilder()
                    .setQueryId(query).setShuffleId(shuffle).setExpectedProducerSetChecksum(
                            ShuffleChecksums.protocol(ShuffleProducerSetChecksum.forContiguousPartitions(
                                    query, 1, 2, 1))).build());
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var grant = taskGrant(harness, registration, producer, org.pangrova.protocol.v2.shuffle
                    .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            byte[] payload = {1, 2, 3, 4};
            rpc.send(new CompleteWriteBatch(shuffle, 0, 0, 1, producer, grant,
                    ByteString.copyFrom(payload), ShuffleChecksums.sha256(payload)));
            var prefix = ListObjectsV2Request.builder().bucket(BUCKET).prefix("integration/" + shuffle + "/").build();
            awaitCondition(() -> objects.listObjectsV2(prefix).contents().size() == 1);
            assertThat(writeReservations(shuffle)).isEqualTo(1);
            assertThat(store.batchStore().readAttemptPage(shuffle, producer, 0, -1, 4)).isEmpty();

            rpc.cancel();
            awaitCondition(() -> writeReservations(shuffle) == 0);
            assertThat(store.batchStore().readAttemptPage(shuffle, producer, 0, -1, 4)).isEmpty();
            engine.cleanup(CleanupRequest.newBuilder().setQueryId(query).setExecutionOwner(owner())
                    .setAccessCapability(registration.getCleanupCapability()).build());
            assertThat(objects.listObjectsV2(prefix).contents()).isEmpty();
        }
    }

    @Test
    void rejectsTheWholeWindowWhenOneNewBatchSkipsItsPartitionFrontier() throws Exception {
        String query = "gap-write-window-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        var harness = openHarness();
        try (var engine = harness.engine;
             var store = PostgresShuffleStore.create(config().database()); var rpc = WriteRpc.open(engine)) {
            var registration = engine.register(registration().toBuilder()
                    .setQueryId(query).setShuffleId(shuffle).setExpectedProducerSetChecksum(
                            ShuffleChecksums.protocol(ShuffleProducerSetChecksum.forContiguousPartitions(
                                    query, 1, 2, 1))).build());
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var grant = taskGrant(harness, registration, producer, org.pangrova.protocol.v2.shuffle
                    .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            byte[] payload = {1, 2, 3, 4};
            for (long sequence : List.of(0L, 2L)) {
                rpc.send(new CompleteWriteBatch(shuffle, 0, sequence, 1, producer, grant,
                        ByteString.copyFrom(payload), ShuffleChecksums.sha256(payload)));
            }
            assertThatThrownBy(rpc::finish).hasRootCauseMessage(
                    "FAILED_PRECONDITION: partition batch sequence is not contiguous");
            assertThat(store.batchStore().readAttemptPage(shuffle, producer, 0, -1, 4)).isEmpty();
            awaitCondition(() -> writeReservations(shuffle) == 0);
        }
    }

    @Test
    void blockedAllocationDoesNotSerializeWritesToAnotherStageOfTheQuery() throws Exception {
        String query = "parallel-stage-writes-" + UUID.randomUUID();
        String firstShuffle = "first-" + query;
        String secondShuffle = "second-" + query;
        seedAnchor(query, owner());
        var executor = Executors.newFixedThreadPool(2);
        try (var store = PostgresShuffleStore.create(config().database());
             var blocker = DriverManager.getConnection(
                     POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var monitor = DriverManager.getConnection(
                     POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            long until = System.currentTimeMillis() + 60_000;
            store.register(registerCommand(query, firstShuffle, 1, until));
            store.register(registerCommand(query, secondShuffle, 2, until));
            var first = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var second = first.toBuilder().setStageId(2).setTaskId("second-stage-producer").build();
            var firstIdentity = new BatchIdentity(firstShuffle, 0, 0, 0, 0);
            var secondIdentity = new BatchIdentity(secondShuffle, 0, 0, 0, 0);
            var firstBatch = new StoredBatch(firstIdentity, first, firstIdentity.objectKey(), 4, 1, new byte[32]);
            var secondBatch = new StoredBatch(secondIdentity, second, secondIdentity.objectKey(), 4, 1, new byte[32]);
            blocker.setAutoCommit(false);
            int blockerPid;
            try (var statement = blocker.prepareStatement(
                    "SELECT pg_backend_pid() FROM shuffle.shuffle_allocation WHERE shuffle_id = ? FOR UPDATE")) {
                statement.setString(1, firstShuffle);
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    blockerPid = row.getInt(1);
                }
            }
            try {
                var blocked = executor.submit(() -> store.batchStore().openWriteWindow(firstShuffle, first, 60_000));
                assertThat(awaitBlockedWriter(monitor, blockerPid)).isTrue();
                var parallel = executor.submit(() -> store.batchStore().openWriteWindow(secondShuffle, second, 60_000));
                store.batchStore().publishWriteWindow(
                        parallel.get(5, TimeUnit.SECONDS), secondShuffle, second, List.of(secondBatch));
                assertThat(blocked.isDone()).isFalse();
                blocker.commit();
                store.batchStore().publishWriteWindow(
                        blocked.get(5, TimeUnit.SECONDS), firstShuffle, first, List.of(firstBatch));
            } finally {
                blocker.rollback();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void persistsExactWorkerAttemptAndRejectsOtherIncarnationAfterRestart() throws Exception {
        String queryId = "worker-identity-query";
        String shuffleId = "worker-identity-shuffle";
        seedAnchor(queryId, owner());
        var producer = attempt(0, 1).toBuilder().setQueryOrJobId(queryId).build();
        var other = producer.toBuilder().setWorkerExecutionIdentity(
                producer.getWorkerExecutionIdentity().toBuilder()
                        .setIncarnationId("00000000-0000-0000-0000-000000000003")).build();
        var identity = new BatchIdentity(shuffleId, 0, 0, 0, 0);
        var batch = new StoredBatch(identity, producer, identity.objectKey(), 4, 1, new byte[32]);
        var summary = new ShuffleServiceModel.PartitionExpectation(0, 0, 4, 1, 1, new byte[32]);
        try (var store = PostgresShuffleStore.create(config().database())) {
            store.register(registerCommand(queryId, shuffleId, 1, System.currentTimeMillis() + 60_000));
            var reservation = store.batchStore().openWriteWindow(shuffleId, producer, 60_000);
            assertThatThrownBy(() -> store.batchStore().openWriteWindow(shuffleId, other, 60_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("attempt identity conflicts");
            store.batchStore().publishWriteWindow(reservation, shuffleId, producer, List.of(batch));
            store.batchStore().commitAttempt(new ShuffleServiceModel.CommitCommand(
                    shuffleId, producer, List.of(summary)));
        }
        try (var restarted = PostgresShuffleStore.create(config().database())) {
            assertThat(restarted.batchStore().readCommittedPage(
                    restarted.findAllocation(shuffleId).orElseThrow(), null, 10))
                    .singleElement().extracting(StoredBatch::producerAttempt).isEqualTo(producer);
            assertThatThrownBy(() -> restarted.batchStore().readAttemptPage(shuffleId, other, 0, -1, 10))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("durable identity");
            assertThatThrownBy(() -> restarted.batchStore().commitAttempt(
                    new ShuffleServiceModel.CommitCommand(shuffleId, other, List.of(summary))))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("attempt identity conflicts");
            // A migrated legacy row has no recoverable target; it is never silently filled from a requester.
            try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                    var statement = connection.prepareStatement(
                            "UPDATE shuffle.shuffle_attempt_fence SET canonical_attempt = NULL WHERE shuffle_id = ?")) {
                statement.setString(1, shuffleId);
                statement.executeUpdate();
            }
            assertThatThrownBy(() -> restarted.batchStore().readCommittedPage(
                    restarted.findAllocation(shuffleId).orElseThrow(), null, 10))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Legacy shuffle attempt");
            assertThatThrownBy(() -> restarted.batchStore().openWriteWindow(shuffleId, producer, 60_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("attempt identity conflicts");
            var cleanup = restarted.beginCleanup(new CleanupCommand(queryId, owner()),
                    new CleanupLease("legacy-cleaner", 100, 200)).orElseThrow();
            restarted.completeCleanup(cleanup);
        }
    }

    @Test
    void renewsQueryCleanupExpiryWhenANewStageRegisters() throws Exception {
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            var first = store.register(registerCommand(
                    "ttl-query", "ttl-stage-1", 1, 1_000));
            var second = store.register(registerCommand(
                    "ttl-query", "ttl-stage-2", 2, 5_000));

            assertThat(second.cleanupExpiresAtMs()).isEqualTo(5_000);
            assertThat(store.findAllocation(first.identity().shuffleId()).orElseThrow()
                    .cleanupExpiresAtMs()).isEqualTo(5_000);

            var plan = store.beginCleanup(new CleanupCommand("ttl-query", owner()),
                    new CleanupLease("ttl-cleaner", 6_000, 7_000)).orElseThrow();
            store.completeCleanup(plan);
        }
    }

    @Test
    void pagesLargeCleanupAndRecoversOnlyAfterLeaseExpiry() throws Exception {
        String queryId = "large-cleanup-query";
        String shuffleId = "large-cleanup-shuffle";
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            store.register(registerCommand(queryId, shuffleId, 1, 10_000));
            insertBatches(shuffleId, 2_505);

            var firstPlan = store.beginCleanup(new CleanupCommand(queryId, owner()),
                    new CleanupLease("first-cleaner", 100, 200)).orElseThrow();
            assertThat(firstPlan.bytes()).isEqualTo(2_505);
            assertThat(firstPlan.partitionCount()).isEqualTo(4);
            assertThat(store.beginExpiredCleanup(
                    new CleanupLease("early-cleaner", 199, 300))).isEmpty();

            var recovered = store.beginExpiredCleanup(
                    new CleanupLease("recovery-cleaner", 201, 1_000)).orElseThrow();
            var cursor = (ShuffleServiceModel.CleanupCursor) null;
            List<Integer> pageSizes = new ArrayList<>();
            int objectCount = 0;
            while (true) {
                var page = store.cleanupObjects(recovered, cursor, 1_000,
                        new CleanupLease("recovery-cleaner", 202, 1_000));
                if (page.objectKeys().isEmpty()) {
                    break;
                }
                pageSizes.add(page.objectKeys().size());
                objectCount += page.objectKeys().size();
                cursor = page.nextCursor();
            }
            assertThat(pageSizes).containsExactly(1_000, 1_000, 505);
            assertThat(objectCount).isEqualTo(2_505);
            store.completeCleanup(recovered);
            assertThat(store.findAllocation(shuffleId)).isEmpty();
        }
    }

    @Test
    void takesOverOnlyWithTheLiveDurableAnchorAndFencesTheOldOwner() throws Exception {
        String queryId = "anchor-takeover-query";
        seedAnchor(queryId, owner());
        ExecutionOwnerIdentity successor = ExecutionOwnerIdentity.newBuilder()
                .setOwnerId("coordinator-2").setOwnerIncarnation("boot-2")
                .setFencingToken(2).build();
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            RegisterCommand original = registerCommand(queryId, "anchor-takeover-shuffle", 1,
                    10_000);
            store.register(original);

            assertThatThrownBy(() -> store.register(new RegisterCommand(
                    original.requestedShuffleId(), queryId, 2, 2, successor,
                    original.shape(), 20_000)))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("live durable anchor");

            replaceAnchor(queryId, successor);
            assertThatThrownBy(() -> store.requireLiveOwner(queryId, owner()))
                    .isInstanceOf(SecurityException.class);
            assertThatThrownBy(() -> store.findAllocation(original.requestedShuffleId()))
                    .isInstanceOf(SecurityException.class);
            assertThatThrownBy(() -> store.register(original))
                    .isInstanceOf(SecurityException.class);

            store.recoverCleanupAccess(queryId, successor,
                    System.currentTimeMillis() + 60_000);
            assertThat(store.findAllocation(original.requestedShuffleId()).orElseThrow()
                    .owner()).isEqualTo(successor);
            assertThatThrownBy(() -> store.beginCleanup(
                    new CleanupCommand(queryId, owner()),
                    new CleanupLease("old-cleaner", 100, 200)))
                    .isInstanceOf(SecurityException.class);
            var plan = store.beginCleanup(new CleanupCommand(queryId, successor),
                    new CleanupLease("new-cleaner", 100, 200)).orElseThrow();
            store.completeCleanup(plan);
        }
    }

    @Test
    void writeReservationFencesCleanupUntilObjectWriteHasSettled() throws Exception {
        String queryId = "write-intent-query";
        String shuffleId = "write-intent-shuffle";
        seedAnchor(queryId, owner());
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            store.register(registerCommand(queryId, shuffleId, 1, 10_000));
            TaskAttemptId producer = TaskAttemptId.newBuilder()
                    .setQueryOrJobId(queryId).setStageId(1).setPlanEpoch(2)
                    .setPartitionId(0).setAttemptId(0).setFencingToken(1)
                    .setTaskId("write-intent-producer").setControlEpoch(1)
                    .setExecutionOwner(owner()).setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                        .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
            var reservation = store.batchStore().openWriteWindow(shuffleId, producer, 300_000);
            assertThatThrownBy(() -> store.beginCleanup(
                    new CleanupCommand(queryId, owner()),
                    new CleanupLease("first-cleaner", 100, 200)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("writes are still in progress");
            assertThatThrownBy(() -> store.batchStore().openWriteWindow(shuffleId, producer, 300_000))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not writable");
            ExecutionOwnerIdentity successor = owner().toBuilder()
                    .setOwnerId("write-successor").setOwnerIncarnation("successor-boot")
                    .setFencingToken(owner().getFencingToken() + 1).build();
            replaceAnchor(queryId, successor);
            long renewedUntil = System.currentTimeMillis() + 60_000;
            store.recoverCleanupAccess(queryId, successor, renewedUntil);
            store.renewAccess(queryId, "", successor, renewedUntil);
            assertThatThrownBy(() -> store.renewAccess(queryId, shuffleId, successor, renewedUntil))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("not active");
            assertThatThrownBy(() -> store.beginCleanup(new CleanupCommand(queryId, successor),
                    new CleanupLease("waiting-successor", 100, 200)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("writes are still in progress");
            store.batchStore().settleWriteWindow(reservation, shuffleId, producer);
            var plan = store.beginCleanup(new CleanupCommand(queryId, successor),
                    new CleanupLease("second-cleaner", 100, 200)).orElseThrow();
            store.completeCleanup(plan);
        }
    }

    @Test
    void cleanupDefersTombstoningAnExpiredWindowWhileAnotherUploadIsProtected() throws Exception {
        String query = "mixed-write-reservations-" + UUID.randomUUID();
        String shuffle = "shuffle-" + query;
        seedAnchor(query, owner());
        try (var store = PostgresShuffleStore.create(config().database());
             var connection = DriverManager.getConnection(
                     POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var expire = connection.prepareStatement("UPDATE shuffle.shuffle_write_intent "
                     + "SET recovery_after = clock_timestamp() - INTERVAL '1 second' "
                     + "WHERE reservation_id = ?::uuid")) {
            store.register(registerCommand(query, shuffle, 1, 60_000));
            var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
            var expired = store.batchStore().openWriteWindow(shuffle, producer, 120_000);
            var active = store.batchStore().openWriteWindow(shuffle, producer, 120_000);
            expire.setString(1, expired.reservation());
            assertThat(expire.executeUpdate()).isEqualTo(1);

            var cleanup = new CleanupCommand(query, owner());
            var lease = new CleanupLease("mixed-reservation-cleaner", 1000, 2000);
            assertThatThrownBy(() -> store.beginCleanup(cleanup, lease))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("writes are still in progress");
            assertThat(hasTombstone(shuffle)).isFalse();

            store.batchStore().settleWriteWindow(active, shuffle, producer);
            var plan = store.beginCleanup(cleanup, lease).orElseThrow();
            assertThat(hasTombstone(shuffle)).isTrue();
            store.completeCleanup(plan);
            deleteTombstone(shuffle);
        }
    }

    @Test
    void terminalIntentFencesLateRegistrationEvenWithoutShuffleScope() throws Exception {
        String queryId = "terminal-before-register-query";
        seedAnchor(queryId, owner());
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database());
                var connection = DriverManager.getConnection(
                        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement(
                        "INSERT INTO shuffle.shuffle_cleanup_intent (query_id) VALUES (?)")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
            assertThatThrownBy(() -> store.register(registerCommand(
                    queryId, "terminal-before-register-shuffle", 1, 10_000)))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("terminally fenced");
            store.recoverCleanupAccess(queryId, owner(), System.currentTimeMillis() + 60_000);
            assertThat(store.beginCleanup(new CleanupCommand(queryId, owner()),
                    new CleanupLease("terminal-cleaner", 100, 200))).isEmpty();
            try (var check = connection.prepareStatement(
                    "SELECT 1 FROM shuffle.shuffle_cleanup_intent WHERE query_id = ?")) {
                check.setString(1, queryId);
                try (var row = check.executeQuery()) {
                    assertThat(row.next()).isFalse();
                }
            }
        }
    }

    @Test
    void crashExpiredWriteIsTombstonedAndLateObjectsAreSweptAfterRestart() throws Exception {
        String queryId = "uncertain-write-query";
        String shuffleId = "uncertain-write-shuffle";
        String objectKey = shuffleId + "/late.bin";
        String manifestKey = ShufflePartitionManifest.objectKey(shuffleId, new byte[32]);
        seedAnchor(queryId, owner());
        Harness first = openHarness();
        var registration = first.engine.register(RegisterShuffleRequest.newBuilder()
                .setQueryId(queryId).setShuffleId(shuffleId).setStageId(1)
                .setNumPartitions(1).setProtocol(CanonicalProtocolIdentity.current())
                .setPlanEpoch(2).setExecutionOwner(owner())
                .setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(
                        ShuffleProducerSetChecksum.forContiguousPartitions(queryId, 1, 2, 1)))
                .build());
        TaskAttemptId producer = TaskAttemptId.newBuilder()
                .setQueryOrJobId(queryId).setStageId(1).setPlanEpoch(2)
                .setPartitionId(0).setAttemptId(0).setFencingToken(1)
                .setTaskId("uncertain-producer").setControlEpoch(1)
                .setExecutionOwner(owner()).setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                        .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            store.batchStore().openWriteWindow(shuffleId, producer, 300_000);
            store.beginManifestWrite(store.findAllocation(shuffleId).orElseThrow(), manifestKey, 300_000);
            assertThatThrownBy(() -> first.engine.cleanup(CleanupRequest.newBuilder()
                    .setQueryId(queryId).setExecutionOwner(owner())
                    .setAccessCapability(registration.getCleanupCapability()).build()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("writes are still in progress");
        }
        first.engine.close();
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var expire = connection.prepareStatement(
                        "UPDATE shuffle.shuffle_write_intent "
                                + "SET recovery_after = clock_timestamp() - INTERVAL '1 second' "
                                + "WHERE shuffle_id = ?")) {
            expire.setString(1, shuffleId);
            assertThat(expire.executeUpdate()).isEqualTo(2);
        }
        Harness restarted = openHarness();
        restarted.engine.cleanup(CleanupRequest.newBuilder()
                .setQueryId(queryId).setExecutionOwner(owner())
                .setAccessCapability(registration.getCleanupCapability()).build());
        assertThat(restarted.engine.sweepUncertainWritePrefixes(1)).isEqualTo(1);
        try (S3Client client = s3Client()) {
            for (String key : List.of(objectKey, manifestKey)) {
                client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("integration/" + key).build(),
                        RequestBody.fromBytes("late".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            }
            assertThat(client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET)
                    .prefix("integration/" + shuffleId + "/").build()).contents()).hasSize(2);
        }
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var due = connection.prepareStatement(
                        "UPDATE shuffle.shuffle_uncertain_write_tombstone "
                                + "SET next_sweep_at = clock_timestamp() - INTERVAL '1 second' "
                                + "WHERE shuffle_id = ?")) {
            due.setString(1, shuffleId);
            assertThat(due.executeUpdate()).isEqualTo(1);
        }
        assertThat(restarted.engine.sweepUncertainWritePrefixes(1)).isEqualTo(1);
        try (S3Client client = s3Client()) {
            assertThat(client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET)
                    .prefix("integration/" + shuffleId + "/").build()).contents()).isEmpty();
        }
        restarted.engine.close();
        String freshQueryId = "uncertain-write-new-query";
        seedAnchor(freshQueryId, owner());
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            assertThatThrownBy(() -> store.register(registerCommand(
                    freshQueryId, shuffleId, 1, 10_000)))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("remains fenced");
        }
        deleteTombstone(shuffleId);
    }

    @Test
    void uncertainSweepLeaseFencesCompetingServiceInstances() throws Exception {
        String shuffleId = "uncertain-lease-" + UUID.randomUUID();
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var insert = connection.prepareStatement(
                        "INSERT INTO shuffle.shuffle_uncertain_write_tombstone "
                                + "(shuffle_id) VALUES (?)")) {
            insert.setString(1, shuffleId);
            insert.executeUpdate();
        }
        String firstOwner = UUID.randomUUID().toString();
        String secondOwner = UUID.randomUUID().toString();
        try (PostgresShuffleStore first = PostgresShuffleStore.create(config().database());
                PostgresShuffleStore second = PostgresShuffleStore.create(config().database())) {
            assertThat(first.claimUncertainSweep(firstOwner, 300_000))
                    .contains(shuffleId);
            assertThat(second.claimUncertainSweep(secondOwner, 300_000)).isEmpty();
            try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                    var expire = connection.prepareStatement(
                            "UPDATE shuffle.shuffle_uncertain_write_tombstone "
                                    + "SET lease_expires_at = "
                                    + "clock_timestamp() - INTERVAL '1 second' "
                                    + "WHERE shuffle_id = ?")) {
                expire.setString(1, shuffleId);
                assertThat(expire.executeUpdate()).isEqualTo(1);
            }
            assertThat(second.claimUncertainSweep(secondOwner, 300_000))
                    .contains(shuffleId);
            assertThatThrownBy(() -> first.renewUncertainSweep(
                    shuffleId, firstOwner, 300_000))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("lease was lost");
            second.completeUncertainSweep(shuffleId, secondOwner, 30_000);
            assertThat(first.claimUncertainSweep(firstOwner, 300_000)).isEmpty();
        }
        deleteTombstone(shuffleId);
    }

    @Test
    void rejectsShuffleServiceWithDifferentObjectNamespace() throws Exception {
        try (PostgresShuffleStore first = PostgresShuffleStore.create(config().database());
                PostgresShuffleStore second = PostgresShuffleStore.create(config().database())) {
            first.requireObjectNamespace(config().objectStorage());
            second.requireObjectNamespace(config().objectStorage());
            var configured = config().objectStorage();
            var wrong = new ShuffleServiceConfig.ObjectStorage(
                    configured.bucket(), "different-prefix", configured.region(),
                    configured.endpoint(), configured.pathStyleAccess(),
                    configured.replicationEnabled());
            assertThatThrownBy(() -> second.requireObjectNamespace(wrong))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("differs from durable metadata authority");
        }
    }

    @Test
    void upgradesLiveVersionFourReservationWithoutLosingItsRecoveryFence() throws Exception {
        String databaseName = "shuffle_upgrade_"
                + UUID.randomUUID().toString().replace("-", "");
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
        String jdbcUrl = POSTGRES.getJdbcUrl().replaceFirst("/[^/]+$", "/" + databaseName);
        createExecutionAuthority(jdbcUrl);
        Flyway.configure().dataSource(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas("shuffle").defaultSchema("shuffle")
                .locations("classpath:org/pangrova/shuffle/migration")
                .target(MigrationVersion.fromVersion("4")).load().migrate();
        String shuffleId = "upgrade-reservation";
        try (var connection = DriverManager.getConnection(
                jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("INSERT INTO shuffle.shuffle_query_scope "
                    + "(query_id, owner_id, owner_incarnation, owner_fencing_token, "
                    + "cleanup_expires_at_ms) VALUES "
                    + "('upgrade-query', 'owner', 'incarnation', 1, 100000)");
            statement.execute("INSERT INTO shuffle.shuffle_allocation "
                    + "(shuffle_id, query_id, stage_id, plan_epoch, partition_count, "
                    + "expected_producer_count, producer_set_checksum, "
                    + "capability_expires_at_ms, state) VALUES "
                    + "('upgrade-reservation', 'upgrade-query', 1, 1, 1, 1, "
                    + "decode(repeat('00', 32), 'hex'), 100000, 'REGISTERED')");
            statement.execute("INSERT INTO shuffle.shuffle_attempt_fence "
                    + "(shuffle_id, producer_partition_id, producer_attempt_id, producer_fencing_token, task_id, "
                    + "control_epoch) VALUES ('upgrade-reservation', 0, 0, 1, 'legacy-task', 1)");
            statement.execute("INSERT INTO shuffle.shuffle_write_intent "
                    + "(reservation_id, shuffle_id, object_key, state) VALUES "
                    + "('00000000-0000-0000-0000-000000000001', "
                    + "'upgrade-reservation', 'upgrade-reservation/late.bin', 'IN_PROGRESS')");
        }
        var database = new ShuffleServiceConfig.Database(
                jdbcUrl, POSTGRES.getUsername(), passwordFile, 4);
        try (PostgresShuffleStore ignored = PostgresShuffleStore.create(database);
                var connection = DriverManager.getConnection(
                        jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement(
                        "SELECT recovery_after > clock_timestamp() FROM "
                                + "shuffle.shuffle_write_intent WHERE shuffle_id = ?")) {
            statement.setString(1, shuffleId);
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getBoolean(1)).isTrue();
            }
            try (var legacy = connection.createStatement();
                    var row = legacy.executeQuery("SELECT canonical_attempt IS NULL FROM "
                            + "shuffle.shuffle_attempt_fence WHERE shuffle_id = 'upgrade-reservation'")) {
                assertThat(row.next()).isTrue();
                assertThat(row.getBoolean(1)).isTrue();
            }
            try (var table = connection.createStatement();
                    var row = table.executeQuery(
                            "SELECT 1 FROM shuffle.shuffle_storage_namespace WHERE FALSE")) {
                assertThat(row.next()).isFalse();
            }
        }
    }

    private static void deleteTombstone(String shuffleId) throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var delete = connection.prepareStatement(
                        "DELETE FROM shuffle.shuffle_uncertain_write_tombstone "
                                + "WHERE shuffle_id = ?")) {
            delete.setString(1, shuffleId);
            delete.executeUpdate();
        }
    }

    private static RegisterCommand registerCommand(
            String queryId,
            String shuffleId,
            int stageId,
            long expiresAtMs) {
        return new RegisterCommand(shuffleId, queryId, stageId, 2, owner(),
                new AllocationShape(4, 1, new byte[32]), expiresAtMs);
    }

    private static void finalizeEmptyShuffle(
            PostgresShuffleStore store, String query, String shuffle, long expiresAt) {
        var registration = registerCommand(query, shuffle, 1, expiresAt);
        store.register(registration);
        var producer = attempt(0, 1).toBuilder().setQueryOrJobId(query).build();
        store.batchStore().commitAttempt(new ShuffleServiceModel.CommitCommand(shuffle, producer, List.of()));
        var fence = new ShuffleServiceModel.FinalizeFence(2, owner(), 1,
                registration.shape().producerSetChecksum(), store.findAllocation(shuffle).orElseThrow().metadataVersion());
        List<ShuffleServiceModel.PartitionPublication> partitions = new ArrayList<>();
        for (int partition = 0; partition < registration.shape().partitionCount(); partition++) {
            var summary = new ShuffleServiceModel.FinalizedPartition(partition, 0, 0, -1,
                    ShufflePartitionManifestChecksum.accumulator().finish());
            partitions.add(new ShuffleServiceModel.PartitionPublication(summary, new byte[0]));
        }
        store.finalizeShuffle(new ShuffleServiceModel.FinalizeCommand(shuffle, fence, partitions));
    }

    private static long protectedReaders(String query) throws Exception {
        try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.prepareStatement(
                     "SELECT count(*) FROM public.execution_read_window WHERE execution_id = ?")) {
            statement.setString(1, query);
            try (var row = statement.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        }
    }

    private static long writeReservations(String shuffle) throws Exception {
        try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.prepareStatement(
                     "SELECT count(*) FROM shuffle.shuffle_write_intent WHERE shuffle_id = ?")) {
            statement.setString(1, shuffle);
            try (var row = statement.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        }
    }

    private static boolean hasTombstone(String shuffle) throws Exception {
        try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.prepareStatement(
                     "SELECT 1 FROM shuffle.shuffle_uncertain_write_tombstone WHERE shuffle_id = ?")) {
            statement.setString(1, shuffle);
            try (var row = statement.executeQuery()) {
                return row.next();
            }
        }
    }

    private static void awaitCondition(Callable<Boolean> condition) throws Exception {
        var poller = Executors.newSingleThreadScheduledExecutor();
        var observed = new CompletableFuture<Void>();
        poller.scheduleWithFixedDelay(() -> {
            try {
                if (condition.call()) {
                    observed.complete(null);
                }
            } catch (Exception failure) {
                observed.completeExceptionally(failure);
            }
        }, 0, 20, TimeUnit.MILLISECONDS);
        try {
            observed.get(5, TimeUnit.SECONDS);
        } finally {
            poller.shutdownNow();
        }
    }

    private static long performedReadWindows() throws Exception {
        try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.prepareStatement(
                     "SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements WHERE query LIKE ?")) {
            statement.setString(1, "WITH pinned AS MATERIALIZED%INSERT INTO public.execution_read_window%");
            try (var row = statement.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        }
    }

    private static void assertShuffleStatementCount(Map<String, Long> before, long expected) throws Exception {
        Map<String, Long> changes = shuffleStatementDelta(before);
        assertThat(changes.values().stream().mapToLong(Long::longValue).sum())
                .as("Shuffle SQL statement delta %s", changes)
                .isEqualTo(expected);
    }

    private static Map<String, Long> shuffleStatementDelta(Map<String, Long> before) throws Exception {
        Map<String, Long> changes = shuffleStatements();
        changes.replaceAll((query, calls) -> calls - before.getOrDefault(query, 0L));
        changes.values().removeIf(calls -> calls == 0);
        return changes;
    }

    private static byte[] sequencePayload(long sequence) {
        return ByteBuffer.allocate(Long.BYTES).putLong(sequence).array();
    }

    private static Map<String, Long> shuffleStatements() throws Exception {
        String sql = """
                SELECT query, SUM(calls) FROM pg_stat_statements
                WHERE toplevel
                  AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query ~ '^(SELECT|WITH|INSERT|UPDATE|DELETE)[[:space:]]'
                  AND (strpos(query, 'shuffle_') > 0 OR strpos(query, 'public.execution_anchor') > 0)
                  AND query NOT LIKE '%pg_stat_statements%'
                GROUP BY query
                """;
        try (var connection = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement();
             var row = statement.executeQuery(sql)) {
            Map<String, Long> result = new HashMap<>();
            while (row.next()) {
                result.put(row.getString(1), row.getLong(2));
            }
            return result;
        }
    }

    private static boolean awaitBlockedWriter(java.sql.Connection monitor, int blockerPid) throws Exception {
        String sql = """
                SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE query LIKE 'WITH writable_scope%' AND ? = ANY(pg_blocking_pids(pid))
                )
                """;
        try (var statement = monitor.prepareStatement(sql)) {
            statement.setInt(1, blockerPid);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            do {
                try (var row = statement.executeQuery()) {
                    row.next();
                    if (row.getBoolean(1)) {
                        return true;
                    }
                }
                Thread.sleep(20);
            } while (System.nanoTime() < deadline);
            return false;
        }
    }

    private static void createExecutionAuthority(String jdbcUrl) {
        Flyway.configure().dataSource(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration").load().migrate();
    }

    private static void seedAnchor(String queryId, ExecutionOwnerIdentity owner)
            throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement(
                        "INSERT INTO public.execution_anchor "
                                + "(execution_id, owner_id, owner_incarnation, fencing_token, "
                                + "acquired_at, expires_at) "
                                + "VALUES (?, ?, ?, ?, clock_timestamp(), "
                                + "clock_timestamp() + INTERVAL '1 hour')")) {
            statement.setString(1, queryId);
            statement.setString(2, owner.getOwnerId());
            statement.setString(3, owner.getOwnerIncarnation());
            statement.setLong(4, owner.getFencingToken());
            statement.executeUpdate();
        }
    }

    private static void replaceAnchor(String queryId, ExecutionOwnerIdentity owner)
            throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement(
                        "UPDATE public.execution_anchor SET owner_id = ?, "
                                + "owner_incarnation = ?, fencing_token = ?, "
                                + "acquired_at = clock_timestamp(), "
                                + "expires_at = clock_timestamp() + INTERVAL '1 hour', "
                                + "version = version + 1 WHERE execution_id = ?")) {
            statement.setString(1, owner.getOwnerId());
            statement.setString(2, owner.getOwnerIncarnation());
            statement.setLong(3, owner.getFencingToken());
            statement.setString(4, queryId);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static void insertBatches(String shuffleId, int count) throws Exception {
        String sql = "INSERT INTO shuffle.shuffle_batch (shuffle_id, producer_partition_id, "
                + "producer_attempt_id, producer_fencing_token, output_partition_id, "
                + "batch_sequence, object_key, batch_size_bytes, row_count, batch_checksum) "
                + "VALUES (?, 0, 0, 1, ?, ?, ?, 1, 1, ?)";
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < count; index++) {
                int partition = index % 4;
                statement.setString(1, shuffleId);
                statement.setInt(2, partition);
                statement.setLong(3, index / 4);
                statement.setString(4, shuffleId + "/batch-" + index);
                statement.setBytes(5, new byte[32]);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static void acceptAndCommit(
            Harness harness,
            org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse registration,
            TaskAttemptId attempt,
            byte[] payload) {
        accept(harness, registration, attempt, payload);
        harness.engine.commit(commitRequest(harness, registration, attempt, payload, 7));
    }

    private static void accept(
            Harness harness,
            org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse registration,
            TaskAttemptId attempt,
            byte[] payload) {
        byte[] checksum = ShuffleChecksums.sha256(payload);
        acceptSingle(harness.engine, new CompleteWriteBatch(registration.getShuffleId(), 0, 0,
                7, attempt, taskGrant(harness, registration, attempt,
                        org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation
                                .SHUFFLE_ACCESS_OPERATION_WRITE), ByteString.copyFrom(payload), checksum));
    }

    private static void acceptSingle(ShuffleServiceEngine engine, CompleteWriteBatch batch) {
        try (var writer = engine.writeSession(() -> false)) {
            writer.accept(batch);
            writer.finish();
        }
    }

    private static CommitProducerAttemptRequest commitRequest(
            Harness harness,
            org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse registration,
            TaskAttemptId attempt,
            byte[] payload,
            long rows) {
        byte[] manifest = manifest(payload, rows);
        var summary = PartitionCommitSummary.newBuilder().setPartitionId(0)
                .setExpectedLastSequence(0).setTotalBytes(payload.length)
                .setTotalBatches(1).setTotalRows(rows)
                .setOutputChecksum(ShuffleChecksums.protocol(manifest)).build();
        return CommitProducerAttemptRequest.newBuilder()
                .setShuffleId(registration.getShuffleId()).setProducerAttempt(attempt)
                .addPartitions(summary).setAccessCapability(taskGrant(harness,
                        registration, attempt, org.pangrova.protocol.v2.shuffle
                                .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE))
                .build();
    }

    private static org.pangrova.protocol.v2.ShuffleAccessCapability taskGrant(
            Harness harness,
            org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse registration,
            TaskAttemptId attempt,
            org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation operation) {
        return harness.engine.renewAccess(org.pangrova.protocol.v2.shuffle
                .RenewAccessRequest.newBuilder()
                .setOperation(operation).setShuffleId(registration.getShuffleId())
                .setExecutionOwner(owner()).setTaskAttempt(attempt).build(), true)
                .getAccessCapability();
    }

    private static byte[] manifest(byte[] payload, long rows) {
        return ShufflePartitionManifestChecksum.accumulator()
                .add(0, payload.length, rows, ShuffleChecksums.sha256(payload))
                .finishNonEmpty();
    }

    private static RegisterShuffleRequest registration() {
        return RegisterShuffleRequest.newBuilder().setQueryId("query-1").setStageId(1)
                .setNumPartitions(1).setShuffleId("restart-fence-shuffle")
                .setProtocol(CanonicalProtocolIdentity.current()).setPlanEpoch(2)
                .setExecutionOwner(owner()).setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(producerChecksum()))
                .build();
    }

    private static TaskAttemptId attempt(int attemptId, long fence) {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setStageId(1)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(attemptId)
                .setFencingToken(fence).setTaskId("producer-" + attemptId)
                .setControlEpoch(fence).setExecutionOwner(owner()).setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                        .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
    }

    private static TaskAttemptId consumerAttempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setStageId(2)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(0).setFencingToken(30)
                .setTaskId("consumer").setControlEpoch(30).setExecutionOwner(owner()).setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                        .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
    }

    private static ExecutionOwnerIdentity owner() {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId("coordinator")
                .setOwnerIncarnation("boot-1").setFencingToken(1).build();
    }

    private static byte[] producerChecksum() {
        return ShuffleProducerSetChecksum.forContiguousPartitions("query-1", 1, 2, 1);
    }

    private static Harness openHarness() {
        try {
            ShuffleServiceConfig config = config();
            PostgresShuffleStore store = PostgresShuffleStore.create(config.database());
            S3ShuffleObjectStorage objects = S3ShuffleObjectStorage.testing(
                    s3Client(), BUCKET, "integration");
            ShuffleServiceEngine engine = new ShuffleServiceEngine(config, store,
                    store.batchStore(), objects,
                    ShuffleCapabilityAuthority.testing(SECRET), Clock.systemUTC());
            return new Harness(engine);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static ShuffleServiceConfig config() {
        return new ShuffleServiceConfig(
                new ShuffleServiceConfig.Server(7443, "shuffle", "shuffle",
                        List.of(new ShuffleServiceConfig.Endpoint(
                                "shuffle", 7443, "shuffle", 0, -1))),
                new ShuffleServiceConfig.Database(POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(), passwordFile, 4),
                new ShuffleServiceConfig.ObjectStorage(BUCKET, "integration", "us-east-1",
                        minioEndpoint(), true, false),
                new ShuffleServiceConfig.Security(Path.of("cert"), Path.of("key"),
                        Path.of("ca"), Path.of("secret"), List.of("coordinator"),
                        Duration.ofHours(1)),
                new ShuffleServiceConfig.Limits(1024, 1024 * 1024,
                        3 * 1024 * 1024, 4, 4, 16));
    }

    private static S3Client s3Client() {
        return S3Client.builder().endpointOverride(minioEndpoint())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build()).build();
    }

    private static URI minioEndpoint() {
        return URI.create("http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000));
    }

    private static final class WriteRpc implements AutoCloseable {
        private final CanonicalShuffleGrpcServer transport;
        private final Server server;
        private final ManagedChannel channel;
        private final CompletableFuture<WriteBatchResponse> terminal = new CompletableFuture<>();
        private ClientCallStreamObserver<WriteBatchRequest> requests;
        private long nextSequence;

        private WriteRpc(ShuffleServiceEngine engine) throws Exception {
            transport = new CanonicalShuffleGrpcServer(engine, config().limits());
            server = ServerBuilder.forPort(0).addService(transport).build().start();
            channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            ShuffleServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS).writeBatch(
                    new ClientResponseObserver<WriteBatchRequest, WriteBatchResponse>() {
                        private WriteBatchResponse acknowledgement;

                        @Override
                        public void beforeStart(ClientCallStreamObserver<WriteBatchRequest> stream) {
                            requests = stream;
                        }

                        @Override
                        public void onNext(WriteBatchResponse response) {
                            if (acknowledgement != null) {
                                terminal.completeExceptionally(new AssertionError("duplicate write acknowledgement"));
                            }
                            acknowledgement = response;
                        }

                        @Override
                        public void onError(Throwable failure) {
                            terminal.completeExceptionally(failure);
                        }

                        @Override
                        public void onCompleted() {
                            if (acknowledgement == null) {
                                terminal.completeExceptionally(new AssertionError("missing write acknowledgement"));
                            } else {
                                terminal.complete(acknowledgement);
                            }
                        }
                    });
        }

        static WriteRpc open(ShuffleServiceEngine engine) throws Exception {
            return new WriteRpc(engine);
        }

        void send(CompleteWriteBatch batch) {
            var checksum = ShuffleChecksums.protocol(batch.checksum());
            requests.onNext(WriteBatchRequest.newBuilder().setShuffleId(batch.shuffleId())
                    .setProducerAttempt(batch.attempt()).setPartitionId(batch.outputPartitionId())
                    .setStreamSequence(nextSequence++).setRowCount(batch.rowCount())
                    .setCodec(CompressionCodec.COMPRESSION_CODEC_NONE).setUncompressedSize(batch.canonicalSize())
                    .setAccessCapability(batch.capability())
                    .setChunk(BatchChunk.newBuilder().setBatchSequence(batch.batchSequence()).setChunkCount(1)
                            .setBatchSizeBytes(batch.canonicalSize()).setData(batch.canonicalBytes())
                            .setBatchChecksum(checksum).setChunkChecksum(checksum)).build());
        }

        WriteBatchResponse finish() throws Exception {
            requests.onCompleted();
            return terminal.get(10, TimeUnit.SECONDS);
        }

        void cancel() {
            requests.cancel("test consumer cancelled the producer RPC", null);
        }

        @Override
        public void close() throws InterruptedException {
            try {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            } finally {
                transport.close();
            }
        }
    }

    private record Harness(ShuffleServiceEngine engine) {}
}
