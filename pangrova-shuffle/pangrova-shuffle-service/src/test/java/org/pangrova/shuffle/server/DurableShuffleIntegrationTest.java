package org.pangrova.shuffle.server;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.shuffle.ShufflePartitionManifestChecksum;
import org.pangrova.shuffle.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.deployment.ShuffleServiceConfig;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.shuffle.transport.CompleteWriteBatch;
import org.pangrova.testing.ContainerTestBase;
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
            DockerImageName.parse("postgres:16-alpine"));
    private static final GenericContainer<?> MINIO = new GenericContainer<>(
            DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"))
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server", "/data")
            .withExposedPorts(9000);
    private static Path passwordFile;

    @BeforeAll
    static void startBackends() throws Exception {
        POSTGRES.start();
        createExecutionAuthority();
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
    void survivesRestartFencesRetriesDetectsCorruptionAndCleansOrphans() {
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
        var read = restarted.engine.read(ReadPartitionRequest.newBuilder()
                .setShuffleId(shuffleId).setPartitionId(0).setStartSequence(0)
                .setConsumerAttempt(consumerAttempt()).setDeadlineEpochMs(
                        System.currentTimeMillis() + 60_000)
                .setAccessCapability(taskGrant(restarted, registration,
                        consumerAttempt(), org.pangrova.protocol.v2.shuffle
                                .ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ))
                .build());
        var page = restarted.engine.readPage(read, 0);
        assertThat(page).hasSize(1);
        assertThat(restarted.engine.readPayload(read, page.get(0))).isEqualTo(winnerPayload);

        try (S3Client client = s3Client()) {
            String key = "integration/" + shuffleId
                    + "/producer-0/attempt-2/output-0/batch-0.bin";
            client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
                    RequestBody.fromBytes("corrupt".getBytes(
                            java.nio.charset.StandardCharsets.UTF_8)));
        }
        assertThatThrownBy(() -> restarted.engine.readPayload(read, page.get(0)))
                .isInstanceOf(org.pangrova.shuffle.server.error.ShuffleServiceFailure.class)
                .hasMessageContaining("checksum");

        var cleanup = restarted.engine.cleanup(CleanupRequest.newBuilder()
                .setQueryId("query-1").setExecutionOwner(owner())
                .setAccessCapability(registration.getCleanupCapability()).build());
        assertThat(cleanup.getBytesFreed()).isPositive();
        var repeatedCleanup = restarted.engine.cleanup(CleanupRequest.newBuilder()
                .setQueryId("query-1").setExecutionOwner(owner())
                .setAccessCapability(registration.getCleanupCapability()).build());
        assertThat(repeatedCleanup.getBytesFreed()).isZero();
        restarted.engine.close();
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
                    .setExecutionOwner(owner()).build();
            StoredBatch batch = new StoredBatch(new BatchIdentity(shuffleId, 0, 0, 0, 0),
                    producer, shuffleId + "/late.bin", 4, 1, new byte[32]);
            String reservation = store.batchStore().beginWriteIntent(batch, 300_000);
            assertThatThrownBy(() -> store.beginCleanup(
                    new CleanupCommand(queryId, owner()),
                    new CleanupLease("first-cleaner", 100, 200)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("writes are still in progress");
            store.batchStore().abortWriteIntent(reservation);
            var plan = store.beginCleanup(new CleanupCommand(queryId, owner()),
                    new CleanupLease("second-cleaner", 100, 200)).orElseThrow();
            store.completeCleanup(plan);
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
        seedAnchor(queryId, owner());
        Harness first = openHarness();
        var registration = first.engine.register(RegisterShuffleRequest.newBuilder()
                .setQueryId(queryId).setShuffleId(shuffleId).setStageId(1)
                .setNumPartitions(1).setProtocol(CanonicalTaskRequests.protocolVersion())
                .setPlanEpoch(2).setExecutionOwner(owner())
                .setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(
                        ShuffleProducerSetChecksum.forContiguousPartitions(queryId, 1, 2, 1)))
                .build());
        TaskAttemptId producer = TaskAttemptId.newBuilder()
                .setQueryOrJobId(queryId).setStageId(1).setPlanEpoch(2)
                .setPartitionId(0).setAttemptId(0).setFencingToken(1)
                .setTaskId("uncertain-producer").setControlEpoch(1)
                .setExecutionOwner(owner()).build();
        StoredBatch batch = new StoredBatch(new BatchIdentity(shuffleId, 0, 0, 0, 0),
                producer, objectKey, 4, 1, new byte[32]);
        try (PostgresShuffleStore store = PostgresShuffleStore.create(config().database())) {
            store.batchStore().beginWriteIntent(batch, 300_000);
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
            assertThat(expire.executeUpdate()).isEqualTo(1);
        }
        Harness restarted = openHarness();
        restarted.engine.cleanup(CleanupRequest.newBuilder()
                .setQueryId(queryId).setExecutionOwner(owner())
                .setAccessCapability(registration.getCleanupCapability()).build());
        assertThat(restarted.engine.sweepUncertainWritePrefixes(1)).isEqualTo(1);
        try (S3Client client = s3Client()) {
            client.putObject(PutObjectRequest.builder().bucket(BUCKET)
                            .key("integration/" + objectKey).build(),
                    RequestBody.fromBytes("late".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            assertThat(client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET)
                    .prefix("integration/" + objectKey).build()).contents()).hasSize(1);
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
                    .prefix("integration/" + objectKey).build()).contents()).isEmpty();
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
        try (var connection = DriverManager.getConnection(
                jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.execution_anchor ("
                    + "execution_id VARCHAR(128) PRIMARY KEY)");
        }
        Flyway.configure().dataSource(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas("shuffle").defaultSchema("shuffle")
                .locations("classpath:db/migration")
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

    private static void createExecutionAuthority() throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.execution_anchor ("
                    + "execution_id VARCHAR(128) PRIMARY KEY, "
                    + "owner_id VARCHAR(128) NOT NULL, "
                    + "owner_incarnation VARCHAR(64) NOT NULL, "
                    + "fencing_token BIGINT NOT NULL CHECK (fencing_token > 0), "
                    + "acquired_at TIMESTAMPTZ(6) NOT NULL, "
                    + "expires_at TIMESTAMPTZ(6) NOT NULL, "
                    + "version BIGINT NOT NULL DEFAULT 1)");
        }
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
        harness.engine.acceptBatch(new CompleteWriteBatch(registration.getShuffleId(), 0, 0,
                7, attempt, taskGrant(harness, registration, attempt,
                        org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation
                                .SHUFFLE_ACCESS_OPERATION_WRITE), payload, checksum));
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
                .setProtocol(CanonicalTaskRequests.protocolVersion()).setPlanEpoch(2)
                .setExecutionOwner(owner()).setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(producerChecksum()))
                .build();
    }

    private static TaskAttemptId attempt(int attemptId, long fence) {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setStageId(1)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(attemptId)
                .setFencingToken(fence).setTaskId("producer-" + attemptId)
                .setControlEpoch(fence).setExecutionOwner(owner()).build();
    }

    private static TaskAttemptId consumerAttempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setStageId(2)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(0).setFencingToken(30)
                .setTaskId("consumer").setControlEpoch(30).setExecutionOwner(owner()).build();
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

    private record Harness(ShuffleServiceEngine engine) {}
}
