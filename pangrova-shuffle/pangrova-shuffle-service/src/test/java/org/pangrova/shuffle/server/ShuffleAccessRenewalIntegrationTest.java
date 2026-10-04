package org.pangrova.shuffle.server;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.deployment.ShuffleServiceConfig;
import org.pangrova.shuffle.security.CapabilityOperation;
import org.pangrova.shuffle.security.CapabilityScope;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.testing.ContainerTestBase;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL authority coverage for canonical v2 capability renewal. */
class ShuffleAccessRenewalIntegrationTest extends ContainerTestBase {
    private static final byte[] SECRET = new byte[32];
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"));
    private static Path passwordFile;

    private String queryId;
    private String shuffleId;
    private PostgresShuffleStore store;
    private ShuffleServiceEngine engine;
    private ShuffleCapabilityAuthority authority;

    @BeforeAll
    static void startPostgres() throws Exception {
        POSTGRES.start();
        passwordFile = Files.createTempFile("shuffle-renewal-postgres", ".password");
        Files.writeString(passwordFile, POSTGRES.getPassword());
        try (var connection = connection(); var statement = connection.createStatement()) {
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

    @AfterAll
    static void stopPostgres() throws Exception {
        POSTGRES.stop();
        Files.deleteIfExists(passwordFile);
    }

    @BeforeEach
    void setUp() throws Exception {
        String identity = UUID.randomUUID().toString();
        queryId = "renew-" + identity;
        shuffleId = "shuffle-" + identity;
        replaceAnchor(owner());
        store = PostgresShuffleStore.create(config().database());
        authority = ShuffleCapabilityAuthority.testing(SECRET);
        engine = new ShuffleServiceEngine(config(), store, store.batchStore(),
                S3ShuffleObjectStorage.testing(s3Client(), "unused", "renewal"),
                authority, Clock.systemUTC());
        engine.register(registration());
    }

    @AfterEach
    void tearDown() {
        engine.close();
    }

    @Test
    void renewsBeforeExpiryOnlyForMatchingAttemptAndLiveOwner() throws Exception {
        RenewAccessRequest grant = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE)
                .setShuffleId(shuffleId).setExecutionOwner(owner())
                .setTaskAttempt(attempt()).build();
        assertThatThrownBy(() -> engine.renewAccess(grant, false))
                .isInstanceOf(SecurityException.class);
        var granted = engine.renewAccess(grant, true).getAccessCapability();
        authority.verify(granted, CapabilityScope.forTask(
                CapabilityOperation.WRITE, shuffleId, owner(), attempt()),
                System.currentTimeMillis());
        var previous = authority.issue(CapabilityScope.forTask(
                CapabilityOperation.WRITE, shuffleId, owner(), attempt()),
                System.currentTimeMillis() + 10_000L);
        RenewAccessRequest request = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE)
                .setShuffleId(shuffleId).setExecutionOwner(owner())
                .setTaskAttempt(attempt()).setPreviousCapability(previous).build();

        var renewed = engine.renewAccess(request, false).getAccessCapability();
        assertThat(renewed.getExpiresAtEpochMs())
                .isGreaterThan(previous.getExpiresAtEpochMs());
        authority.verify(renewed, CapabilityScope.forTask(
                CapabilityOperation.WRITE, shuffleId, owner(), attempt()),
                System.currentTimeMillis());
        assertThat(store.findAllocation(shuffleId).orElseThrow().capabilityExpiresAtMs())
                .isGreaterThanOrEqualTo(renewed.getExpiresAtEpochMs());

        var stagePrevious = authority.issue(CapabilityScope.forShuffle(
                CapabilityOperation.WRITE, shuffleId, owner()),
                System.currentTimeMillis() + 10_000L);
        RenewAccessRequest coordinatorReissue = request.toBuilder()
                .clearTaskAttempt().setPreviousCapability(stagePrevious).build();
        assertThatThrownBy(() -> engine.renewAccess(coordinatorReissue, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(engine.renewAccess(coordinatorReissue, true)
                .getAccessCapability().getExpiresAtEpochMs())
                .isGreaterThan(previous.getExpiresAtEpochMs());
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setPreviousCapability(stagePrevious).build(), false))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(grant.toBuilder()
                .setPreviousCapability(stagePrevious).build(), true))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)
                .build(), false)).isInstanceOf(SecurityException.class)
                .hasMessageContaining("scope");
        String otherShuffle = shuffleId + "-other";
        engine.register(registration().toBuilder().setStageId(2)
                .setShuffleId(otherShuffle)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(
                        ShuffleProducerSetChecksum.forContiguousPartitions(queryId, 2, 2, 1)))
                .build());
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setShuffleId(otherShuffle)
                .setTaskAttempt(attempt().toBuilder().setStageId(2))
                .build(), false)).isInstanceOf(SecurityException.class)
                .hasMessageContaining("scope");

        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setTaskAttempt(attempt().toBuilder().setTaskId("other-task"))
                .build(), false)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setTaskAttempt(attempt().toBuilder().setControlEpoch(99))
                .build(), false)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setTaskAttempt(attempt().toBuilder().setQueryOrJobId("other"))
                .build(), false)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setExecutionOwner(successor()).build(), false))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> engine.renewAccess(request.toBuilder()
                .setPreviousCapability(previous.toBuilder()
                        .setToken(com.google.protobuf.ByteString.copyFromUtf8("forged")))
                .build(), false)).isInstanceOf(SecurityException.class);

        replaceAnchor(successor());
        assertThatThrownBy(() -> engine.renewAccess(request, false))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("live durable anchor");
    }

    @Test
    void rejectsMalformedShuffleIdsBeforeAccessLookup() {
        RenewAccessRequest request = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE)
                .setShuffleId("../other-shuffle").setExecutionOwner(owner())
                .setTaskAttempt(attempt()).build();
        assertThatThrownBy(() -> engine.renewAccess(request, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shuffle id is invalid");
    }

    @Test
    void rejectsExpiredPreviousCapabilityAndWorkerControlRenewal() throws Exception {
        var previous = authority.issue(CapabilityScope.forTask(
                CapabilityOperation.READ, shuffleId, owner(), attempt()),
                System.currentTimeMillis() + 1_000L);
        RenewAccessRequest read = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)
                .setShuffleId(shuffleId).setExecutionOwner(owner())
                .setTaskAttempt(attempt()).setPreviousCapability(previous).build();
        PostgresShuffleStore futureStore = PostgresShuffleStore.create(config().database());
        try (ShuffleServiceEngine future = new ShuffleServiceEngine(config(), futureStore,
                futureStore.batchStore(), S3ShuffleObjectStorage.testing(s3Client(),
                        "unused", "renewal"), authority,
                Clock.offset(Clock.systemUTC(), Duration.ofSeconds(2)))) {
            assertThatThrownBy(() -> future.renewAccess(read, false))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("expired");
        }
        assertThatThrownBy(() -> engine.renewAccess(RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE)
                .setShuffleId(shuffleId).setExecutionOwner(owner()).build(), false))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Coordinator mTLS");
    }

    @Test
    void grantsConsumerFromAnotherStageAndEpochButNotAnotherTask() {
        TaskAttemptId consumer = attempt().toBuilder().setStageId(9)
                .setPlanEpoch(12).setTaskId("consumer-task").build();
        RenewAccessRequest grant = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)
                .setShuffleId(shuffleId).setExecutionOwner(owner())
                .setTaskAttempt(consumer).build();
        var capability = engine.renewAccess(grant, true).getAccessCapability();
        authority.verify(capability, CapabilityScope.forTask(
                CapabilityOperation.READ, shuffleId, owner(), consumer),
                System.currentTimeMillis());
        assertThatThrownBy(() -> engine.renewAccess(grant.toBuilder()
                .setPreviousCapability(capability)
                .setTaskAttempt(consumer.toBuilder().setTaskId("other"))
                .build(), false)).isInstanceOf(SecurityException.class);
    }

    @Test
    void coordinatorRecoversControlCapabilitiesAfterFencedTakeover() throws Exception {
        replaceAnchor(successor());
        RenewAccessRequest finalize = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE)
                .setShuffleId(shuffleId).setExecutionOwner(successor()).build();
        assertThatThrownBy(() -> engine.renewAccess(finalize, false))
                .isInstanceOf(SecurityException.class);
        var renewed = engine.renewAccess(finalize, true).getAccessCapability();
        authority.verify(renewed, CapabilityScope.forShuffle(
                CapabilityOperation.FINALIZE, shuffleId, successor()),
                System.currentTimeMillis());
        assertThat(store.findAllocation(shuffleId).orElseThrow().owner())
                .isEqualTo(successor());

        RenewAccessRequest cleanup = RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP)
                .setQueryId(queryId).setExecutionOwner(successor()).build();
        var cleanupCapability = engine.renewAccess(cleanup, true).getAccessCapability();
        authority.verify(cleanupCapability, CapabilityScope.forQuery(queryId, successor()),
                System.currentTimeMillis());
    }

    @Test
    void stopsAnAlreadyOpenedReadAtTheNextBatchBoundaryAfterCapabilityExpiry() {
        long now = System.currentTimeMillis();
        var allocation = store.findAllocation(shuffleId).orElseThrow();
        var read = new ShuffleServiceEngine.ReadResult(allocation,
                new ShuffleServiceModel.FinalizeResult(
                        1, new byte[32], List.of(), 0, 0),
                0, 0, now + 60_000L, now - 1L);

        assertThatThrownBy(() -> engine.readPage(read, 0))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("batch boundary");
    }

    private RegisterShuffleRequest registration() {
        return RegisterShuffleRequest.newBuilder().setQueryId(queryId).setStageId(1)
                .setNumPartitions(1).setShuffleId(shuffleId)
                .setProtocol(CanonicalTaskRequests.protocolVersion()).setPlanEpoch(2)
                .setExecutionOwner(owner()).setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(ShuffleChecksums.protocol(
                        ShuffleProducerSetChecksum.forContiguousPartitions(queryId, 1, 2, 1)))
                .build();
    }

    private TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId(queryId).setStageId(1)
                .setPartitionId(0).setPlanEpoch(2).setAttemptId(1)
                .setFencingToken(1).setControlEpoch(1).setTaskId("producer")
                .setExecutionOwner(owner()).build();
    }

    private void replaceAnchor(ExecutionOwnerIdentity owner) throws Exception {
        try (var connection = connection(); var statement = connection.prepareStatement(
                "INSERT INTO public.execution_anchor (execution_id, owner_id, "
                        + "owner_incarnation, fencing_token, acquired_at, expires_at) "
                        + "VALUES (?, ?, ?, ?, clock_timestamp(), "
                        + "clock_timestamp() + INTERVAL '1 hour') "
                        + "ON CONFLICT (execution_id) DO UPDATE SET owner_id = EXCLUDED.owner_id, "
                        + "owner_incarnation = EXCLUDED.owner_incarnation, "
                        + "fencing_token = EXCLUDED.fencing_token, "
                        + "acquired_at = EXCLUDED.acquired_at, "
                        + "expires_at = EXCLUDED.expires_at")) {
            statement.setString(1, queryId);
            statement.setString(2, owner.getOwnerId());
            statement.setString(3, owner.getOwnerIncarnation());
            statement.setLong(4, owner.getFencingToken());
            statement.executeUpdate();
        }
    }

    private static java.sql.Connection connection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static ShuffleServiceConfig config() {
        return new ShuffleServiceConfig(
                new ShuffleServiceConfig.Server(7443, "shuffle", "shuffle",
                        List.of(new ShuffleServiceConfig.Endpoint(
                                "shuffle", 7443, "shuffle", 0, -1))),
                new ShuffleServiceConfig.Database(POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(), passwordFile, 4),
                new ShuffleServiceConfig.ObjectStorage("unused", "renewal", "us-east-1",
                        URI.create("http://127.0.0.1:1"), true, false),
                new ShuffleServiceConfig.Security(Path.of("cert"), Path.of("key"),
                        Path.of("ca"), Path.of("secret"), List.of("coordinator"),
                        Duration.ofMinutes(1)),
                new ShuffleServiceConfig.Limits(1024, 1024 * 1024,
                        3 * 1024 * 1024, 4, 4, 16));
    }

    private static S3Client s3Client() {
        return S3Client.builder().endpointOverride(URI.create("http://127.0.0.1:1"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("unused", "unused"))).build();
    }

    private static ExecutionOwnerIdentity owner() {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId("coordinator")
                .setOwnerIncarnation("boot-1").setFencingToken(1).build();
    }

    private static ExecutionOwnerIdentity successor() {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId("coordinator-2")
                .setOwnerIncarnation("boot-2").setFencingToken(2).build();
    }
}
