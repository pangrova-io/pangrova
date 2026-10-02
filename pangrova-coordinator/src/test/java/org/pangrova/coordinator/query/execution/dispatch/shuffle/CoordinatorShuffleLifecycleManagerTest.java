package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShuffleServiceAddress;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CleanupResponse;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.PartitionFinalizationSummary;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorShuffleLifecycleManagerTest {

    @Test
    void grantsExactBoundTaskForBothProducerAndConsumerWithoutStageBearer() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            var owner = org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                    .setOwnerId("owner").setOwnerIncarnation("incarnation")
                    .setFencingToken(9).build();
            TaskAttemptId attempt = TaskAttemptId.newBuilder()
                    .setQueryOrJobId("query-task").setStageId(2).setPartitionId(1)
                    .setPlanEpoch(7).setAttemptId(3).setFencingToken(11)
                    .setTaskId("task-exact").setControlEpoch(12)
                    .setExecutionOwner(owner).build();
            var address = ShuffleServiceAddress.newBuilder()
                    .setHost("127.0.0.1").setPort(running.port()).build();
            var stageTicket = ShuffleAccessCapability.newBuilder()
                    .setToken(ByteString.copyFromUtf8("stage-only"))
                    .setExpiresAtEpochMs(System.currentTimeMillis() - 1L)
                    .build();
            TaskRequest canonical = TaskRequest.newBuilder().setAttempt(attempt)
                    .setExecutionOptions(TaskExecutionOptions.newBuilder()
                            .setShuffle(ShuffleTaskOptions.newBuilder()
                                    .setExecutionBinding(ShuffleExecutionBinding.newBuilder()
                                            .setShuffleId("shuffle-output")
                                            .setPlanEpoch(7).setExecutionOwner(owner)
                                            .setCommitAddress(address)
                                            .setWriteCapability(stageTicket))))
                    .addExchangeInputs(ExchangeInputBinding.newBuilder()
                            .setExternal(ExternalShuffleInput.newBuilder()
                                    .setShuffleId("shuffle-input")
                                    .setPlanEpoch(5).setExecutionOwner(owner)
                                    .setCommitAddress(address)
                                    .setReadCapability(stageTicket)))
                    .build();

            TaskRequest granted = manager.bindTaskAccess(
                    SubmitTaskRequest.from(canonical)).taskRequest();

            assertThat(service.renewals).hasSize(2);
            assertThat(service.renewals).extracting(RenewAccessRequest::getOperation)
                    .containsExactly(
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE,
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ);
            assertThat(service.renewals).allSatisfy(renewal -> {
                assertThat(renewal.getTaskAttempt()).isEqualTo(attempt);
                assertThat(renewal.getPreviousCapability().getToken()).isEmpty();
            });
            assertThat(granted.getExecutionOptions().getShuffle()
                    .getExecutionBinding().getWriteCapability().getToken().toStringUtf8())
                    .isEqualTo("renewed");
            assertThat(granted.getExchangeInputs(0).getExternal()
                    .getReadCapability().getToken().toStringUtf8())
                    .isEqualTo("renewed");
            assertThatThrownBy(() -> manager.bindTaskAccess(
                    SubmitTaskRequest.from(canonical.toBuilder()
                            .setAttempt(attempt.toBuilder().clearControlEpoch()).build())))
                    .isInstanceOf(SecurityException.class);
            assertThat(service.renewals).hasSize(2);
        }
    }

    @Test
    void refreshesCachedStageBeforeDispatchingNearExpiryCapabilities() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.nearExpiryFirstRegistration = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            ShuffleRegistration registration = registration("query-refresh", 1, 2, 2, 1L);
            var first = manager.registerShuffle(registration);
            var refreshed = manager.registerShuffle(registration);

            assertThat(service.registrations).hasSize(1);
            assertThat(service.renewals).extracting(RenewAccessRequest::getOperation)
                    .containsExactly(
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ,
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
            assertThat(refreshed.readAccess().expiresAtEpochMs())
                    .isGreaterThan(first.readAccess().expiresAtEpochMs());
        }
    }

    @Test
    void renewsNearExpiryFinalizeCapabilityAtControlBoundary() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.nearExpiryFirstRegistration = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            manager.registerShuffle(registration("query-finalize-refresh", 1, 2, 2, 1L));
            manager.commitShuffle("query-finalize-refresh", 1);

            assertThat(service.renewals).singleElement().satisfies(request -> {
                assertThat(request.getOperation()).isEqualTo(
                        ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE);
                assertThat(request.getPreviousCapability().getToken()).isEmpty();
            });
            assertThat(service.finalizations).singleElement().satisfies(request ->
                    assertThat(request.getAccessCapability().getToken().toStringUtf8())
                            .isEqualTo("renewed"));
        }
    }

    @Test
    void recoversCleanupAfterCoordinatorStateIsLost() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager first = manager(running.port())) {
            first.registerShuffle(registration("query-recovered-cleanup", 1, 2, 2, 1L));
        }
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager recovered = manager(running.port())) {
            recovered.recoverCleanupShuffle("query-recovered-cleanup");

            assertThat(service.renewals).singleElement().satisfies(request -> {
                assertThat(request.getOperation()).isEqualTo(
                        ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP);
                assertThat(request.getPreviousCapability().getToken()).isEmpty();
            });
            assertThat(service.cleanups).singleElement().satisfies(request ->
                    assertThat(request.getAccessCapability().getToken().toStringUtf8())
                            .isEqualTo("renewed"));
        }
    }

    @Test
    void drivesMultiStageRegisterFinalizeAndIdempotentCleanup() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            ShuffleRegistration first = registration("query-1", 1, 3, 2, 1L);
            ShuffleRegistration second = registration("query-1", 2, 5, 4, 1L);

            manager.registerShuffle(first);
            manager.registerShuffle(second);
            var firstResult = manager.commitShuffle("query-1", 1);
            var secondResult = manager.commitShuffle("query-1", 2);
            manager.cleanupShuffle("query-1");
            manager.cleanupShuffle("query-1");

            assertThat(service.registrations).hasSize(2);
            assertThat(service.registrations.get(0).getNumPartitions()).isEqualTo(3);
            assertThat(service.registrations.get(1).getExpectedProducerPartitions())
                    .isEqualTo(4);
            assertThat(service.finalizations).hasSize(2);
            assertThat(service.finalizations.get(0).getExpectedProducerSetChecksum())
                    .isEqualTo(service.registrations.get(0)
                            .getExpectedProducerSetChecksum());
            assertThat(firstResult.partitionStats()).hasSize(3);
            assertThat(secondResult.partitionStats()).hasSize(5);
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void usesRenewedCleanupCapabilityFromLaterStage() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.renewCleanupCapability = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            manager.registerShuffle(registration("query-renew", 1, 2, 2, 1L));
            manager.registerShuffle(registration("query-renew", 2, 2, 2, 1L));
            manager.cleanupShuffle("query-renew");

            assertThat(service.cleanups).singleElement().satisfies(request ->
                    assertThat(request.getAccessCapability())
                            .isEqualTo(service.latestCleanupCapability));
        }
    }

    @Test
    void rejectsChangedCleanupCapabilityWithoutLaterExpiry() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.rotateCleanupTokenWithoutRenewal = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            manager.registerShuffle(registration("query-token-change", 1, 2, 2, 1L));

            assertThatThrownBy(() -> manager.registerShuffle(
                    registration("query-token-change", 2, 2, 2, 1L)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("without a later expiry");
        }
    }

    @Test
    void retryReplacesOnlyWithANewerEpochAndFinalizesLatestIdentity() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            ShuffleRegistration initial = registration("query-retry", 3, 2, 3, 1L);
            ShuffleRegistration retry = registration("query-retry", 3, 2, 3, 2L);

            manager.registerShuffle(initial);
            manager.registerShuffle(retry);
            manager.commitShuffle("query-retry", 3);

            assertThat(service.registrations).hasSize(2);
            assertThat(service.finalizations).singleElement()
                    .satisfies(request -> {
                        assertThat(request.getPlanEpoch()).isEqualTo(2L);
                        assertThat(request.getExpectedProducerSetChecksum())
                                .isEqualTo(service.registrations.get(1)
                                        .getExpectedProducerSetChecksum());
                    });
            assertThatThrownBy(() -> manager.registerShuffle(initial))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("stale");
        }
    }

    @Test
    void rejectsFinalizationWithoutExactPartitionCoverage() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.omitLastPartition = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            manager.registerShuffle(registration("query-incomplete", 4, 4, 2, 1L));

            assertThatThrownBy(() -> manager.commitShuffle("query-incomplete", 4))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cover every partition");
        }
    }

    @Test
    void executesLifecycleOverMutualTls() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService("localhost");
        ShuffleTlsConfig tls = tlsConfig("localhost");
        try (RunningService running = RunningService.startTls(service, tls);
                CoordinatorShuffleLifecycleManager manager =
                        manager(running.port(), "localhost", tls)) {
            manager.registerShuffle(registration("query-tls", 5, 2, 2, 1L));
            manager.commitShuffle("query-tls", 5);
            manager.cleanupShuffle("query-tls");

            assertThat(service.registrations).hasSize(1);
            assertThat(service.finalizations).hasSize(1);
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void rejectsTlsIdentityMismatch() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService("localhost");
        ShuffleTlsConfig tls = tlsConfig("localhost");
        try (RunningService running = RunningService.startTls(service, tls);
                CoordinatorShuffleLifecycleManager manager =
                        manager(running.port(), "wrong.internal", tls)) {
            assertThatThrownBy(() -> manager.registerShuffle(
                    registration("query-wrong-identity", 6, 2, 2, 1L)))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .hasMessageContaining("UNAVAILABLE");
        }
    }

    @Test
    void rejectsPlaintextDataEndpointsReturnedOverTlsControlPlane() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        ShuffleTlsConfig tls = tlsConfig("localhost");
        try (RunningService running = RunningService.startTls(service, tls);
                CoordinatorShuffleLifecycleManager manager =
                        manager(running.port(), "localhost", tls)) {
            assertThatThrownBy(() -> manager.registerShuffle(
                    registration("query-tls-downgrade", 9, 2, 2, 1L)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("outside Coordinator TLS policy");
        }
    }

    @Test
    void tlsEndpointWithoutClientTrustMaterialFailsClosed() {
        assertThatThrownBy(() -> manager(9443, "shuffle.internal", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trust and client material");
    }

    @Test
    void cleanupAfterRegistrationFailureRecoversUnknownServerState() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.failRegistration = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            assertThatThrownBy(() -> manager.registerShuffle(
                    registration("query-register-failure", 7, 2, 2, 1L)))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .hasMessageContaining("INTERNAL");

            manager.cleanupShuffle("query-register-failure");
            manager.cleanupShuffle("query-register-failure");

            assertThat(service.registrations).hasSize(1);
            assertThat(service.renewals).singleElement().satisfies(request ->
                    assertThat(request.getOperation()).isEqualTo(
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP));
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void cleansPersistedShuffleWhenRegisterResponseIsLost() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.loseRegistrationResponse = true;
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            assertThatThrownBy(() -> manager.registerShuffle(
                    registration("query-lost-response", 1, 2, 2, 1L)))
                    .isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .hasMessageContaining("UNAVAILABLE");
            manager.cleanupShuffle("query-lost-response");

            assertThat(service.registrationsById).hasSize(1);
            assertThat(service.renewals).singleElement().satisfies(request ->
                    assertThat(request.getOperation()).isEqualTo(
                            ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP));
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void rejectsMixedTlsAndPlaintextEndpointPolicy() throws Exception {
        ShuffleTlsConfig tls = tlsConfig("localhost");
        assertThatThrownBy(() -> new CoordinatorShuffleLifecycleManager(
                List.of(
                        new org.pangrova.shuffle.ShuffleServiceAddress(
                                "127.0.0.1", 9443, "localhost"),
                        new org.pangrova.shuffle.ShuffleServiceAddress(
                                "127.0.0.1", 9444, "")),
                queryId -> new ShuffleExecutionOwner("owner", "incarnation", 9L),
                tls))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot mix TLS and plaintext");
    }

    @Test
    void registersIndependentQueriesConcurrently() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.concurrentRegistrationArrivals = new CountDownLatch(2);
        service.registrationRelease = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try (RunningService running = RunningService.start(service);
                CoordinatorShuffleLifecycleManager manager = manager(running.port())) {
            var first = executor.submit(() -> manager.registerShuffle(
                    registration("query-concurrent-a", 8, 2, 2, 1L)));
            var second = executor.submit(() -> manager.registerShuffle(
                    registration("query-concurrent-b", 8, 2, 2, 1L)));

            assertThat(service.concurrentRegistrationArrivals.await(2, TimeUnit.SECONDS))
                    .as("independent queries must not share a global lifecycle RPC lock")
                    .isTrue();
            service.registrationRelease.countDown();

            assertThat(first.get(2, TimeUnit.SECONDS).shuffleId()).isNotBlank();
            assertThat(second.get(2, TimeUnit.SECONDS).shuffleId()).isNotBlank();
        } finally {
            service.registrationRelease.countDown();
            executor.shutdownNow();
        }
    }

    private static CoordinatorShuffleLifecycleManager manager(int port) {
        return manager(port, "", null);
    }

    private static CoordinatorShuffleLifecycleManager manager(
            int port, String tlsIdentity, ShuffleTlsConfig tlsConfig) {
        return new CoordinatorShuffleLifecycleManager(
                List.of(new org.pangrova.shuffle.ShuffleServiceAddress(
                        "127.0.0.1", port, tlsIdentity)),
                queryId -> new ShuffleExecutionOwner("owner", "incarnation", 9L),
                tlsConfig);
    }

    private static ShuffleTlsConfig tlsConfig(String identity)
            throws URISyntaxException {
        Path certificate = Path.of(CoordinatorShuffleLifecycleManagerTest.class
                .getResource("/tls/localhost-cert.pem").toURI());
        Path privateKey = Path.of(CoordinatorShuffleLifecycleManagerTest.class
                .getResource("/tls/localhost-key.pem").toURI());
        return new ShuffleTlsConfig(
                certificate, privateKey, certificate, identity);
    }

    private static ShuffleRegistration registration(
            String queryId,
            int stageId,
            int partitions,
            int producers,
            long planEpoch) {
        return new ShuffleRegistration(
                queryId, stageId, partitions, 128L, false, planEpoch, producers,
                ShuffleProducerSetChecksum.forContiguousPartitions(
                        queryId, stageId, planEpoch, producers));
    }

    private static final class RunningService implements AutoCloseable {
        private final Server server;

        private RunningService(Server server) {
            this.server = server;
        }

        static RunningService start(RecordingShuffleService service) throws IOException {
            Server server = ServerBuilder.forPort(0).addService(service).build().start();
            service.port = server.getPort();
            return new RunningService(server);
        }

        static RunningService startTls(
                RecordingShuffleService service,
                ShuffleTlsConfig tlsConfig) throws IOException {
            var sslContext = GrpcSslContexts.forServer(
                            tlsConfig.certificateChain().toFile(),
                            tlsConfig.privateKey().toFile())
                    .trustManager(tlsConfig.rootCa().toFile())
                    .clientAuth(ClientAuth.REQUIRE)
                    .build();
            Server server = NettyServerBuilder.forPort(0)
                    .sslContext(sslContext)
                    .addService(service)
                    .build()
                    .start();
            service.port = server.getPort();
            return new RunningService(server);
        }

        int port() {
            return server.getPort();
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    private static final class RecordingShuffleService
            extends ShuffleServiceGrpc.ShuffleServiceImplBase {
        private final List<RegisterShuffleRequest> registrations =
                new CopyOnWriteArrayList<>();
        private final List<FinalizeShuffleRequest> finalizations =
                new CopyOnWriteArrayList<>();
        private final List<CleanupRequest> cleanups = new CopyOnWriteArrayList<>();
        private final List<RenewAccessRequest> renewals = new CopyOnWriteArrayList<>();
        private final Map<String, RegisterShuffleRequest> registrationsById =
                new ConcurrentHashMap<>();
        private final long capabilityExpiry = System.currentTimeMillis() + 300_000L;
        private volatile boolean nearExpiryFirstRegistration;
        private volatile boolean omitLastPartition;
        private volatile boolean failRegistration;
        private volatile boolean loseRegistrationResponse;
        private volatile boolean renewCleanupCapability;
        private volatile boolean rotateCleanupTokenWithoutRenewal;
        private volatile ShuffleAccessCapability latestCleanupCapability;
        private volatile CountDownLatch concurrentRegistrationArrivals;
        private volatile CountDownLatch registrationRelease;
        private volatile int port;
        private final String tlsIdentity;

        private RecordingShuffleService() {
            this("");
        }

        private RecordingShuffleService(String tlsIdentity) {
            this.tlsIdentity = tlsIdentity;
        }

        @Override
        public void registerShuffle(
                RegisterShuffleRequest request,
                StreamObserver<RegisterShuffleResponse> observer) {
            registrations.add(request);
            if (failRegistration) {
                observer.onError(Status.INTERNAL
                        .withDescription("registration rejected")
                        .asRuntimeException());
                return;
            }
            if (concurrentRegistrationArrivals != null) {
                concurrentRegistrationArrivals.countDown();
                try {
                    if (!registrationRelease.await(5, TimeUnit.SECONDS)) {
                        observer.onError(Status.DEADLINE_EXCEEDED
                                .withDescription("concurrent registrations did not arrive")
                                .asRuntimeException());
                        return;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    observer.onError(Status.CANCELLED
                            .withDescription("registration interrupted")
                            .withCause(interrupted)
                            .asRuntimeException());
                    return;
                }
            }
            String shuffleId = request.getQueryId() + "-" + request.getStageId()
                    + "-" + request.getPlanEpoch();
            registrationsById.put(shuffleId, request);
            if (loseRegistrationResponse) {
                observer.onError(Status.UNAVAILABLE
                        .withDescription("registration response lost")
                        .asRuntimeException());
                return;
            }
            long issuedExpiry = nearExpiryFirstRegistration && registrations.size() == 1
                    ? System.currentTimeMillis() + 45_000L : capabilityExpiry;
            long cleanupExpiry = issuedExpiry
                    + (renewCleanupCapability ? registrations.size() * 1_000L : 0L);
            String cleanupToken = "cleanup-" + cleanupExpiry
                    + (rotateCleanupTokenWithoutRenewal ? "-" + registrations.size() : "");
            latestCleanupCapability = capability(cleanupToken, cleanupExpiry);
            RegisterShuffleResponse response = RegisterShuffleResponse.newBuilder()
                    .setShuffleId(shuffleId)
                    .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                            .setHost("127.0.0.1")
                            .setPort(port)
                            .setPartitionStart(0)
                            .setPartitionEnd(request.getNumPartitions())
                            .setTlsIdentity(tlsIdentity))
                    .setCommitAddress(ShuffleServiceAddress.newBuilder()
                            .setHost("127.0.0.1").setPort(port)
                            .setTlsIdentity(tlsIdentity))
                    .setReadCapability(capability("read", issuedExpiry))
                    .setWriteCapability(capability("write", issuedExpiry))
                    .setFinalizeCapability(capability("finalize", issuedExpiry))
                    .setCleanupCapability(latestCleanupCapability)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        }

        @Override
        public void finalizeShuffle(
                FinalizeShuffleRequest request,
                StreamObserver<FinalizeShuffleResponse> observer) {
            finalizations.add(request);
            RegisterShuffleRequest registration = registrationsById.get(
                    request.getShuffleId());
            int partitionCount = registration.getNumPartitions();
            int emitted = omitLastPartition ? partitionCount - 1 : partitionCount;
            FinalizeShuffleResponse.Builder response = FinalizeShuffleResponse.newBuilder()
                    .setCommittedProducerCount(request.getExpectedProducerPartitions())
                    .setProducerSetChecksum(request.getExpectedProducerSetChecksum())
                    .setTotalBytes(emitted * 10L)
                    .setTotalRows(emitted);
            for (int partitionId = 0; partitionId < emitted; partitionId++) {
                response.addPartitions(PartitionFinalizationSummary.newBuilder()
                        .setPartitionId(partitionId)
                        .setTotalBytes(10L)
                        .setTotalRows(1L)
                        .setCommittedLastSequence(0L)
                        .setOutputChecksum(checksum((byte) (partitionId + 1))));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void cleanup(
                CleanupRequest request,
                StreamObserver<CleanupResponse> observer) {
            cleanups.add(request);
            observer.onNext(CleanupResponse.newBuilder()
                    .setPartitionsCleaned(registrationsById.size())
                    .setBytesFreed(10L * registrationsById.size())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void renewAccess(
                RenewAccessRequest request,
                StreamObserver<RenewAccessResponse> observer) {
            renewals.add(request);
            observer.onNext(RenewAccessResponse.newBuilder()
                    .setAccessCapability(capability(
                            "renewed", System.currentTimeMillis() + 300_000L))
                    .build());
            observer.onCompleted();
        }

        private static ShuffleAccessCapability capability(String token, long expiry) {
            return ShuffleAccessCapability.newBuilder()
                    .setToken(ByteString.copyFromUtf8(token))
                    .setExpiresAtEpochMs(expiry)
                    .build();
        }

        private static Checksum checksum(byte value) {
            return Checksum.newBuilder()
                    .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(new byte[32]).substring(0, 31)
                            .concat(ByteString.copyFrom(new byte[] {value})))
                    .build();
        }
    }
}
