package org.pangrova.server.coordinator.bootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleTaskGrantBinder;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleRetryPolicyValidator;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.protocol.v2.Checksum;
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
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool.PemTlsConfig;
import org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleCoordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaShuffleCoordinatorTest {

    @Test
    void logicalProducerSetDoesNotPermitSpeculativeWinnerSelection() {
        try (var manager = manager(1)) {
            var capabilities = manager.capabilities();
            assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateSpeculation(true, capabilities))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("winning producer attempts");
            ShuffleRetryPolicyValidator.validateSpeculation(false, capabilities);
        }
    }

    @Test
    void grantsExactBoundTaskForBothProducerAndConsumerWithoutStageBearer() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        try (RunningService running = RunningService.start(service);
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            var owner = org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                    .setOwnerId("owner").setOwnerIncarnation("incarnation")
                    .setFencingToken(9).build();
            TaskAttemptId attempt = TaskAttemptId.newBuilder()
                    .setQueryOrJobId("query-task").setStageId(2).setPartitionId(1)
                    .setPlanEpoch(7).setAttemptId(3).setFencingToken(11)
                    .setTaskId("task-exact").setControlEpoch(12)
                    .setExecutionOwner(owner).setWorkerExecutionIdentity(
                            org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                                    .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                                    .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
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

            TaskRequest granted = new CoordinatorShuffleTaskGrantBinder(manager).bind(
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
            assertThatThrownBy(() -> new CoordinatorShuffleTaskGrantBinder(manager).bind(
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            manager.registerShuffle(registration("query-finalize-refresh", 1, 2, 2, 1L));
            manager.commitShuffle(new ShufflePublication("query-finalize-refresh", 1, 1, "query-finalize-refresh-1-1"));

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
                PangrovaShuffleCoordinator first = manager(running.port())) {
            first.registerShuffle(registration("query-recovered-cleanup", 1, 2, 2, 1L));
        }
        try (RunningService running = RunningService.start(service);
                PangrovaShuffleCoordinator recovered = manager(running.port())) {
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            ShuffleRegistration first = registration("query-1", 1, 3, 2, 1L);
            ShuffleRegistration second = registration("query-1", 2, 5, 4, 1L);

            manager.registerShuffle(first);
            manager.registerShuffle(second);
            var firstResult = manager.commitShuffle(new ShufflePublication("query-1", 1, 1, "query-1-1-1"));
            var secondResult = manager.commitShuffle(new ShufflePublication("query-1", 2, 1, "query-1-2-1"));
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
            assertThat(firstResult.partitions()).hasSize(3);
            assertThat(secondResult.partitions()).hasSize(5);
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void acknowledgesPublicationOnlyAfterTheJournalRecordsValidatedProviderEvidence() throws Exception {
        var service = new RecordingShuffleService();
        try (var running = RunningService.start(service); var manager = manager(running.port())) {
            var request = registration("durable-publication", 4, 2, 3, 1L);
            var owner = new ShuffleExecutionOwner("owner", "incarnation", 9L);
            var observed = new AtomicInteger();
            manager.bindDurableEvidence(request.queryId(), new ShuffleLifecycleEvidence() {
                @Override
                public Optional<Allocation> restoredAllocation(ShuffleRegistration registration) {
                    return Optional.empty();
                }

                @Override
                public void registered(ShuffleRegistration registration, Allocation handle, ShuffleExecutionOwner issued) {
                    assertThat(registration).isSameAs(request);
                    assertThat(issued).isEqualTo(owner);
                }

                @Override
                public void finalized(ShuffleRegistration registration, Allocation handle, ShuffleExecutionOwner issued,
                        ShuffleCommitReceipt receipt, byte[] response) {
                    assertThat(registration).isSameAs(request);
                    assertThat(issued).isEqualTo(owner);
                    assertThat(receipt.publication().allocationId()).isEqualTo(handle.shuffleId());
                    assertThat(receipt.partitions()).hasSize(request.numPartitions());
                    assertThat(response).isEqualTo(service.finalizeResponse.toByteArray());
                    if (observed.incrementAndGet() == 1) {
                        throw new IllegalStateException("journal write failed");
                    }
                }
            });
            var handle = manager.registerShuffle(request);
            var publication = new ShufflePublication(request.queryId(), request.stageId(), request.planEpoch(), handle.shuffleId());
            service.omitLastPartition = true;
            assertThatThrownBy(() -> manager.commitShuffle(publication)).hasMessageContaining("cover every partition");
            assertThat(observed.get()).isZero();
            service.omitLastPartition = false;
            assertThatThrownBy(() -> manager.commitShuffle(publication)).hasMessage("journal write failed");
            var result = manager.commitShuffle(publication);
            assertThat(manager.commitShuffle(publication)).isSameAs(result);
            assertThat(service.finalizations).hasSize(3);
            assertThat(observed.get()).isEqualTo(2);
        }
    }

    @Test
    void restoredDescriptorNeedsCurrentServiceAuthorizationAndTheSameRoutes() throws Exception {
        var service = new RecordingShuffleService();
        var request = registration("durable-recovery", 1, 2, 3, 1L);
        var saved = new java.util.concurrent.atomic.AtomicReference<ShuffleLifecycleEvidence.Allocation>();
        var journal = new ShuffleLifecycleEvidence() {
            @Override
            public Optional<Allocation> restoredAllocation(ShuffleRegistration registration) {
                return Optional.ofNullable(saved.get());
            }

            @Override
            public void registered(ShuffleRegistration registration, Allocation allocation, ShuffleExecutionOwner owner) {
                var original = saved.compareAndExchange(null, allocation);
                if (original != null) assertThat(allocation).isEqualTo(original);
            }

            @Override
            public void finalized(ShuffleRegistration registration, Allocation allocation, ShuffleExecutionOwner owner,
                    ShuffleCommitReceipt receipt, byte[] response) {
                throw new AssertionError("Recovery does not publish");
            }
        };
        try (var running = RunningService.start(service); var first = manager(running.port());
                var next = manager(running.port())) {
            first.bindDurableEvidence(request.queryId(), journal);
            var issued = first.registerShuffle(request);
            next.bindDurableEvidence(request.queryId(), journal);
            service.denyRecovery = true;
            assertThatThrownBy(() -> next.registerShuffle(request)).isInstanceOf(io.grpc.StatusRuntimeException.class)
                    .hasMessageContaining("PERMISSION_DENIED");
            service.denyRecovery = false;
            var original = saved.get();
            var changed = next.describe(issued.shuffleId(), issued.commitAddress(), List.of(
                    new org.pangrova.shuffle.api.ShuffleServerEndpoint("different", running.port(), 0, 2, "")),
                    request.numPartitions());
            saved.set(new ShuffleLifecycleEvidence.Allocation(issued.shuffleId(), changed));
            assertThatThrownBy(() -> next.registerShuffle(request)).hasMessageContaining("changed the restored");
            saved.set(original);
            assertThat(next.registerShuffle(request).shuffleId()).isEqualTo(issued.shuffleId());
            assertThat(service.renewals).hasSize(3);
            assertThat(service.registrations).hasSize(3);
            assertThat(service.registrations.get(2).getShuffleId()).isEqualTo(issued.shuffleId());
        }
    }

    @Test
    void cachedAllocationAndReceiptAreFencedWhenProcessAuthorityChanges() throws Exception {
        var service = new RecordingShuffleService();
        var owner = new java.util.concurrent.atomic.AtomicReference<>(new ShuffleExecutionOwner("first", "first", 1));
        try (var running = RunningService.start(service);
                var manager = new PangrovaShuffleCoordinator(List.of(
                        new org.pangrova.shuffle.api.ShuffleServiceAddress("127.0.0.1", running.port(), "")),
                        query -> owner.get(), null)) {
            var request = registration("cached-fence", 1, 1, 1, 1);
            var handle = manager.registerShuffle(request);
            var publication = new ShufflePublication(request.queryId(), request.stageId(), 1, handle.shuffleId());
            manager.commitShuffle(publication);
            owner.set(new ShuffleExecutionOwner("successor", "successor", 2));
            assertThatThrownBy(() -> manager.registerShuffle(request)).hasMessageContaining("owner changed");
            assertThatThrownBy(() -> manager.commitShuffle(publication)).hasMessageContaining("owner changed");
            assertThat(service.registrations).hasSize(1);
            assertThat(service.finalizations).hasSize(1);
        }
    }

    @Test
    void usesRenewedCleanupCapabilityFromLaterStage() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.renewCleanupCapability = true;
        try (RunningService running = RunningService.start(service);
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            manager.registerShuffle(registration("query-renew", 1, 2, 2, 1L));
            manager.registerShuffle(registration("query-renew", 2, 2, 2, 1L));
            manager.cleanupShuffle("query-renew");

            assertThat(service.cleanups).singleElement().satisfies(request ->
                    assertThat(request.getAccessCapability())
                            .isEqualTo(service.latestCleanupCapability));
        }
    }

    @Test
    void cleanupReleasesOnlyItsQueryStageAllocationsAndReceipts() throws Exception {
        var service = new RecordingShuffleService();
        try (var running = RunningService.start(service); var manager = manager(running.port())) {
            var first = registration("first-query", 1, 2, 2, 1);
            var second = registration("second-query", 1, 2, 2, 1);
            var firstHandle = manager.registerShuffle(first);
            var secondHandle = manager.registerShuffle(second);
            var firstPublication = new ShufflePublication(first.queryId(), first.stageId(), 1, firstHandle.shuffleId());
            var secondPublication = new ShufflePublication(second.queryId(), second.stageId(), 1, secondHandle.shuffleId());
            manager.commitShuffle(firstPublication);
            var secondReceipt = manager.commitShuffle(secondPublication);

            manager.cleanupShuffle(first.queryId());
            assertThatThrownBy(() -> manager.commitShuffle(firstPublication)).hasMessageContaining("unregistered");
            assertThat(manager.registerShuffle(second)).isSameAs(secondHandle);
            assertThat(manager.commitShuffle(secondPublication)).isSameAs(secondReceipt);
            assertThat(service.registrations).hasSize(2);
            assertThat(service.finalizations).hasSize(2);
            assertThat(service.cleanups).extracting(CleanupRequest::getQueryId).containsExactly(first.queryId());
        }
    }

    @Test
    void rejectsChangedCleanupCapabilityWithoutLaterExpiry() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService();
        service.rotateCleanupTokenWithoutRenewal = true;
        try (RunningService running = RunningService.start(service);
                PangrovaShuffleCoordinator manager = manager(running.port())) {
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            ShuffleRegistration initial = registration("query-retry", 3, 2, 3, 1L);
            ShuffleRegistration retry = registration("query-retry", 3, 2, 3, 2L);

            manager.registerShuffle(initial);
            manager.registerShuffle(retry);
            var previous = new ShufflePublication("query-retry", 3, 1, "query-retry-3-1");
            assertThatThrownBy(() -> manager.commitShuffle(previous)).hasMessageContaining("stale");
            manager.commitShuffle(new ShufflePublication("query-retry", 3, 2, "query-retry-3-2"));

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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
            manager.registerShuffle(registration("query-incomplete", 4, 4, 2, 1L));

            assertThatThrownBy(() -> manager.commitShuffle(new ShufflePublication("query-incomplete", 4, 1, "query-incomplete-4-1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cover every partition");
        }
    }

    @Test
    void executesLifecycleOverMutualTls() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService("localhost");
        PemTlsConfig tls = tlsConfig();
        try (RunningService running = RunningService.startTls(service, tls);
                PangrovaShuffleCoordinator manager =
                        manager(running.port(), "localhost", tls)) {
            manager.registerShuffle(registration("query-tls", 5, 2, 2, 1L));
            manager.commitShuffle(new ShufflePublication("query-tls", 5, 1, "query-tls-5-1"));
            manager.cleanupShuffle("query-tls");

            assertThat(service.registrations).hasSize(1);
            assertThat(service.finalizations).hasSize(1);
            assertThat(service.cleanups).hasSize(1);
        }
    }

    @Test
    void rejectsTlsIdentityMismatch() throws Exception {
        RecordingShuffleService service = new RecordingShuffleService("localhost");
        PemTlsConfig tls = tlsConfig();
        try (RunningService running = RunningService.startTls(service, tls);
                PangrovaShuffleCoordinator manager =
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
        PemTlsConfig tls = tlsConfig();
        try (RunningService running = RunningService.startTls(service, tls);
                PangrovaShuffleCoordinator manager =
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
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
        PemTlsConfig tls = tlsConfig();
        assertThatThrownBy(() -> new PangrovaShuffleCoordinator(
                List.of(
                        new org.pangrova.shuffle.api.ShuffleServiceAddress(
                                "127.0.0.1", 9443, "localhost"),
                        new org.pangrova.shuffle.api.ShuffleServiceAddress(
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
                PangrovaShuffleCoordinator manager = manager(running.port())) {
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

    private static PangrovaShuffleCoordinator manager(int port) {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(org.pangrova.config.PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE", "coordinator.shuffle.provider", "pangrova",
                "coordinator.shuffle.provider.pangrova.endpoints", "127.0.0.1:" + port)));
        return (PangrovaShuffleCoordinator) CoordinatorShuffleProviders.open(config,
                queryId -> new ShuffleExecutionOwner("owner", "incarnation", 9L));
    }

    private static PangrovaShuffleCoordinator manager(
            int port, String tlsIdentity, PemTlsConfig tlsConfig) {
        return new PangrovaShuffleCoordinator(
                List.of(new org.pangrova.shuffle.api.ShuffleServiceAddress(
                        "127.0.0.1", port, tlsIdentity)),
                queryId -> new ShuffleExecutionOwner("owner", "incarnation", 9L),
                tlsConfig);
    }

    private static PemTlsConfig tlsConfig()
            throws URISyntaxException {
        Path certificate = Path.of(PangrovaShuffleCoordinatorTest.class
                .getResource("/tls/localhost-cert.pem").toURI());
        Path privateKey = Path.of(PangrovaShuffleCoordinatorTest.class
                .getResource("/tls/localhost-key.pem").toURI());
        return new PemTlsConfig(
                certificate, privateKey, certificate);
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
            Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                    .addService(service).build().start();
            service.port = server.getPort();
            return new RunningService(server);
        }

        static RunningService startTls(
                RecordingShuffleService service,
                PemTlsConfig tlsConfig) throws IOException {
            var sslContext = GrpcSslContexts.forServer(
                            tlsConfig.certificateChain().toFile(),
                            tlsConfig.privateKey().toFile())
                    .trustManager(tlsConfig.rootCa().toFile())
                    .clientAuth(ClientAuth.REQUIRE)
                    .build();
            Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
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
        private volatile FinalizeShuffleResponse finalizeResponse;
        private volatile boolean failRegistration;
        private volatile boolean denyRecovery;
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
            finalizeResponse = response.build();
            observer.onNext(finalizeResponse);
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
            if (denyRecovery && request.getOperation() == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE) {
                observer.onError(Status.PERMISSION_DENIED.withDescription("Current owner rejected").asRuntimeException());
                return;
            }
            if (request.getOperation() != ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP
                    && !request.getShuffleId().isEmpty() && !request.getQueryId().isEmpty()) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription("Conflicting renewal scopes").asRuntimeException());
                return;
            }
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
