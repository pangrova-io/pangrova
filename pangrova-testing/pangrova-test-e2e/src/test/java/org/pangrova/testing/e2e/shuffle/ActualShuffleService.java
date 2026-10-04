package org.pangrova.testing.e2e.shuffle;

import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.deployment.ShuffleServiceConfig;
import org.pangrova.shuffle.server.ShuffleServerRuntime;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientPool;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Full production mTLS listener and durable state machine, never synthesized RPC responses. */
final class ActualShuffleService implements AutoCloseable {
    private final ActualShuffleBackend backend;
    private final List<ShuffleServerRuntime> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final List<ShuffleWireRelay> relays = new ArrayList<>();
    private final List<ShuffleServiceConfig.Endpoint> endpoints;
    private final int partitions;

    ActualShuffleService(Path directory, int partitions, int serviceCount) throws Exception {
        if (partitions <= 0 || serviceCount <= 0 || serviceCount > partitions) {
            throw new IllegalArgumentException("Actual endpoints must exactly cover all partitions");
        }
        this.partitions = partitions;
        backend = new ActualShuffleBackend(directory);
        try {
            endpoints = endpointRanges(partitions, serviceCount);
            startListeners();
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static List<ShuffleServiceConfig.Endpoint> endpointRanges(int partitions, int count)
            throws Exception {
        var endpoints = new ArrayList<ShuffleServiceConfig.Endpoint>();
        for (int index = 0; index < count; index++) {
            try (var socket = new ServerSocket(0)) {
                endpoints.add(new ShuffleServiceConfig.Endpoint("localhost", socket.getLocalPort(),
                        "localhost", index * partitions / count, (index + 1) * partitions / count));
            }
        }
        return List.copyOf(endpoints);
    }

    private void startListeners() throws Exception {
        for (var endpoint : endpoints) {
            var config = new ShuffleServiceConfig(
                    new ShuffleServiceConfig.Server(endpoint.port(), "localhost", "localhost", endpoints),
                    backend.databaseConfig(), backend.objectConfig(), backend.securityConfig(),
                    new ShuffleServiceConfig.Limits(1024 * 1024, 16 * 1024 * 1024,
                            48 * 1024 * 1024, 4, 4, 16));
            servers.add(ShuffleServerRuntime.start(config));
            var credentials = TlsChannelCredentials.newBuilder()
                    .keyManager(backend.certificate().toFile(), backend.privateKey().toFile())
                    .trustManager(backend.certificate().toFile()).build();
            channels.add(Grpc.newChannelBuilderForAddress("localhost", endpoint.port(), credentials).build());
            relays.add(new ShuffleWireRelay(backend, channels.get(channels.size() - 1)));
        }
    }

    ShuffleServiceGrpc.ShuffleServiceBlockingStub control() {
        return ShuffleServiceGrpc.newBlockingStub(channels.get(0)).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    RemoteShuffleClientPool clientPool() {
        return clientPool(new org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission(
                new org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission.Limits(100, 8)));
    }

    RemoteShuffleClientPool clientPool(org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission admission) {
        return new RemoteShuffleClientPool(5_000, 5_000,
                RemoteShuffleClientPool.PoolOptions.mutualTls(new RemoteShuffleClientPool.PemTlsConfig(
                        backend.certificate(), backend.privateKey(), backend.certificate())), admission);
    }

    RemoteShuffleClientPool shortIdleClientPool() {
        return shortIdleClientPool(new org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission(
                new org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission.Limits(100, 8)));
    }

    RemoteShuffleClientPool shortIdleClientPool(org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission admission) {
        return new RemoteShuffleClientPool(5_000, 5_000,
                new RemoteShuffleClientPool.PoolOptions(100, TimeUnit.MILLISECONDS, 100, true, null,
                        new RemoteShuffleClientPool.PemTlsConfig(
                                backend.certificate(), backend.privateKey(), backend.certificate())),
                admission);
    }

    ActualShuffleBackend backend() { return backend; }
    List<ShuffleServiceConfig.Endpoint> endpoints() { return endpoints; }
    ShuffleWireRelay wire(int serviceIndex) { return relays.get(serviceIndex); }

    /** A real short-TTL control deployment delegates; the long-TTL data deployment performs renewal. */
    org.pangrova.protocol.v2.ShuffleAccessCapability shortWriteGrant(AdmittedProducer producer) throws Exception {
        return shortGrant(producer, producer.request(),
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE, Duration.ofSeconds(10));
    }

    org.pangrova.protocol.v2.ShuffleAccessCapability shortReadGrant(
            AdmittedProducer producer, TaskRequest consumer) throws Exception {
        return shortGrant(producer, consumer,
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ, Duration.ofSeconds(45));
    }

    private org.pangrova.protocol.v2.ShuffleAccessCapability shortGrant(
            AdmittedProducer producer, TaskRequest request, ShuffleAccessOperation operation,
            Duration ttl) throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var security = backend.securityConfig();
        var shortPolicy = new ShuffleServiceConfig.Security(security.certificateChain(), security.privateKey(),
                security.rootCa(), security.capabilitySecret(), security.coordinatorTlsIdentities(),
                ttl);
        servers.add(ShuffleServerRuntime.start(new ShuffleServiceConfig(
                new ShuffleServiceConfig.Server(port, "localhost", "localhost", endpoints),
                backend.databaseConfig(), backend.objectConfig(), shortPolicy,
                new ShuffleServiceConfig.Limits(1024 * 1024, 16 * 1024 * 1024,
                        48 * 1024 * 1024, 4, 4, 16))));
        var credentials = TlsChannelCredentials.newBuilder()
                .keyManager(backend.certificate().toFile(), backend.privateKey().toFile())
                .trustManager(backend.certificate().toFile()).build();
        var channel = Grpc.newChannelBuilderForAddress("localhost", port, credentials).build();
        channels.add(channel);
        return ShuffleServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(operation)
                        .setShuffleId(producer.registration().getShuffleId())
                        .setExecutionOwner(request.getAttempt().getExecutionOwner())
                        .setTaskAttempt(request.getAttempt()).build()).getAccessCapability();
    }

    /** Acquires an actual DB-clock owner, registers over mTLS, and delegates an exact task capability. */
    AdmittedProducer registerProducer() {
        var base = ActualNativeShuffleRequests.pendingProducer(partitions);
        String query = "actual-shuffle-" + UUID.randomUUID();
        var lease = backend.anchors().tryAcquire(query, "acceptance-coordinator",
                UUID.randomUUID().toString(), Duration.ofMinutes(5)).orElseThrow();
        var owner = ExecutionOwnerIdentity.newBuilder().setOwnerId(lease.ownerId())
                .setOwnerIncarnation(lease.ownerIncarnation()).setFencingToken(lease.fencingToken()).build();
        var attempt = base.getAttempt().toBuilder().setQueryOrJobId(query).setStageId(3)
                .setExecutionOwner(owner).build();
        var producerSet = org.pangrova.protocol.v2.Checksum.newBuilder()
                .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256).setDigest(
                com.google.protobuf.ByteString.copyFrom(ShuffleProducerSetChecksum.forContiguousPartitions(
                        query, attempt.getStageId(), attempt.getPlanEpoch(), 1))).build();
        var registration = control().registerShuffle(RegisterShuffleRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion()).setQueryId(query).setStageId(3)
                .setShuffleId("shuffle-" + UUID.randomUUID()).setNumPartitions(partitions)
                .setPlanEpoch(attempt.getPlanEpoch()).setExecutionOwner(owner)
                .setExpectedProducerPartitions(1).setExpectedProducerSetChecksum(producerSet).build());
        var access = control().renewAccess(RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE)
                .setShuffleId(registration.getShuffleId()).setExecutionOwner(owner).setTaskAttempt(attempt).build())
                .getAccessCapability();
        var binding = ShuffleExecutionBinding.newBuilder().setShuffleId(registration.getShuffleId())
                .setPlanEpoch(attempt.getPlanEpoch()).setExecutionOwner(owner).setWriteCapability(access)
                .setCommitAddress(registration.getCommitAddress().toBuilder().setPort(relays.get(0).port()));
        for (int index = 0; index < registration.getEndpointsCount(); index++) {
            binding.addEndpoints(registration.getEndpoints(index).toBuilder().setPort(relays.get(index).port()));
        }
        var request = base.toBuilder().setAttempt(attempt).setExecutionOptions(
                base.getExecutionOptions().toBuilder().setShuffle(base.getExecutionOptions().getShuffle().toBuilder()
                        .setPersistenceMode(ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE)
                        .setExecutionBinding(binding))).build();
        CanonicalTaskRequests.validateForAdmission(request);
        return new AdmittedProducer(request, registration, producerSet);
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (var relay : relays) {
            try { relay.close(); } catch (Exception cleanup) { failure = accumulate(failure, cleanup); }
        }
        for (var channel : channels) {
            channel.shutdownNow();
            try {
                if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Actual shuffle RPC channel did not terminate");
                }
            } catch (Exception cleanup) { failure = accumulate(failure, cleanup); }
        }
        for (var server : servers) {
            try { server.close(); } catch (Exception cleanup) { failure = accumulate(failure, cleanup); }
        }
        try { backend.close(); } catch (Exception cleanup) { failure = accumulate(failure, cleanup); }
        if (failure != null) throw failure;
    }

    private static Exception accumulate(Exception failure, Exception cleanup) {
        if (failure == null) return cleanup;
        failure.addSuppressed(cleanup);
        return failure;
    }

    record AdmittedProducer(TaskRequest request, RegisterShuffleResponse registration,
                           org.pangrova.protocol.v2.Checksum producerSetChecksum) {}

    void finalizeProducer(AdmittedProducer producer) {
        control().finalizeShuffle(FinalizeShuffleRequest.newBuilder()
                .setShuffleId(producer.registration().getShuffleId())
                .setPlanEpoch(producer.request().getAttempt().getPlanEpoch())
                .setExecutionOwner(producer.request().getAttempt().getExecutionOwner())
                .setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(producer.producerSetChecksum())
                .setAccessCapability(producer.registration().getFinalizeCapability()).build());
    }

    org.pangrova.shuffle.exchange.ExternalShuffleTransport delegateRead(
            AdmittedProducer producer, TaskRequest consumer) {
        var attempt = consumer.getAttempt();
        var owner = attempt.getExecutionOwner();
        var exchangeOwner = new org.pangrova.shuffle.ShuffleExecutionOwner(
                owner.getOwnerId(), owner.getOwnerIncarnation(), owner.getFencingToken());
        var exchangeAttempt = new org.pangrova.shuffle.exchange.ExchangeTaskAttempt(
                attempt.getQueryOrJobId(), attempt.getStageId(), attempt.getPartitionId(),
                attempt.getTaskId(), attempt.getControlEpoch(),
                new org.pangrova.shuffle.exchange.ExchangeAttemptFence(attempt.getPlanEpoch(),
                        attempt.getAttemptId(), attempt.getFencingToken()), exchangeOwner);
        var capability = control().renewAccess(RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)
                .setShuffleId(producer.registration().getShuffleId()).setExecutionOwner(owner)
                .setTaskAttempt(attempt).build()).getAccessCapability();
        var binding = producer.request().getExecutionOptions().getShuffle().getExecutionBinding();
        var endpoints = binding.getEndpointsList().stream().map(endpoint ->
                new org.pangrova.shuffle.ShuffleServerEndpoint(endpoint.getHost(), endpoint.getPort(),
                        endpoint.getPartitionStart(), endpoint.getPartitionEnd(), endpoint.getTlsIdentity())).toList();
        var commit = binding.getCommitAddress();
        return new org.pangrova.shuffle.exchange.ExternalShuffleTransport(binding.getShuffleId(),
                endpoints, new org.pangrova.shuffle.ShuffleServiceAddress(commit.getHost(), commit.getPort(),
                        commit.getTlsIdentity()), 0, partitions, attempt.getPlanEpoch(), exchangeOwner,
                exchangeAttempt, consumer.getDeadlineEpochMs(),
                new org.pangrova.shuffle.ShuffleAccess(capability.getToken().toByteArray(),
                        capability.getExpiresAtEpochMs()), 1, producer.producerSetChecksum().getDigest().toByteArray());
    }
}
