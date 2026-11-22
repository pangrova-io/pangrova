package org.pangrova.testing.integration.e2e.shuffle;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

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
import org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleCoordinator;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.server.ShuffleServerRuntime;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool;

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
    private final java.util.Map<String, ShuffleCommitReceipt> receipts = new java.util.HashMap<>();
    private org.pangrova.common.lifecycle.ProcessIncarnationStore workerIncarnation;

    record ReadBinding(org.pangrova.protocol.v2.ExternalShuffleInput input,
                       org.pangrova.protocol.v2.TaskAttemptId consumerAttempt) {
        org.pangrova.shuffle.api.ShuffleAccess readAccess() {
            var grant = input.getReadCapability();
            return new org.pangrova.shuffle.api.ShuffleAccess(grant.getToken().toByteArray(),
                    grant.getExpiresAtEpochMs());
        }

        org.pangrova.shuffle.api.ShuffleServerEndpoint selectedEndpoint() {
            int partition = input.getOutputPartitionId();
            var endpoint = input.getEndpointsList().stream().filter(candidate ->
                    candidate.getPartitionStart() <= partition && partition < candidate.getPartitionEnd())
                    .findFirst().orElseThrow();
            return new org.pangrova.shuffle.api.ShuffleServerEndpoint(endpoint.getHost(), endpoint.getPort(),
                    endpoint.getPartitionStart(), endpoint.getPartitionEnd(), endpoint.getTlsIdentity());
        }
    }

    static org.pangrova.shuffle.pangrova.client.PangrovaPartitionRead partitionRead(ReadBinding read) {
        var input = read.input();
        var part = input.getPublication();
        return new org.pangrova.shuffle.pangrova.client.PangrovaPartitionRead(
                input.getShuffleId(), new org.pangrova.shuffle.api.identity.ExchangeChannelContext(
                        org.pangrova.shuffle.nativewire.NativeExchangeAttempts.decode(read.consumerAttempt()),
                        input.getOutputPartitionId(), input.getDeadlineEpochMs()),
                input.getPlanEpoch(), input.getExpectedProducerPartitions(),
                input.getExpectedProducerSetChecksum().getDigest().toByteArray(), new ShuffleCommitReceipt.Partition(
                        part.getLastSequence(), part.getTotalBytes(), part.getTotalRows(),
                        java.util.HexFormat.of().formatHex(part.getManifestSha256().toByteArray())));
    }

    ActualShuffleService(Path directory, int partitions, int serviceCount) throws Exception {
        if (partitions <= 0 || serviceCount <= 0 || serviceCount > partitions) {
            throw new IllegalArgumentException("Actual endpoints must exactly cover all partitions");
        }
        this.partitions = partitions;
        backend = new ActualShuffleBackend(directory);
        try {
            workerIncarnation = org.pangrova.common.lifecycle.ProcessIncarnationStore.open(
                    directory.resolve("external-worker-incarnation"));
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
            var workerCredentials = TlsChannelCredentials.newBuilder()
                    .keyManager(backend.workerCertificate().toFile(), backend.workerPrivateKey().toFile())
                    .trustManager(backend.certificate().toFile()).build();
            var workerChannel = Grpc.newChannelBuilderForAddress(
                    "localhost", endpoint.port(), workerCredentials).build();
            channels.add(workerChannel);
            relays.add(new ShuffleWireRelay(backend, workerChannel));
        }
    }

    ShuffleServiceGrpc.ShuffleServiceBlockingStub control() {
        return ShuffleServiceGrpc.newBlockingStub(channels.get(0)).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.Session workerShuffle() {
        var factory = java.util.ServiceLoader.load(org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.class)
                .stream().map(java.util.ServiceLoader.Provider::get).filter(f -> f.providerId().equals("pangrova"))
                .findFirst().orElseThrow();
        return factory.open(new org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.Configuration(
                new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(100, 8),
                new org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.Tls(
                        backend.workerCertificate(), backend.workerPrivateKey(), backend.certificate()), java.util.Map.of()));
    }

    RemoteShuffleClientPool clientPool() {
        return clientPool(new org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission(
                new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(100, 8)));
    }

    RemoteShuffleClientPool clientPool(org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission admission) {
        return new RemoteShuffleClientPool(5_000, 5_000,
                RemoteShuffleClientPool.PoolOptions.mutualTls(new RemoteShuffleClientPool.PemTlsConfig(
                        backend.workerCertificate(), backend.workerPrivateKey(),
                        backend.certificate())), admission);
    }

    RemoteShuffleClientPool shortIdleClientPool() {
        return shortIdleClientPool(new org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission(
                new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(100, 8)));
    }

    RemoteShuffleClientPool shortIdleClientPool(org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission admission) {
        return new RemoteShuffleClientPool(5_000, 5_000,
                new RemoteShuffleClientPool.PoolOptions(100, TimeUnit.MILLISECONDS, 100, true, null,
                        new RemoteShuffleClientPool.PemTlsConfig(
                                backend.workerCertificate(), backend.workerPrivateKey(),
                                backend.certificate())),
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
        var identity = workerIncarnation.identity();
        return registerProducer(request -> request.toBuilder().setAttempt(request.getAttempt().toBuilder()
                .setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId(identity.ownershipDomainId())
                        .setIncarnationId(identity.incarnationId()))).build());
    }

    AdmittedProducer registerProducer(java.util.function.UnaryOperator<TaskRequest> bindWorker) {
        var base = bindWorker.apply(ActualNativeShuffleRequests.pendingProducer(partitions));
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
                .setProtocol(CanonicalProtocolIdentity.current()).setQueryId(query).setStageId(3)
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
        if (workerIncarnation != null) {
            try { workerIncarnation.closeAfterDrain(() -> {}); }
            catch (Exception cleanup) { failure = accumulate(failure, cleanup); }
        }
        if (failure != null) throw failure;
    }

    private static Exception accumulate(Exception failure, Exception cleanup) {
        if (failure == null) return cleanup;
        failure.addSuppressed(cleanup);
        return failure;
    }

    record AdmittedProducer(TaskRequest request, RegisterShuffleResponse registration,
                           org.pangrova.protocol.v2.Checksum producerSetChecksum) {}

    ShuffleCommitReceipt finalizeProducer(AdmittedProducer producer) {
        var attempt = producer.request().getAttempt();
        var tls = new RemoteShuffleClientPool.PemTlsConfig(backend.certificate(), backend.privateKey(), backend.certificate());
        var controlEndpoints = List.of(new ShuffleServiceAddress("localhost", endpoints.get(0).port(), "localhost"));
        try (var lifecycle = new PangrovaShuffleCoordinator(controlEndpoints, queryId -> {
            var current = backend.anchors().current(queryId).orElseThrow();
            return new ShuffleExecutionOwner(current.ownerId(), current.ownerIncarnation(), current.fencingToken());
        }, tls)) {
            var handle = lifecycle.registerShuffle(new ShuffleRegistration(attempt.getQueryOrJobId(),
                    attempt.getStageId(), partitions, 0, false, attempt.getPlanEpoch(), 1,
                    producer.producerSetChecksum().getDigest().toByteArray()));
            if (!handle.shuffleId().equals(producer.registration().getShuffleId())) {
                throw new IllegalStateException("Coordinator registration changed the producer allocation");
            }
            var receipt = lifecycle.commitShuffle(new ShufflePublication(attempt.getQueryOrJobId(), attempt.getStageId(),
                    attempt.getPlanEpoch(), handle.shuffleId()));
            receipts.put(handle.shuffleId(), receipt);
            return receipt;
        }
    }

    ReadBinding delegateRead(AdmittedProducer producer, TaskRequest consumer) {
        var attempt = consumer.getAttempt();
        var owner = attempt.getExecutionOwner();
        var capability = control().renewAccess(RenewAccessRequest.newBuilder()
                .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ)
                .setShuffleId(producer.registration().getShuffleId()).setExecutionOwner(owner)
                .setTaskAttempt(attempt).build()).getAccessCapability();
        var binding = producer.request().getExecutionOptions().getShuffle().getExecutionBinding();
        var part = receipts.get(binding.getShuffleId()).partitions().get(0);
        var input = org.pangrova.protocol.v2.ExternalShuffleInput.newBuilder().setShuffleId(binding.getShuffleId())
                .addAllEndpoints(binding.getEndpointsList()).setCommitAddress(binding.getCommitAddress())
                .setOutputPartitionId(0).setPartitionCount(partitions).setPlanEpoch(attempt.getPlanEpoch())
                .setExecutionOwner(owner).setDeadlineEpochMs(consumer.getDeadlineEpochMs())
                .setReadCapability(capability).setExpectedProducerPartitions(1)
                .setExpectedProducerSetChecksum(producer.producerSetChecksum())
                .setPublication(org.pangrova.protocol.v2.ShufflePartitionPublication.newBuilder()
                        .setLastSequence(part.lastSequence()).setTotalBytes(part.bytes()).setTotalRows(part.rows())
                        .setManifestSha256(com.google.protobuf.ByteString.copyFrom(
                                java.util.HexFormat.of().parseHex(part.manifestSha256()))))
                .build();
        return new ReadBinding(input, attempt);
    }
}
