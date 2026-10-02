package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.artifact.CoordinatorShuffleEvidenceRegistry;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.artifact.ShuffleLifecycleEvidence;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.ShuffleExecutionOwnerResolver;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.requireSameRegistration;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.toHandle;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.toProtocol;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.toProtocolChecksum;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.toRegisterRequest;
import static org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleProtocolAdapter.validateFinalization;

/**
 * Coordinator-side {@link ShuffleLifecycleManager} that communicates with
 * remote shuffle servers via gRPC to manage shuffle registration, commit,
 * and cleanup on behalf of the DAG dispatch engine.
 *
 * <p>Unlike the worker-side {@code RemoteShuffleExchangeManager} which also
 * handles data channels (sink/source), this class only manages the lifecycle
 * RPCs the coordinator needs: register before task dispatch, and cleanup
 * after query completion or failure.</p>
 *
 * <p>Thread-safe: gRPC stubs are thread-safe; internal maps use
 * {@link ConcurrentHashMap}.</p>
 */
@ThreadSafe
public final class CoordinatorShuffleLifecycleManager
        implements ShuffleLifecycleManager, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorShuffleLifecycleManager.class);
    private static final long RPC_TIMEOUT_MS = 10_000;
    private static final long CAPABILITY_REFRESH_MARGIN_MS = 60_000;

    private final List<ShuffleServiceAddress> endpoints;
    private final ShuffleExecutionOwnerResolver ownerResolver;
    private final ShuffleTlsConfig tlsConfig;
    private final ChannelCredentials channelCredentials;
    private final CoordinatorShuffleTaskGrantBinder taskGrants;
    private final ConcurrentHashMap<String, ManagedChannel> channels =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShuffleExecutionOwner> queryOwners =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShuffleAccess> queryCleanupAccess =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<QueryStageKey, RegisteredShuffle> registeredShuffles =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<QueryStageKey, ShuffleCommitResult> finalizedShuffles =
            new ConcurrentHashMap<>();
    private final org.pangrova.coordinator.query.execution.dispatch.shuffle.lifecycle.ShuffleLifecycleOperations operations =
            new org.pangrova.coordinator.query.execution.dispatch.shuffle.lifecycle.ShuffleLifecycleOperations();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CoordinatorShuffleEvidenceRegistry evidence = new CoordinatorShuffleEvidenceRegistry();

    /** Installs durable control before any stage allocation or Worker dispatch for this exact execution. */
    public void bindDurableEvidence(String executionId, ShuffleLifecycleEvidence sink) {
        operations.execute(executionId, () -> {
            ensureOpen();
            evidence.bind(executionId, sink);
            return null;
        });
    }

    /** Drops only volatile failed-pass state; the durable allocation and external producer bytes remain intact. */
    public void detachDurableControl(String executionId, ShuffleExecutionOwner expectedOwner) {
        operations.execute(executionId, () -> {
            var owner = queryOwners.get(executionId);
            if (owner != null && !owner.equals(expectedOwner)) {
                throw new IllegalStateException("Cannot detach another shuffle execution owner's control state");
            }
            if (evidence.find(executionId) == null) {
                throw new IllegalStateException("Only a bound durable execution may retain its external allocation");
            }
            removeQueryState(executionId);
            return null;
        });
    }

    /** Creates the lifecycle with explicit remote endpoints, actual ownership and transport policy. */
    public CoordinatorShuffleLifecycleManager(
            List<ShuffleServiceAddress> endpoints,
            ShuffleExecutionOwnerResolver ownerResolver,
            ShuffleTlsConfig tlsConfig) {
        checkNotNull(endpoints, "endpoints must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException(
                    "shuffle endpoints must not be empty for REMOTE mode");
        }
        this.endpoints = List.copyOf(endpoints);
        this.ownerResolver = checkNotNull(
                ownerResolver, "ownerResolver must not be null");
        this.tlsConfig = tlsConfig;
        CoordinatorShuffleChannelSecurity.validateTransportSecurity(this.endpoints, tlsConfig);
        this.channelCredentials = CoordinatorShuffleChannelSecurity.channelCredentials(tlsConfig);
        this.taskGrants = new CoordinatorShuffleTaskGrantBinder(this::stubFor, RPC_TIMEOUT_MS);
        LOG.info("event=coordinator_shuffle_lifecycle_init endpointCount={}",
                endpoints.size());
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        checkNotNull(registration, "registration must not be null");
        return operations.execute(registration.queryId(), () -> {
            ensureOpen();
            return registerShuffleOwned(registration);
        });
    }

    private ShuffleHandle registerShuffleOwned(ShuffleRegistration registration) {
        QueryStageKey key = new QueryStageKey(
                registration.queryId(), registration.stageId());
        ShuffleHandle reused = reuseRegisteredShuffle(registration, key);
        if (reused != null) {
            return reused;
        }
        var observer = evidence.find(registration.queryId());
        var restored = observer == null ? java.util.Optional.<ShuffleLifecycleEvidence.Allocation>empty()
                : observer.restoredAllocation(registration);
        ShuffleServiceAddress endpoint = restored.map(ShuffleLifecycleEvidence.Allocation::service)
                .orElseGet(() -> selectEndpoint(registration.queryId()));
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);
        ShuffleExecutionOwner owner = requireStableOwner(registration.queryId());

        RegisterShuffleRequest request = toRegisterRequest(registration, owner).toBuilder()
                .setShuffleId(restored.map(ShuffleLifecycleEvidence.Allocation::shuffleId).orElse(""))
                .build();

        RegisterShuffleResponse response = stub
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .registerShuffle(request);

        ShuffleHandle handle = toHandle(response, registration);
        if (restored.isPresent() && !restored.orElseThrow().shuffleId().equals(handle.shuffleId())) {
            throw new IllegalStateException("External service changed the restored immutable shuffle allocation");
        }
        CoordinatorShuffleChannelSecurity.validateHandleTransportSecurity(handle, tlsConfig);
        if (observer != null) observer.registered(registration, handle, owner);
        rememberCleanupAccess(registration.queryId(), handle);
        registeredShuffles.put(key, new RegisteredShuffle(
                registration, handle, owner));
        finalizedShuffles.remove(key);
        LOG.info("event=shuffle_registered queryId={} stageId={} shuffleId={} "
                        + "endpointCount={}",
                registration.queryId(), registration.stageId(),
                response.getShuffleId(), handle.endpoints().size());

        return handle;
    }

    private void rememberCleanupAccess(String executionId, ShuffleHandle handle) {
        queryCleanupAccess.compute(executionId, (queryId, existingCleanup) -> {
            ShuffleAccess renewed = handle.cleanupAccess();
            if (existingCleanup == null || existingCleanup.equals(renewed)) {
                return renewed;
            }
            if (renewed.expiresAtEpochMs() <= existingCleanup.expiresAtEpochMs()) {
                throw new IllegalStateException(
                        "shuffle cleanup capability changed without a later expiry");
            }
            return renewed;
        });
    }

    private ShuffleHandle reuseRegisteredShuffle(
            ShuffleRegistration registration, QueryStageKey key) {
        RegisteredShuffle existing = registeredShuffles.get(key);
        if (existing != null) {
            long existingEpoch = existing.registration().planEpoch();
            if (existingEpoch > registration.planEpoch()) {
                throw new IllegalStateException(
                        "stale shuffle registration generation");
            }
            if (existingEpoch == registration.planEpoch()) {
                requireSameRegistration(existing.registration(), registration);
                if (!requiresRegistrationRefresh(existing.handle())) {
                    return existing.handle();
                }
                if (existing.handle().readAccess().expiresAtEpochMs()
                                > System.currentTimeMillis()
                        && existing.handle().writeAccess().expiresAtEpochMs()
                                > System.currentTimeMillis()) {
                    ShuffleHandle refreshed = refreshDataAccess(existing);
                    registeredShuffles.put(key, new RegisteredShuffle(
                            registration, refreshed, existing.owner()));
                    return refreshed;
                }
            }
        }
        return null;
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        checkNotNull(queryId, "queryId must not be null");
        return operations.execute(queryId, () -> {
            ensureOpen();
            return commitShuffleOwned(queryId, stageId);
        });
    }

    private ShuffleCommitResult commitShuffleOwned(String queryId, int stageId) {
        QueryStageKey key = new QueryStageKey(queryId, stageId);
        ShuffleCommitResult existing = finalizedShuffles.get(key);
        if (existing != null) {
            return existing;
        }
        RegisteredShuffle registered = registeredShuffles.get(key);
        if (registered == null) {
            throw new IllegalStateException(
                    "Cannot finalize an unregistered shuffle stage: " + key);
        }
        ShuffleRegistration registration = registered.registration();
        ShuffleHandle handle = registered.handle();
        FinalizeShuffleRequest request = FinalizeShuffleRequest.newBuilder()
                .setShuffleId(handle.shuffleId())
                .setPlanEpoch(registration.planEpoch())
                .setExecutionOwner(toProtocol(registered.owner()))
                .setExpectedProducerPartitions(registration.expectedProducerCount())
                .setExpectedProducerSetChecksum(toProtocolChecksum(
                        registration.expectedProducerSetChecksum()))
                .setAccessCapability(finalizeCapability(registered))
                .build();
        FinalizeShuffleResponse response = stubFor(handle.commitAddress())
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .finalizeShuffle(request);
        ShuffleCommitResult result = validateFinalization(registration, response);
        var observer = evidence.find(queryId);
        if (observer != null) observer.finalized(registration, handle, request, response);
        finalizedShuffles.put(key, result);
        LOG.info("event=shuffle_finalized queryId={} stageId={} shuffleId={} "
                        + "producerCount={} partitions={} totalBytes={} totalRows={}",
                queryId, stageId, handle.shuffleId(),
                response.getCommittedProducerCount(), result.partitionStats().size(),
                result.totalBytes(), result.totalRows());
        return result;
    }

    @Override
    public void cleanupShuffle(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        operations.execute(queryId, () -> {
            ensureOpen();
            cleanupShuffleOwned(queryId);
            return null;
        });
    }

    /** Recovers a durable terminal cleanup intent after a fenced owner takeover. */
    public void recoverCleanupShuffle(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        operations.execute(queryId, () -> {
            ensureOpen();
            recoverCleanupOwned(queryId);
            return null;
        });
    }

    private void recoverCleanupOwned(String queryId) {
        ShuffleExecutionOwner owner = requireStableOwner(queryId);
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(selectEndpoint(queryId));
        var renewed = stub.withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP)
                        .setQueryId(queryId)
                        .setExecutionOwner(toProtocol(owner))
                        .build())
                .getAccessCapability();
        var response = stub.withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .cleanup(CleanupRequest.newBuilder()
                        .setQueryId(queryId)
                        .setExecutionOwner(toProtocol(owner))
                        .setAccessCapability(renewed)
                        .build());
        removeQueryState(queryId);
        LOG.info("event=shuffle_cleanup_recovered queryId={} partitionsCleaned={} "
                        + "bytesFreed={}", queryId,
                response.getPartitionsCleaned(), response.getBytesFreed());
    }

    private void cleanupShuffleOwned(String queryId) {
        ShuffleExecutionOwner owner = queryOwners.get(queryId);
        ShuffleAccess cleanupAccess = queryCleanupAccess.get(queryId);
        if (owner == null && cleanupAccess == null) {
            removeQueryState(queryId);
            return;
        }
        if (cleanupAccess == null) {
            recoverCleanupOwned(queryId);
            return;
        }
        if (owner == null) {
            throw new IllegalStateException(
                    "shuffle cleanup capability has no execution owner: " + queryId);
        }
        if (cleanupAccess.expiresAtEpochMs()
                <= System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS) {
            recoverCleanupOwned(queryId);
            return;
        }
        ShuffleServiceAddress endpoint = selectEndpoint(queryId);
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);
        CleanupRequest request = CleanupRequest.newBuilder()
                .setQueryId(queryId)
                .setExecutionOwner(toProtocol(owner))
                .setAccessCapability(toProtocol(cleanupAccess))
                .build();
        var response = stub
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .cleanup(request);
        removeQueryState(queryId);
        LOG.info("event=shuffle_cleanup queryId={} partitionsCleaned={} bytesFreed={}",
                queryId, response.getPartitionsCleaned(), response.getBytesFreed());
    }

    private void removeQueryState(String queryId) {
        registeredShuffles.keySet().removeIf(key -> key.queryId().equals(queryId));
        finalizedShuffles.keySet().removeIf(key -> key.queryId().equals(queryId));
        queryOwners.remove(queryId);
        queryCleanupAccess.remove(queryId);
        evidence.remove(queryId);
    }

    private boolean requiresRegistrationRefresh(ShuffleHandle handle) {
        long threshold = System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS;
        return handle.readAccess().expiresAtEpochMs() <= threshold
                || handle.writeAccess().expiresAtEpochMs() <= threshold
                || handle.finalizeAccess().expiresAtEpochMs() <= threshold
                || handle.cleanupAccess().expiresAtEpochMs() <= threshold;
    }

    private ShuffleHandle refreshDataAccess(RegisteredShuffle registered) {
        ShuffleHandle old = registered.handle();
        ShuffleAccess read = renewDataAccess(registered,
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ, old.readAccess());
        ShuffleAccess write = renewDataAccess(registered,
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE, old.writeAccess());
        return new ShuffleHandle(old.shuffleId(), old.endpoints(), old.commitAddress(),
                read, write, old.finalizeAccess(), old.cleanupAccess(),
                old.expectedProducerCount(), old.expectedProducerSetChecksum());
    }

    private ShuffleAccess renewDataAccess(
            RegisteredShuffle registered,
            ShuffleAccessOperation operation,
            ShuffleAccess previous) {
        ShuffleHandle handle = registered.handle();
        var renewed = stubFor(handle.commitAddress())
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(operation)
                        .setShuffleId(handle.shuffleId())
                        .setQueryId(registered.registration().queryId())
                        .setExecutionOwner(toProtocol(registered.owner()))
                        .setPreviousCapability(toProtocol(previous))
                        .build())
                .getAccessCapability();
        if (renewed.getToken().isEmpty()
                || renewed.getExpiresAtEpochMs()
                        <= System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS) {
            throw new IllegalStateException("shuffle service returned non-durable renewal");
        }
        return new ShuffleAccess(renewed.getToken().toByteArray(),
                renewed.getExpiresAtEpochMs());
    }

    /**
     * Replaces Coordinator-held stage data tickets with grants for the exact
     * control-bound task attempt immediately before Worker dispatch.
     */
    public SubmitTaskRequest bindTaskAccess(SubmitTaskRequest request) {
        checkNotNull(request, "request must not be null");
        ensureOpen();
        return taskGrants.bind(request);
    }

    private org.pangrova.protocol.v2.ShuffleAccessCapability finalizeCapability(
            RegisteredShuffle registered) {
        ShuffleHandle handle = registered.handle();
        if (handle.finalizeAccess().expiresAtEpochMs()
                > System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS) {
            return toProtocol(handle.finalizeAccess());
        }
        return stubFor(handle.commitAddress())
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE)
                        .setShuffleId(handle.shuffleId())
                        .setExecutionOwner(toProtocol(registered.owner()))
                        .build())
                .getAccessCapability();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        operations.awaitIdle();
        CoordinatorShuffleChannelSecurity.closeChannels(channels.values());
        channels.clear();
        stubs.clear();
        queryOwners.clear();
        queryCleanupAccess.clear();
        registeredShuffles.clear();
        finalizedShuffles.clear();
        evidence.clear();
        LOG.info("event=coordinator_shuffle_lifecycle_closed");
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Coordinator shuffle lifecycle manager is closed");
        }
    }

    private ShuffleServiceAddress selectEndpoint(String queryId) {
        int index = Math.floorMod(queryId.hashCode(), endpoints.size());
        return endpoints.get(index);
    }

    private ShuffleServiceGrpc.ShuffleServiceBlockingStub stubFor(
            ShuffleServiceAddress endpoint) {
        boolean endpointTls = !endpoint.tlsIdentity().isBlank();
        if (endpointTls != (tlsConfig != null)) {
            throw new IllegalStateException(
                    "shuffle endpoint TLS identity does not match Coordinator TLS policy");
        }
        String key = endpoint.host() + ":" + endpoint.port()
                + ":" + endpoint.tlsIdentity();
        return stubs.computeIfAbsent(key, k -> {
            ManagedChannelBuilder<?> builder = Grpc.newChannelBuilderForAddress(
                    endpoint.host(), endpoint.port(), channelCredentials);
            if (endpointTls) {
                builder.overrideAuthority(endpoint.tlsIdentity());
            }
            ManagedChannel channel = builder.build();
            channels.put(key, channel);
            return ShuffleServiceGrpc.newBlockingStub(channel);
        });
    }

    private ShuffleExecutionOwner requireStableOwner(String queryId) {
        ShuffleExecutionOwner resolved = checkNotNull(
                ownerResolver.resolve(queryId), "shuffle owner must not be null");
        ShuffleExecutionOwner existing = queryOwners.putIfAbsent(queryId, resolved);
        if (existing != null && !existing.equals(resolved)) {
            throw new IllegalStateException(
                    "Shuffle owner changed during one query lifecycle: " + queryId);
        }
        return existing == null ? resolved : existing;
    }

    private record QueryStageKey(String queryId, int stageId) {}

    private record RegisteredShuffle(
            ShuffleRegistration registration,
            ShuffleHandle handle,
            ShuffleExecutionOwner owner) {}
}
