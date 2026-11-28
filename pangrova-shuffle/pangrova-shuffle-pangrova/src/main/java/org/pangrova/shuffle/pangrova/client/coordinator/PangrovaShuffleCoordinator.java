package org.pangrova.shuffle.pangrova.client.coordinator;

import org.pangrova.shuffle.api.enums.ShuffleDataOperation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CleanupResponse;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleCleanupRecovery;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence.Allocation;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.coordinator.RecoverableShuffleControl;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorSession;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool.PemTlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.validation.Preconditions.checkNotNull;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.requireSameRegistration;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.toHandle;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.toProtocol;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.toProtocolChecksum;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.toRegisterRequest;
import static org.pangrova.shuffle.pangrova.client.coordinator.PangrovaShuffleControlProtocol.validateFinalization;

/** Owns the Pangrova shuffle service control channels, allocations and confirmed publications. */
@ThreadSafe
public final class PangrovaShuffleCoordinator
        implements ShuffleCoordinatorSession, ShuffleCleanupRecovery, RecoverableShuffleControl,
                ExternalShuffleDescriptorEncoder {

    private static final Logger LOG =
            LoggerFactory.getLogger(PangrovaShuffleCoordinator.class);
    private static final long RPC_TIMEOUT_MS = 10_000;
    private static final long GRANT_RENEWAL_MARGIN_MS = 30_000L;
    private static final long CAPABILITY_REFRESH_MARGIN_MS = 60_000;

    private final List<ShuffleServiceAddress> endpoints;
    private final ShuffleExecutionOwnerResolver ownerResolver;
    private final PemTlsConfig tlsConfig;
    private final ChannelCredentials channelCredentials;
    private final ConcurrentHashMap<String, ManagedChannel> channels =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, QueryShuffleState> queryStates =
            new ConcurrentHashMap<>();
    private final ShuffleLifecycleOperations operations = new ShuffleLifecycleOperations();
    private volatile boolean closed;
    private boolean drained;
    private final ShuffleEvidenceRegistry evidence = new ShuffleEvidenceRegistry();

    /** Installs durable control before any stage allocation or Worker dispatch for this exact execution. */
    @Override
    public void bindDurableEvidence(String executionId, ShuffleLifecycleEvidence sink) {
        operations.execute(executionId, () -> {
            ensureOpen();
            evidence.bind(executionId, sink);
            return null;
        });
    }

    /** Drops only volatile failed-pass state; the durable allocation and external producer bytes remain intact. */
    @Override
    public void detachDurableControl(String executionId, ShuffleExecutionOwner expectedOwner) {
        operations.execute(executionId, () -> {
            var state = queryStates.get(executionId);
            if (state != null && !state.owner.equals(expectedOwner)) {
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
    public PangrovaShuffleCoordinator(
            List<ShuffleServiceAddress> endpoints,
            ShuffleExecutionOwnerResolver ownerResolver,
            PemTlsConfig tlsConfig) {
        checkNotNull(endpoints, "endpoints must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException(
                    "shuffle endpoints must not be empty for REMOTE mode");
        }
        this.endpoints = List.copyOf(endpoints);
        this.ownerResolver = checkNotNull(
                ownerResolver, "ownerResolver must not be null");
        this.tlsConfig = tlsConfig;
        ShuffleControlChannels.validateTransportSecurity(this.endpoints, tlsConfig);
        this.channelCredentials = ShuffleControlChannels.channelCredentials(tlsConfig);
        LOG.info("event=coordinator_shuffle_lifecycle_init endpointCount={}",
                endpoints.size());
    }

    @Override
    public ShuffleCapabilityDescriptor capabilities() {
        return PangrovaShuffleCoordinatorFactory.CAPABILITIES;
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
        QueryShuffleState state = requireStableState(registration.queryId());
        ShuffleExecutionOwner owner = state.owner;
        ShuffleHandle reused = reuseRegisteredShuffle(registration, state);
        if (reused != null) {
            return reused;
        }
        var observer = evidence.find(registration.queryId());
        var restored = observer == null ? java.util.Optional.<ShuffleLifecycleEvidence.Allocation>empty()
                : observer.restoredAllocation(registration);
        // Verify persisted bytes before opening a channel. Only configured control services receive owner identity.
        // A service may advertise a different task address behind its configured discovery endpoint.
        ShuffleServiceAddress endpoint = restored
                .map(allocation -> PangrovaAllocationCodec.decode(allocation, registration.numPartitions()))
                .filter(endpoints::contains).orElseGet(() -> selectEndpoint(registration.queryId()));
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);

        if (restored.isPresent()) {
            // Register accepts a caller-assigned ID for new allocations. Recover the existing allocation under
            // its current database fence first, so lost storage cannot be recreated as an empty exchange.
            recoverFinalizeGrant(stub, restored.orElseThrow().shuffleId(), owner);
        }
        RegisterShuffleRequest request = toRegisterRequest(registration, owner).toBuilder()
                .setShuffleId(restored.map(ShuffleLifecycleEvidence.Allocation::shuffleId).orElse(""))
                .build();

        RegisterShuffleResponse response = stub
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .registerShuffle(request);

        ShuffleHandle handle = toHandle(response, registration);
        ShuffleControlChannels.validateHandleTransportSecurity(handle, tlsConfig);
        if (observer != null) {
            var allocation = allocation(handle, registration);
            if (restored.isPresent() && !allocation.equals(restored.orElseThrow())) {
                throw new IllegalStateException("External service changed the restored immutable shuffle allocation");
            }
            observer.registered(registration, allocation, owner);
        }
        state.rememberCleanupAccess(handle.cleanupAccess());
        state.stages.put(registration.stageId(), new RegisteredShuffle(registration, handle, null));
        LOG.info("event=shuffle_registered queryId={} stageId={} shuffleId={} "
                        + "endpointCount={}",
                registration.queryId(), registration.stageId(),
                response.getShuffleId(), handle.endpoints().size());

        return handle;
    }

    @Override
    public ShuffleProviderDescriptor describe(String allocationId, ShuffleServiceAddress commitAddress,
            List<ShuffleServerEndpoint> serverEndpoints, int partitionCount) {
        return PangrovaAllocationCodec.encode(allocationId, commitAddress, serverEndpoints, partitionCount);
    }

    private Allocation allocation(ShuffleHandle handle, ShuffleRegistration registration) {
        return new Allocation(handle.shuffleId(), describe(handle.shuffleId(), handle.commitAddress(),
                handle.endpoints(), registration.numPartitions()));
    }

    private ShuffleHandle reuseRegisteredShuffle(
            ShuffleRegistration registration, QueryShuffleState state) {
        RegisteredShuffle existing = state.stages.get(registration.stageId());
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
                    ShuffleHandle refreshed = refreshDataAccess(existing.handle(), state.owner);
                    state.stages.put(registration.stageId(), new RegisteredShuffle(
                            registration, refreshed, existing.receipt()));
                    return refreshed;
                }
            }
        }
        return null;
    }

    @Override
    public ShuffleCommitReceipt commitShuffle(ShufflePublication publication) {
        checkNotNull(publication, "publication");
        return operations.execute(publication.queryId(), () -> {
            ensureOpen();
            return commitShuffleOwned(publication);
        });
    }

    private ShuffleCommitReceipt commitShuffleOwned(ShufflePublication publication) {
        String queryId = publication.queryId();
        int stageId = publication.stageId();
        QueryShuffleState state = queryStates.get(queryId);
        RegisteredShuffle registered = state == null ? null : state.stages.get(stageId);
        if (registered == null || registered.registration().planEpoch() != publication.generation()
                || !registered.handle().shuffleId().equals(publication.allocationId())) {
            throw new IllegalStateException("Shuffle allocation is unregistered or stale: " + publication);
        }
        requireStableState(queryId);
        ShuffleCommitReceipt existing = registered.receipt();
        if (existing != null) {
            return existing;
        }
        ShuffleRegistration registration = registered.registration();
        ShuffleHandle handle = registered.handle();
        FinalizeShuffleRequest request = FinalizeShuffleRequest.newBuilder()
                .setShuffleId(handle.shuffleId())
                .setPlanEpoch(registration.planEpoch())
                .setExecutionOwner(toProtocol(state.owner))
                .setExpectedProducerPartitions(registration.expectedProducerCount())
                .setExpectedProducerSetChecksum(toProtocolChecksum(
                        registration.expectedProducerSetChecksum()))
                .setAccessCapability(finalizeCapability(handle, state.owner))
                .build();
        FinalizeShuffleResponse response = stubFor(handle.commitAddress())
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .finalizeShuffle(request);
        ShuffleCommitReceipt result = validateFinalization(publication, registration, response);
        var observer = evidence.find(queryId);
        if (observer != null) {
            observer.finalized(registration, allocation(handle, registration), state.owner, result,
                    response.toByteArray());
        }
        state.stages.put(stageId, new RegisteredShuffle(registration, handle, result));
        LOG.info("event=shuffle_finalized queryId={} stageId={} shuffleId={} "
                        + "producerCount={} partitions={} totalBytes={} totalRows={}",
                queryId, stageId, handle.shuffleId(),
                response.getCommittedProducerCount(), result.partitions().size(),
                response.getTotalBytes(), response.getTotalRows());
        return result;
    }

    @Override
    public void cleanupShuffle(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        operations.execute(queryId, () -> {
            ensureOpen();
            var state = queryStates.get(queryId);
            if (state == null) {
                removeQueryState(queryId);
            } else {
                cleanupShuffleOwned(queryId, state);
            }
            return null;
        });
    }

    @Override
    public boolean cleanupTrackedShuffle(String queryId, ShuffleExecutionOwner owner) {
        checkNotNull(queryId, "queryId must not be null");
        return operations.execute(queryId, () -> {
            ensureOpen();
            var state = queryStates.get(queryId);
            if (state == null) return false;
            if (!state.owner.equals(owner)) {
                throw new IllegalStateException("Cannot clean another Shuffle execution owner's scope: " + queryId);
            }
            cleanupShuffleOwned(queryId, state);
            return true;
        });
    }

    /** Recovers a durable terminal cleanup intent after a fenced owner takeover. */
    @Override
    public void recoverCleanupShuffle(String queryId, ShuffleExecutionOwner owner) {
        checkNotNull(queryId, "queryId must not be null");
        operations.execute(queryId, () -> {
            ensureOpen();
            recoverCleanupOwned(queryId, owner);
            return null;
        });
    }

    private void recoverCleanupOwned(String queryId, ShuffleExecutionOwner owner) {
        if (!owner.equals(ownerResolver.resolve(queryId))) {
            throw new SecurityException("Shuffle cleanup owner is no longer current: " + queryId);
        }
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(selectEndpoint(queryId));
        var renewed = stub.withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP)
                        .setQueryId(queryId)
                        .setExecutionOwner(toProtocol(owner))
                        .build())
                .getAccessCapability();
        CleanupRequest request = CleanupRequest.newBuilder()
                .setQueryId(queryId)
                .setExecutionOwner(toProtocol(owner))
                .setAccessCapability(renewed)
                .build();
        var response = stub.withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS).cleanup(request);
        acceptCleanupReceipt(request, response, "shuffle_cleanup_recovered");
    }

    private void cleanupShuffleOwned(String queryId, QueryShuffleState state) {
        ShuffleAccess cleanupAccess = state.cleanupAccess;
        if (cleanupAccess == null
                || cleanupAccess.expiresAtEpochMs() <= System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS) {
            recoverCleanupOwned(queryId, state.owner);
            return;
        }
        ShuffleServiceAddress endpoint = selectEndpoint(queryId);
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);
        CleanupRequest request = CleanupRequest.newBuilder()
                .setQueryId(queryId)
                .setExecutionOwner(toProtocol(state.owner))
                .setAccessCapability(toProtocol(cleanupAccess))
                .build();
        var response = stub
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .cleanup(request);
        acceptCleanupReceipt(request, response, "shuffle_cleanup");
    }

    private void acceptCleanupReceipt(CleanupRequest request, CleanupResponse receipt, String event) {
        var owner = request.getExecutionOwner();
        if (!request.getQueryId().equals(receipt.getQueryId()) || !owner.equals(receipt.getExecutionOwner())
                || receipt.getRevocationGeneration() <= 0
                || receipt.getRevocationGeneration() > owner.getFencingToken()
                || receipt.getScopeConvergedAtEpochMillis() <= 0) {
            throw new IllegalStateException("Shuffle cleanup receipt is not bound to the requested query and owner");
        }
        removeQueryState(request.getQueryId());
        LOG.info("event={} queryId={} revocationGeneration={} scopeConvergedAt={} uncertainWriteHandoff={}",
                event, request.getQueryId(), receipt.getRevocationGeneration(),
                receipt.getScopeConvergedAtEpochMillis(), receipt.getUncertainWriteHandoff());
    }

    private void removeQueryState(String queryId) {
        queryStates.remove(queryId);
        evidence.remove(queryId);
    }

    private boolean requiresRegistrationRefresh(ShuffleHandle handle) {
        long threshold = System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS;
        return handle.readAccess().expiresAtEpochMs() <= threshold
                || handle.writeAccess().expiresAtEpochMs() <= threshold
                || handle.finalizeAccess().expiresAtEpochMs() <= threshold
                || handle.cleanupAccess().expiresAtEpochMs() <= threshold;
    }

    private ShuffleHandle refreshDataAccess(ShuffleHandle old, ShuffleExecutionOwner owner) {
        ShuffleAccess read = renewDataAccess(old, owner,
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ, old.readAccess());
        ShuffleAccess write = renewDataAccess(old, owner,
                ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE, old.writeAccess());
        return new ShuffleHandle(old.shuffleId(), old.endpoints(), old.commitAddress(),
                read, write, old.finalizeAccess(), old.cleanupAccess(),
                old.expectedProducerCount(), old.expectedProducerSetChecksum());
    }

    private ShuffleAccess renewDataAccess(
            ShuffleHandle handle,
            ShuffleExecutionOwner owner,
            ShuffleAccessOperation operation,
            ShuffleAccess previous) {
        var renewed = stubFor(handle.commitAddress())
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(operation)
                        .setShuffleId(handle.shuffleId())
                        .setExecutionOwner(toProtocol(owner))
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

    @Override
    public ShuffleAccess grant(ShuffleServiceAddress destination, String allocationId, ExchangeTaskAttempt attempt,
            ShuffleDataOperation operation) {
        java.util.Objects.requireNonNull(operation, "operation");
        if (allocationId == null || allocationId.isBlank()) {
            throw new IllegalArgumentException("shuffle task grant requires an allocation");
        }
        ShuffleAccessOperation accessOperation = switch (operation) {
            case READ -> ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ;
            case WRITE -> ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE;
        };
        return operations.executeGrant(() -> {
            ensureOpen();
            var task = PangrovaShuffleControlProtocol.toProtocol(attempt);
            var renewed = stubFor(destination).withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .renewAccess(RenewAccessRequest.newBuilder()
                            .setOperation(accessOperation)
                            .setShuffleId(allocationId)
                            .setExecutionOwner(task.getExecutionOwner())
                            .setTaskAttempt(task).build()).getAccessCapability();
            if (renewed.getToken().isEmpty()
                    || renewed.getExpiresAtEpochMs() <= System.currentTimeMillis() + GRANT_RENEWAL_MARGIN_MS) {
                throw new SecurityException("shuffle service returned an invalid task grant");
            }
            return new ShuffleAccess(renewed.getToken().toByteArray(), renewed.getExpiresAtEpochMs());
        });
    }

    private org.pangrova.protocol.v2.ShuffleAccessCapability finalizeCapability(
            ShuffleHandle handle, ShuffleExecutionOwner owner) {
        if (handle.finalizeAccess().expiresAtEpochMs()
                > System.currentTimeMillis() + CAPABILITY_REFRESH_MARGIN_MS) {
            return toProtocol(handle.finalizeAccess());
        }
        return recoverFinalizeGrant(stubFor(handle.commitAddress()), handle.shuffleId(), owner);
    }

    private org.pangrova.protocol.v2.ShuffleAccessCapability recoverFinalizeGrant(
            ShuffleServiceGrpc.ShuffleServiceBlockingStub stub, String allocationId, ShuffleExecutionOwner owner) {
        var grant = stub.withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE)
                        .setShuffleId(allocationId).setExecutionOwner(toProtocol(owner)).build())
                .getAccessCapability();
        if (grant.getToken().isEmpty() || grant.getExpiresAtEpochMs() <= System.currentTimeMillis()) {
            throw new IllegalStateException("Shuffle service returned invalid recovery grant");
        }
        return grant;
    }

    @Override
    public synchronized void close() {
        if (drained) {
            return;
        }
        closed = true;
        operations.awaitIdle();
        ShuffleControlChannels.closeChannels(channels.values());
        channels.clear();
        stubs.clear();
        queryStates.clear();
        evidence.clear();
        drained = true;
        LOG.info("event=coordinator_shuffle_lifecycle_closed");
    }

    private void ensureOpen() {
        if (closed) {
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

    private QueryShuffleState requireStableState(String queryId) {
        ShuffleExecutionOwner resolved = checkNotNull(
                ownerResolver.resolve(queryId), "shuffle owner must not be null");
        var state = queryStates.computeIfAbsent(queryId, ignored -> new QueryShuffleState(resolved));
        if (!state.owner.equals(resolved)) {
            throw new IllegalStateException(
                    "Shuffle owner changed during one query lifecycle: " + queryId);
        }
        return state;
    }

    /** Mutable state accessed only during the serialized lifecycle operation for this query. */
    private static final class QueryShuffleState {
        private final ShuffleExecutionOwner owner;
        private final Map<Integer, RegisteredShuffle> stages = new HashMap<>();
        private ShuffleAccess cleanupAccess;

        private QueryShuffleState(ShuffleExecutionOwner owner) {
            this.owner = owner;
        }

        private void rememberCleanupAccess(ShuffleAccess renewed) {
            if (cleanupAccess != null && !cleanupAccess.equals(renewed)
                    && renewed.expiresAtEpochMs() <= cleanupAccess.expiresAtEpochMs()) {
                throw new IllegalStateException(
                        "shuffle cleanup capability changed without a later expiry");
            }
            cleanupAccess = renewed;
        }
    }

    private record RegisteredShuffle(
            ShuffleRegistration registration,
            ShuffleHandle handle,
            ShuffleCommitReceipt receipt) {}
}
