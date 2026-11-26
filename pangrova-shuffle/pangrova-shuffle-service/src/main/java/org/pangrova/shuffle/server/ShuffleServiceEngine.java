package org.pangrova.shuffle.server;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleServiceAddress;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CleanupResponse;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.PartitionCommitSummary;
import org.pangrova.protocol.v2.shuffle.PartitionFinalizationSummary;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.CapabilityScope;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.shuffle.security.enums.CapabilityOperation;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.enums.ShuffleAllocationState;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationShape;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeFence;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PublishedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionPublication;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionExpectation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;
import static org.pangrova.shuffle.server.ShuffleServiceModel.ReadBatch;

/** Canonical v2 shuffle state machine over durable metadata and object storage. */
final class ShuffleServiceEngine implements AutoCloseable {

    private static final int BATCH_PAGE_SIZE = 1_000;
    private static final long WRITE_INTENT_DRAIN_GRACE_MILLIS = 60_000L;

    private final ShuffleServiceConfig config;
    private final ShuffleLifecycleStore lifecycle;
    private final ShuffleBatchStore batches;
    private final ShuffleObjectStorage objects;
    private final ShuffleCapabilityAuthority capabilities;
    private final Clock clock;
    private final ShuffleCleanupCoordinator cleanup;
    private final ShuffleAccessControl access;

    ShuffleServiceEngine(
            ShuffleServiceConfig config,
            ShuffleLifecycleStore lifecycle,
            ShuffleBatchStore batches,
            ShuffleObjectStorage objects,
            ShuffleCapabilityAuthority capabilities,
            Clock clock) {
        this.config = config;
        this.lifecycle = lifecycle;
        this.batches = batches;
        this.objects = objects;
        this.capabilities = capabilities;
        this.clock = clock;
        this.cleanup = new ShuffleCleanupCoordinator(lifecycle, objects, clock);
        this.access = new ShuffleAccessControl(config, lifecycle, capabilities, clock);
    }

    RegisterShuffleResponse register(RegisterShuffleRequest request) {
        requireRegistration(request);
        long expiry = Math.addExact(clock.millis(), config.security().capabilityTtl().toMillis());
        Allocation allocation = lifecycle.register(new RegisterCommand(
                request.getShuffleId(), request.getQueryId(), request.getStageId(),
                request.getPlanEpoch(), request.getExecutionOwner(),
                new AllocationShape(request.getNumPartitions(),
                        request.getExpectedProducerPartitions(),
                        request.getExpectedProducerSetChecksum().getDigest().toByteArray()),
                expiry));
        return registrationResponse(allocation);
    }

    ShuffleWriteSession writeSession(BooleanSupplier cancelled) {
        return new ShuffleWriteSession(access, batches, objects, clock, recoveryDelayMillis(), cancelled);
    }

    CommitProducerAttemptResponse commit(CommitProducerAttemptRequest request) {
        Allocation allocation = access.requireAllocation(request.getShuffleId());
        access.verifyTask(CapabilityOperation.WRITE, allocation,
                request.getProducerAttempt(), request.getAccessCapability());
        ShuffleAccessControl.requireProducerAttempt(allocation, request.getProducerAttempt(), -1);
        List<PartitionExpectation> expected = new ArrayList<>();
        for (PartitionCommitSummary summary : request.getPartitionsList()) {
            byte[] expectedChecksum = ShuffleChecksums.requireSha256(
                    summary.getOutputChecksum(), "producer partition checksum");
            byte[] actualChecksum = attemptManifest(request.getShuffleId(),
                    request.getProducerAttempt(), summary.getPartitionId());
            if (!MessageDigest.isEqual(actualChecksum, expectedChecksum)) {
                throw new IllegalStateException(
                        "commit manifest checksum does not match durable batches");
            }
            expected.add(new PartitionExpectation(summary.getPartitionId(),
                    summary.getExpectedLastSequence(), summary.getTotalBytes(),
                    summary.getTotalBatches(), summary.getTotalRows(), expectedChecksum));
        }
        CommitResult result = batches.commitAttempt(new CommitCommand(
                request.getShuffleId(), request.getProducerAttempt(), expected));
        return CommitProducerAttemptResponse.newBuilder()
                .setAcceptedPartitions(result.acceptedPartitions())
                .setTotalBytes(result.totalBytes())
                .setCommittedAttemptFencingToken(result.fencingToken()).build();
    }

    FinalizeShuffleResponse finalizeShuffle(FinalizeShuffleRequest request) {
        Allocation allocation = access.requireAllocation(request.getShuffleId());
        access.verify(CapabilityOperation.FINALIZE, allocation, request.getAccessCapability());
        FinalizeFence fence = new FinalizeFence(request.getPlanEpoch(),
                request.getExecutionOwner(), request.getExpectedProducerPartitions(),
                ShuffleChecksums.requireSha256(request.getExpectedProducerSetChecksum(),
                        "producer set checksum"), allocation.metadataVersion());
        fence.requireBinding(allocation);
        if (allocation.state() == ShuffleAllocationState.FINALIZED) {
            return finalizationResponse(lifecycle.finalizeShuffle(
                    new FinalizeCommand(request.getShuffleId(), fence, List.of())));
        }
        if (allocation.state() != ShuffleAllocationState.REGISTERED) {
            throw new IllegalStateException("shuffle allocation is not writable");
        }
        FinalizeResult result = lifecycle.finalizeShuffle(
                new FinalizeCommand(request.getShuffleId(), fence, finalizePartitions(allocation)));
        return finalizationResponse(result);
    }

    ReadResult read(ReadPartitionRequest request) {
        var attempt = request.getConsumerAttempt();
        access.verifyTask(CapabilityOperation.READ, request.getShuffleId(), attempt, request.getAccessCapability());
        ShuffleAccessControl.requireConsumerIdentity(attempt);
        if (request.getDeadlineEpochMs() <= clock.millis()) {
            throw new ShuffleServiceException(FailureKind.DEADLINE,
                    "shuffle read deadline has expired");
        }
        if (request.getPartitionId() < 0 || request.getStartSequence() < 0) {
            throw new IllegalArgumentException("shuffle read range is invalid");
        }
        long capabilityExpiry = request.getAccessCapability().getExpiresAtEpochMs();
        long readUntil = Math.min(request.getDeadlineEpochMs(), capabilityExpiry);
        String queryId = attempt.getQueryOrJobId();
        var owner = attempt.getExecutionOwner();
        long startedNanos = System.nanoTime();
        long startedMillis = clock.millis();
        var read = lifecycle.openRead(queryId, owner, request.getShuffleId(),
                request.getPartitionId(), request.getStartSequence(), readUntil,
                config.objectStorage().apiCallTimeout().toMillis());
        return new ReadResult(request.getShuffleId(), read.published(), request.getStartSequence(),
                request.getDeadlineEpochMs(), capabilityExpiry,
                new ShuffleReadLease(lifecycle, queryId, owner, read.pin(), clock, startedNanos, startedMillis));
    }

    List<ReadBatch> readPage(ReadResult result, long offset) {
        requireReadAuthority(result);
        return result.manifest().page(offset, objects::get, () -> requireReadAuthority(result));
    }

    byte[] readPayload(ReadResult result, ReadBatch batch) {
        requireReadAuthority(result);
        byte[] payload = objects.get(batch.objectKey(), batch.sizeBytes());
        requireDigest(payload, batch.checksum(), "stored batch");
        if (payload.length != batch.sizeBytes()) {
            throw new IllegalStateException("stored batch length differs from finalized metadata");
        }
        requireReadAuthority(result);
        return payload;
    }

    void requireReadAuthority(ReadResult result) {
        long now = clock.millis();
        requireReadDeadline(result, now);
        result.lease().requireLive(now);
        requireReadDeadline(result, clock.millis());
    }

    private static void requireReadDeadline(ReadResult result, long now) {
        if (result.capabilityExpiresAtMs() <= now) {
            throw new SecurityException("shuffle read capability expired at batch boundary");
        }
        if (result.deadlineEpochMs() <= now) {
            throw new ShuffleServiceException(FailureKind.DEADLINE, "shuffle read deadline has expired");
        }
    }

    CleanupResponse cleanup(CleanupRequest request) {
        capabilities.verify(request.getAccessCapability(), CapabilityScope.forQuery(
                request.getQueryId(), request.getExecutionOwner()), clock.millis());
        var plan = cleanup.cleanup(
                new CleanupCommand(request.getQueryId(), request.getExecutionOwner()));
        if (plan.isEmpty()) {
            return CleanupResponse.getDefaultInstance();
        }
        CleanupPlan claimed = plan.orElseThrow();
        return CleanupResponse.newBuilder()
                .setPartitionsCleaned(claimed.partitionCount())
                .setBytesFreed(claimed.bytes()).build();
    }

    RenewAccessResponse renewAccess(RenewAccessRequest request, boolean coordinatorPeer) {
        return access.renewAccess(request, coordinatorPeer);
    }

    int recoverInterruptedCleanups(int limit) {
        return cleanup.recoverInterrupted(limit);
    }

    int sweepUncertainWritePrefixes(int limit) {
        return cleanup.sweepUncertainPrefixes(limit);
    }

    private void requireRegistration(RegisterShuffleRequest request) {
        if (!validRegistrationIdentity(request)
                || !validRegistrationShape(request)
                || !replicationSatisfied(request)) {
            throw new IllegalArgumentException("shuffle registration is incomplete");
        }
        byte[] actual = ShuffleChecksums.requireSha256(
                request.getExpectedProducerSetChecksum(), "producer set checksum");
        byte[] expected = ShuffleProducerSetChecksum.forContiguousPartitions(
                request.getQueryId(), request.getStageId(), request.getPlanEpoch(),
                request.getExpectedProducerPartitions());
        if (!MessageDigest.isEqual(actual, expected)) {
            throw new IllegalArgumentException("producer set checksum is not canonical");
        }
        advertisedEndpoints(request.getNumPartitions());
    }

    private static boolean validRegistrationIdentity(RegisterShuffleRequest request) {
        return request.getProtocol().equals(CanonicalProtocolIdentity.current())
                && !request.getQueryId().isBlank() && request.getStageId() >= 0
                && request.getPlanEpoch() > 0 && ShuffleAccessControl.validOwner(request.getExecutionOwner())
                && (request.getShuffleId().isBlank()
                        || ShuffleAccessControl.validShuffleId(request.getShuffleId()));
    }

    private static boolean validRegistrationShape(RegisterShuffleRequest request) {
        return request.getNumPartitions() > 0
                && request.getExpectedProducerPartitions() > 0;
    }

    private boolean replicationSatisfied(RegisterShuffleRequest request) {
        return !request.getRequireReplication()
                || config.objectStorage().replicationEnabled();
    }

    private RegisterShuffleResponse registrationResponse(Allocation allocation) {
        ExecutionOwnerIdentity owner = allocation.owner();
        String shuffleId = allocation.identity().shuffleId();
        return RegisterShuffleResponse.newBuilder()
                .setShuffleId(shuffleId)
                .addAllEndpoints(advertisedEndpoints(allocation.shape().partitionCount()))
                .setCommitAddress(ShuffleServiceAddress.newBuilder()
                        .setHost(config.server().advertisedHost())
                        .setPort(config.server().port())
                        .setTlsIdentity(config.server().tlsIdentity()))
                .setReadCapability(access.issue(CapabilityOperation.READ, shuffleId, owner,
                        allocation.capabilityExpiresAtMs()))
                .setWriteCapability(access.issue(CapabilityOperation.WRITE, shuffleId, owner,
                        allocation.capabilityExpiresAtMs()))
                .setFinalizeCapability(access.issue(CapabilityOperation.FINALIZE, shuffleId, owner,
                        allocation.capabilityExpiresAtMs()))
                .setCleanupCapability(capabilities.issue(CapabilityScope.forQuery(
                        allocation.identity().queryId(), owner), allocation.cleanupExpiresAtMs()))
                .build();
    }

    private List<ShuffleServiceEndpoint> advertisedEndpoints(int partitionCount) {
        List<ShuffleServiceEndpoint> result = new ArrayList<>();
        int expectedStart = 0;
        for (ShuffleServiceConfig.Endpoint endpoint : config.server().endpoints()) {
            int end = endpoint.partitionEnd() == -1 ? partitionCount : endpoint.partitionEnd();
            if (endpoint.partitionStart() != expectedStart || end > partitionCount
                    || end <= endpoint.partitionStart()) {
                throw new IllegalStateException("shuffle endpoints do not exactly cover allocation");
            }
            result.add(ShuffleServiceEndpoint.newBuilder()
                    .setHost(endpoint.host()).setPort(endpoint.port())
                    .setTlsIdentity(endpoint.tlsIdentity())
                    .setPartitionStart(endpoint.partitionStart()).setPartitionEnd(end).build());
            expectedStart = end;
        }
        if (expectedStart != partitionCount) {
            throw new IllegalStateException("shuffle endpoints do not exactly cover allocation");
        }
        return List.copyOf(result);
    }

    private List<PartitionPublication> finalizePartitions(Allocation allocation) {
        int partitionCount = allocation.shape().partitionCount();
        List<PartitionPublication> publications = new ArrayList<>(partitionCount);
        ShufflePartitionManifest.NodeWriter storeNode = (digest, bytes) -> putManifestNode(allocation, digest, bytes);
        var inventory = new ShufflePartitionManifest.Writer(storeNode);
        BatchIdentity after = null;
        List<StoredBatch> page;
        do {
            page = batches.readCommittedPage(allocation, after, BATCH_PAGE_SIZE);
            for (StoredBatch batch : page) {
                int partition = batch.identity().outputPartitionId();
                if (partition < publications.size() || partition >= partitionCount) {
                    throw new IllegalStateException("committed shuffle batch has an invalid partition");
                }
                while (publications.size() < partition) {
                    publications.add(inventory.finish(publications.size()));
                    inventory = new ShufflePartitionManifest.Writer(storeNode);
                }
                inventory.add(batch);
                after = batch.identity();
            }
        } while (page.size() == BATCH_PAGE_SIZE);
        publications.add(inventory.finish(publications.size()));
        while (publications.size() < partitionCount) {
            publications.add(new ShufflePartitionManifest.Writer(storeNode).finish(publications.size()));
        }
        return publications;
    }

    private void putManifestNode(Allocation allocation, byte[] digest, byte[] bytes) {
        String shuffleId = allocation.identity().shuffleId();
        String key = ShufflePartitionManifest.objectKey(shuffleId, digest);
        String reservation = lifecycle.beginManifestWrite(allocation, key, recoveryDelayMillis());
        objects.putImmutable(key, () -> new ByteArrayInputStream(bytes), bytes.length, digest);
        lifecycle.settleManifestWrite(reservation, shuffleId, key);
    }

    private long recoveryDelayMillis() {
        return Math.addExact(config.objectStorage().apiCallTimeout().toMillis(), WRITE_INTENT_DRAIN_GRACE_MILLIS);
    }

    private FinalizeShuffleResponse finalizationResponse(FinalizeResult result) {
        FinalizeShuffleResponse.Builder response = FinalizeShuffleResponse.newBuilder()
                .setCommittedProducerCount(result.committedProducerCount())
                .setProducerSetChecksum(ShuffleChecksums.protocol(
                        result.producerSetChecksum()))
                .setTotalBytes(result.totalBytes()).setTotalRows(result.totalRows());
        for (FinalizedPartition partition : result.partitions()) {
            response.addPartitions(PartitionFinalizationSummary.newBuilder()
                    .setPartitionId(partition.partitionId())
                    .setTotalBytes(partition.totalBytes()).setTotalRows(partition.totalRows())
                    .setCommittedLastSequence(partition.committedLastSequence())
                    .setOutputChecksum(ShuffleChecksums.protocol(partition.checksum())));
        }
        return response.build();
    }

    private byte[] attemptManifest(
            String shuffleId,
            TaskAttemptId attempt,
            int outputPartitionId) {
        var manifest = ShufflePartitionManifestChecksum.accumulator();
        long after = -1;
        List<StoredBatch> page;
        do {
            page = batches.readAttemptPage(shuffleId, attempt,
                    outputPartitionId, after, BATCH_PAGE_SIZE);
            for (StoredBatch batch : page) {
                objects.verifyStored(batch.objectKey(), batch.sizeBytes(), batch.checksum());
                manifest.add(batch.identity().batchSequence(), batch.sizeBytes(),
                        batch.rowCount(), batch.checksum());
                after = batch.identity().batchSequence();
            }
        } while (page.size() == BATCH_PAGE_SIZE);
        return manifest.finishNonEmpty();
    }

    private static void requireDigest(byte[] payload, byte[] expected, String subject) {
        if (!MessageDigest.isEqual(ShuffleChecksums.sha256(payload), expected)) {
            throw new ShuffleServiceException(FailureKind.CORRUPT,
                    subject + " checksum does not match metadata");
        }
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            objects.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            lifecycle.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    record ReadResult(
            FinalizedPartition partition,
            long planEpoch,
            int committedProducerCount,
            byte[] producerSetChecksum,
            long startSequence,
            long deadlineEpochMs,
            long capabilityExpiresAtMs,
            ShuffleReadLease lease,
            ShufflePartitionManifest.Reader manifest) implements AutoCloseable {
        ReadResult {
            producerSetChecksum = producerSetChecksum.clone();
        }

        ReadResult(String shuffleId, PublishedPartition published, long startSequence,
                long deadlineEpochMs, long capabilityExpiresAtMs, ShuffleReadLease lease) {
            this(published.partition(), published.planEpoch(), published.committedProducerCount(),
                    published.producerSetChecksum(), startSequence, deadlineEpochMs, capabilityExpiresAtMs, lease,
                    new ShufflePartitionManifest.Reader(shuffleId, published.partition().partitionId(),
                            published.partition().committedLastSequence(), published.publication().manifestRoot()));
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }

        @Override
        public void close() {
            manifest.discard();
            lease.close();
        }
    }
}
