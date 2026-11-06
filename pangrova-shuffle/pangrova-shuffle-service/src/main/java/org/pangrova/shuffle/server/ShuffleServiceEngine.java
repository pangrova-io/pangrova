package org.pangrova.shuffle.server;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.security.MessageDigest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

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
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.CapabilityOperation;
import org.pangrova.shuffle.security.CapabilityScope;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.shuffle.server.error.FailureKind;
import org.pangrova.shuffle.server.error.ShuffleServiceFailure;
import org.pangrova.shuffle.transport.CompleteWriteBatch;

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
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionExpectation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

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

    StoredBatch acceptBatch(CompleteWriteBatch batch) {
        Allocation allocation = access.requireAllocation(batch.shuffleId());
        access.verifyTask(CapabilityOperation.WRITE, allocation,
                batch.attempt(), batch.capability());
        ShuffleAccessControl.requireProducerAttempt(allocation, batch.attempt(), batch.outputPartitionId());
        String key = objectKey(batch);
        StoredBatch stored = new StoredBatch(
                new BatchIdentity(batch.shuffleId(), batch.attempt().getPartitionId(),
                        batch.attempt().getAttemptId(), batch.outputPartitionId(),
                        batch.batchSequence()),
                batch.attempt(), key, batch.canonicalSize(),
                batch.rowCount(), batch.checksum());
        String reservationId = batches.beginWriteIntent(stored,
                Math.addExact(config.objectStorage().apiCallTimeout().toMillis(),
                        WRITE_INTENT_DRAIN_GRACE_MILLIS));
        // A failed network PUT is ambiguous: the object server can still commit it.
        // Retain the IN_PROGRESS reservation so cleanup cannot pass a late PUT.
        objects.putImmutable(key, batch::openCanonicalStream,
                batch.canonicalSize(), batch.checksum());
        try {
            batches.completeWriteIntent(reservationId, stored);
        } catch (RuntimeException | Error failure) {
            try {
                // Never delete here: a duplicate request may already have committed this key.
                // Cleanup sweeps the durable reservation's shuffle prefix after write drain.
                batches.abortWriteIntent(reservationId);
            } catch (RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            throw failure;
        }
        return stored;
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
        long version = batches.committedMetadataVersion(request.getShuffleId());
        List<FinalizedPartition> partitions = new ArrayList<>(
                allocation.shape().partitionCount());
        for (int partition = 0; partition < allocation.shape().partitionCount(); partition++) {
            partitions.add(finalizePartition(request.getShuffleId(), partition));
        }
        FinalizeFence fence = new FinalizeFence(request.getPlanEpoch(),
                request.getExecutionOwner(), request.getExpectedProducerPartitions(),
                ShuffleChecksums.requireSha256(request.getExpectedProducerSetChecksum(),
                        "producer set checksum"), version);
        FinalizeResult result = lifecycle.finalizeShuffle(
                new FinalizeCommand(request.getShuffleId(), fence, partitions));
        return finalizationResponse(result);
    }

    ReadResult read(ReadPartitionRequest request) {
        Allocation allocation = access.requireAllocation(request.getShuffleId());
        access.verifyTask(CapabilityOperation.READ, allocation,
                request.getConsumerAttempt(), request.getAccessCapability());
        ShuffleAccessControl.requireConsumerAttempt(allocation, request.getConsumerAttempt());
        if (request.getDeadlineEpochMs() <= clock.millis()) {
            throw new ShuffleServiceFailure(FailureKind.DEADLINE,
                    "shuffle read deadline has expired");
        }
        if (request.getPartitionId() < 0
                || request.getPartitionId() >= allocation.shape().partitionCount()
                || request.getStartSequence() < 0) {
            throw new IllegalArgumentException("shuffle read range is invalid");
        }
        FinalizeResult finalization = lifecycle.findFinalization(request.getShuffleId())
                .orElseThrow(() -> new IllegalStateException("shuffle is not finalized"));
        return new ReadResult(allocation, finalization,
                request.getPartitionId(), request.getStartSequence(),
                request.getDeadlineEpochMs(),
                request.getAccessCapability().getExpiresAtEpochMs());
    }

    List<StoredBatch> readPage(ReadResult result, long offset) {
        requireReadLease(result);
        if (result.deadlineEpochMs() <= clock.millis()) {
            throw new ShuffleServiceFailure(FailureKind.DEADLINE,
                    "shuffle read deadline expired during metadata retrieval");
        }
        lifecycle.requireLiveOwner(result.allocation().identity().queryId(),
                result.allocation().owner());
        return batches.readFinalizedPage(result.allocation().identity().shuffleId(),
                result.partitionId(), offset, BATCH_PAGE_SIZE);
    }

    byte[] readPayload(ReadResult result, StoredBatch batch) {
        requireReadLease(result);
        if (result.deadlineEpochMs() <= clock.millis()) {
            throw new ShuffleServiceFailure(FailureKind.DEADLINE,
                    "shuffle read deadline expired during object retrieval");
        }
        lifecycle.requireLiveOwner(result.allocation().identity().queryId(),
                result.allocation().owner());
        byte[] payload = objects.get(batch.objectKey());
        requireDigest(payload, batch.checksum(), "stored batch");
        return payload;
    }

    private void requireReadLease(ReadResult result) {
        if (result.capabilityExpiresAtMs() <= clock.millis()) {
            throw new SecurityException("shuffle read capability expired at batch boundary");
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

    private FinalizedPartition finalizePartition(String shuffleId, int partition) {
        var manifest = ShufflePartitionManifestChecksum.accumulator();
        long bytes = 0;
        long rows = 0;
        long count = 0;
        int producerAfter = -1;
        long batchAfter = -1;
        List<StoredBatch> page;
        do {
            page = batches.readCommittedPage(shuffleId, partition,
                    producerAfter, batchAfter, BATCH_PAGE_SIZE);
            for (StoredBatch batch : page) {
                manifest.add(count, batch.sizeBytes(), batch.rowCount(), batch.checksum());
                bytes = Math.addExact(bytes, batch.sizeBytes());
                rows = Math.addExact(rows, batch.rowCount());
                count++;
                producerAfter = batch.identity().producerPartitionId();
                batchAfter = batch.identity().batchSequence();
            }
        } while (page.size() == BATCH_PAGE_SIZE);
        return new FinalizedPartition(partition, bytes, rows, count - 1L,
                manifest.finish());
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
            throw new ShuffleServiceFailure(FailureKind.CORRUPT,
                    subject + " checksum does not match metadata");
        }
    }

    private static String objectKey(CompleteWriteBatch batch) {
        TaskAttemptId attempt = batch.attempt();
        return batch.shuffleId() + "/producer-" + attempt.getPartitionId()
                + "/attempt-" + attempt.getAttemptId()
                + "/output-" + batch.outputPartitionId()
                + "/batch-" + batch.batchSequence() + ".bin";
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
            Allocation allocation,
            FinalizeResult finalization,
            int partitionId,
            long startSequence,
            long deadlineEpochMs,
            long capabilityExpiresAtMs) {}
}
