package org.pangrova.shuffle.pangrova.client.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

/** Converts and validates the canonical remote-shuffle control protocol. */
final class PangrovaShuffleControlProtocol {

    private static final int SHA256_BYTES = 32;
    private static final long MINIMUM_CAPABILITY_LIFETIME_MS = 30_000L;

    private PangrovaShuffleControlProtocol() {}

    static RegisterShuffleRequest toRegisterRequest(
            ShuffleRegistration registration,
            ShuffleExecutionOwner owner) {
        return registrationIdentity(registration).toBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setExecutionOwner(toProtocol(owner))
                .build();
    }

    static ShuffleHandle toHandle(
            RegisterShuffleResponse response,
            ShuffleRegistration registration) {
        List<ShuffleServerEndpoint> endpoints = toEndpoints(response);
        ShuffleHandle handle = new ShuffleHandle(
                response.getShuffleId(), endpoints,
                toAddress(response.getCommitAddress()),
                toAccess(response.getReadCapability(), "read"),
                toAccess(response.getWriteCapability(), "write"),
                toAccess(response.getFinalizeCapability(), "finalize"),
                toAccess(response.getCleanupCapability(), "cleanup"),
                registration.expectedProducerCount(),
                registration.expectedProducerSetChecksum());
        handle.validatePartitionCoverage(registration.numPartitions());
        return handle;
    }

    static org.pangrova.protocol.v2.ExecutionOwnerIdentity toProtocol(
            ShuffleExecutionOwner owner) {
        return org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                .setOwnerId(owner.ownerId())
                .setOwnerIncarnation(owner.ownerIncarnation())
                .setFencingToken(owner.fencingToken())
                .build();
    }

    static org.pangrova.protocol.v2.ShuffleAccessCapability toProtocol(
            ShuffleAccess access) {
        return org.pangrova.protocol.v2.ShuffleAccessCapability.newBuilder()
                .setToken(ByteString.copyFrom(access.token()))
                .setExpiresAtEpochMs(access.expiresAtEpochMs())
                .build();
    }

    static Checksum toProtocolChecksum(byte[] digest) {
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(digest))
                .build();
    }

    static org.pangrova.protocol.v2.TaskAttemptId toProtocol(
            org.pangrova.shuffle.api.identity.ExchangeTaskAttempt attempt) {
        return org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                .setQueryOrJobId(attempt.queryId()).setTaskId(attempt.taskId())
                .setStageId(attempt.stageId()).setPartitionId(attempt.taskPartitionId())
                .setControlEpoch(attempt.controlEpoch()).setPlanEpoch(attempt.fence().planEpoch())
                .setAttemptId(attempt.fence().attemptId()).setFencingToken(attempt.fence().fencingToken())
                .setExecutionOwner(toProtocol(attempt.executionOwner()))
                .setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId(attempt.workerAdmissionDomainId()).setIncarnationId(
                                attempt.workerIncarnationId()))
                .build();
    }

    static ShuffleCommitReceipt validateFinalization(ShufflePublication publication,
            ShuffleRegistration registration,
            FinalizeShuffleResponse response) {
        requireProducerSet(registration, response);
        PartitionTotals totals = collectPartitions(registration, response);
        requireTotals(response, totals);
        return new ShuffleCommitReceipt(publication, response.getCommittedProducerCount(),
                HexFormat.of().formatHex(response.getProducerSetChecksum().getDigest().toByteArray()),
                        totals.partitions());
    }

    static void requireSameRegistration(
            ShuffleRegistration existing,
            ShuffleRegistration requested) {
        if (!registrationIdentity(existing).equals(registrationIdentity(requested))) {
            throw new IllegalStateException(
                    "shuffle registration changed for an existing query stage");
        }
    }

    private static RegisterShuffleRequest registrationIdentity(
            ShuffleRegistration registration) {
        return RegisterShuffleRequest.newBuilder()
                .setQueryId(registration.queryId())
                .setStageId(registration.stageId())
                .setNumPartitions(registration.numPartitions())
                .setEstimatedBytesPerPartition(registration.estimatedBytesPerPartition())
                .setRequireReplication(registration.requireReplication())
                .setPlanEpoch(registration.planEpoch())
                .setExpectedProducerPartitions(registration.expectedProducerCount())
                .setExpectedProducerSetChecksum(toProtocolChecksum(
                        registration.expectedProducerSetChecksum()))
                .build();
    }

    private static List<ShuffleServerEndpoint> toEndpoints(
            RegisterShuffleResponse response) {
        List<ShuffleServerEndpoint> result = new ArrayList<>(response.getEndpointsCount());
        for (var endpoint : response.getEndpointsList()) {
            result.add(new ShuffleServerEndpoint(
                    endpoint.getHost(), endpoint.getPort(),
                    endpoint.getPartitionStart(), endpoint.getPartitionEnd(),
                    endpoint.getTlsIdentity()));
        }
        return result;
    }

    private static ShuffleAccess toAccess(
            org.pangrova.protocol.v2.ShuffleAccessCapability capability,
            String operation) {
        if (capability.getToken().isEmpty()
                || capability.getExpiresAtEpochMs()
                        <= System.currentTimeMillis() + MINIMUM_CAPABILITY_LIFETIME_MS) {
            throw new IllegalStateException(
                    "shuffle service returned invalid " + operation + " capability");
        }
        return new ShuffleAccess(
                capability.getToken().toByteArray(), capability.getExpiresAtEpochMs());
    }

    private static ShuffleServiceAddress toAddress(
            org.pangrova.protocol.v2.ShuffleServiceAddress address) {
        return new ShuffleServiceAddress(
                address.getHost(), address.getPort(), address.getTlsIdentity());
    }

    private static void requireProducerSet(
            ShuffleRegistration registration,
            FinalizeShuffleResponse response) {
        if (response.getCommittedProducerCount()
                != registration.expectedProducerCount()) {
            throw new IllegalStateException(
                    "shuffle finalized with an incomplete producer set");
        }
        requireChecksum(
                response.getProducerSetChecksum(),
                registration.expectedProducerSetChecksum(),
                "producer set");
    }

    private static PartitionTotals collectPartitions(
            ShuffleRegistration registration,
            FinalizeShuffleResponse response) {
        Map<Integer, ShuffleCommitReceipt.Partition> partitions = new HashMap<>();
        long partitionBytes = 0;
        long partitionRows = 0;
        for (var partition : response.getPartitionsList()) {
            requireValidPartition(registration, partition);
            int partitionId = partition.getPartitionId();
            var previous = partitions.put(
                    partitionId,
                    new ShuffleCommitReceipt.Partition(partition.getCommittedLastSequence(),
                            partition.getTotalBytes(), partition.getTotalRows(),
                            HexFormat.of().formatHex(partition.getOutputChecksum().getDigest().toByteArray()),
                            readRanges(partition)));
            if (previous != null) {
                throw new IllegalStateException(
                        "shuffle finalization returned a duplicate partition");
            }
            partitionBytes = Math.addExact(partitionBytes, partition.getTotalBytes());
            partitionRows = Math.addExact(partitionRows, partition.getTotalRows());
        }
        if (partitions.size() != registration.numPartitions()) {
            throw new IllegalStateException(
                    "shuffle finalization did not cover every partition");
        }
        return new PartitionTotals(partitions, partitionBytes, partitionRows);
    }

    private static void requireValidPartition(
            ShuffleRegistration registration,
            org.pangrova.protocol.v2.shuffle.PartitionFinalizationSummary partition) {
        int partitionId = partition.getPartitionId();
        if (partitionId < 0 || partitionId >= registration.numPartitions()) {
            throw new IllegalStateException(
                    "shuffle finalization returned an out-of-range partition");
        }
        if (partition.getTotalBytes() < 0 || partition.getTotalRows() < 0
                || partition.getCommittedLastSequence() < -1) {
            throw new IllegalStateException(
                    "shuffle finalization returned invalid partition statistics");
        }
        requireChecksum(partition.getOutputChecksum(), null, "partition output");
    }

    private static List<ShuffleCommitReceipt.ReadRange> readRanges(
            org.pangrova.protocol.v2.shuffle.PartitionFinalizationSummary partition) {
        if (partition.getReadRangesCount() > ShuffleCommitReceipt.MAX_READ_RANGES_PER_PARTITION) {
            throw new IllegalStateException("shuffle finalization returned too many read ranges");
        }
        return partition.getReadRangesList().stream().map(range -> {
            if (!org.pangrova.protocol.shuffle.codec.CanonicalShuffleReadRanges.valid(range)) {
                throw new IllegalStateException("shuffle finalization returned incomplete read range evidence");
            }
            return new ShuffleCommitReceipt.ReadRange(range.getStartSequence(), range.getEndSequenceExclusive(),
                    range.getTotalBytes(), range.getTotalRows(),
                    HexFormat.of().formatHex(range.getManifestSha256().toByteArray()));
        }).toList();
    }

    private static void requireTotals(
            FinalizeShuffleResponse response,
            PartitionTotals totals) {
        if (response.getTotalBytes() < 0 || response.getTotalRows() < 0
                || response.getTotalBytes() != totals.bytes()
                || response.getTotalRows() != totals.rows()) {
            throw new IllegalStateException(
                    "shuffle finalization totals do not match partition statistics");
        }
    }

    private static void requireChecksum(
            Checksum actual,
            byte[] expected,
            String subject) {
        if (actual.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || actual.getDigest().size() != SHA256_BYTES
                || (expected != null
                        && !Arrays.equals(actual.getDigest().toByteArray(), expected))) {
            throw new IllegalStateException(
                    "shuffle finalization returned an invalid " + subject + " checksum");
        }
    }

    private record PartitionTotals(
            Map<Integer, ShuffleCommitReceipt.Partition> partitions,
            long bytes,
            long rows) {}
}
