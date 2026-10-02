package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.shuffle.ShuffleAccess;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.pangrova.shuffle.ShuffleRegistration;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleServiceAddress;

/** Converts and validates the canonical remote-shuffle control protocol. */
final class ShuffleProtocolAdapter {

    private static final int SHA256_BYTES = 32;
    private static final long MINIMUM_CAPABILITY_LIFETIME_MS = 30_000L;

    private ShuffleProtocolAdapter() {}

    static RegisterShuffleRequest toRegisterRequest(
            ShuffleRegistration registration,
            ShuffleExecutionOwner owner) {
        return registrationIdentity(registration).toBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
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

    static ShuffleCommitResult validateFinalization(
            ShuffleRegistration registration,
            FinalizeShuffleResponse response) {
        requireProducerSet(registration, response);
        PartitionTotals totals = collectPartitions(registration, response);
        requireTotals(response, totals);
        return new ShuffleCommitResult(
                totals.partitionStats(), response.getTotalBytes(), response.getTotalRows());
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
        Map<Integer, ShufflePartitionStats> partitionStats = new HashMap<>();
        long partitionBytes = 0;
        long partitionRows = 0;
        for (var partition : response.getPartitionsList()) {
            requireValidPartition(registration, partition);
            int partitionId = partition.getPartitionId();
            ShufflePartitionStats previous = partitionStats.put(
                    partitionId,
                    new ShufflePartitionStats(
                            partitionId, partition.getTotalBytes(), partition.getTotalRows()));
            if (previous != null) {
                throw new IllegalStateException(
                        "shuffle finalization returned a duplicate partition");
            }
            partitionBytes = Math.addExact(partitionBytes, partition.getTotalBytes());
            partitionRows = Math.addExact(partitionRows, partition.getTotalRows());
        }
        if (partitionStats.size() != registration.numPartitions()) {
            throw new IllegalStateException(
                    "shuffle finalization did not cover every partition");
        }
        return new PartitionTotals(partitionStats, partitionBytes, partitionRows);
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
            Map<Integer, ShufflePartitionStats> partitionStats,
            long bytes,
            long rows) {}
}
