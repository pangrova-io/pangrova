package org.pangrova.shuffle.service.model;

import java.util.Arrays;
import java.util.List;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Internal immutable state exchanged between the service and durable metadata stores. */
public final class ShuffleServiceModel {

    private ShuffleServiceModel() {}

    public enum AllocationState {
        REGISTERED,
        FINALIZED,
        CLEANING
    }

    public record AllocationIdentity(
            String shuffleId,
            String queryId,
            int stageId,
            long planEpoch) {}

    public record AllocationShape(
            int partitionCount,
            int expectedProducerCount,
            byte[] producerSetChecksum) {
        public AllocationShape {
            producerSetChecksum = producerSetChecksum.clone();
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }
    }

    public record Allocation(
            AllocationIdentity identity,
            ExecutionOwnerIdentity owner,
            AllocationShape shape,
            long capabilityExpiresAtMs,
            long cleanupExpiresAtMs,
            long metadataVersion,
            AllocationState state) {}

    public record RegisterCommand(
            String requestedShuffleId,
            String queryId,
            int stageId,
            long planEpoch,
            ExecutionOwnerIdentity owner,
            AllocationShape shape,
            long capabilityExpiresAtMs) {}

    public record BatchIdentity(
            String shuffleId,
            int producerPartitionId,
            int producerAttemptId,
            int outputPartitionId,
            long batchSequence) {}

    public record StoredBatch(
            BatchIdentity identity,
            TaskAttemptId producerAttempt,
            String objectKey,
            long sizeBytes,
            long rowCount,
            byte[] checksum) {
        public StoredBatch {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }

        public boolean sameContent(StoredBatch other) {
            return identity.equals(other.identity)
                    && producerAttempt.equals(other.producerAttempt)
                    && objectKey.equals(other.objectKey)
                    && sizeBytes == other.sizeBytes
                    && rowCount == other.rowCount
                    && Arrays.equals(checksum, other.checksum);
        }
    }

    public record PartitionExpectation(
            int partitionId,
            long expectedLastSequence,
            long totalBytes,
            long totalBatches,
            long totalRows,
            byte[] checksum) {
        public PartitionExpectation {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    public record CommitCommand(
            String shuffleId,
            TaskAttemptId attempt,
            List<PartitionExpectation> partitions) {
        public CommitCommand {
            partitions = List.copyOf(partitions);
        }
    }

    public record CommitResult(int acceptedPartitions, long totalBytes, long fencingToken) {}

    public record FinalizedPartition(
            int partitionId,
            long totalBytes,
            long totalRows,
            long committedLastSequence,
            byte[] checksum) {
        public FinalizedPartition {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    public record FinalizeFence(
            long planEpoch,
            ExecutionOwnerIdentity owner,
            int expectedProducerCount,
            byte[] producerSetChecksum,
            long metadataVersion) {
        public FinalizeFence {
            producerSetChecksum = producerSetChecksum.clone();
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }
    }

    public record FinalizeCommand(
            String shuffleId,
            FinalizeFence fence,
            List<FinalizedPartition> partitions) {
        public FinalizeCommand {
            partitions = List.copyOf(partitions);
        }
    }

    public record FinalizeResult(
            int committedProducerCount,
            byte[] producerSetChecksum,
            List<FinalizedPartition> partitions,
            long totalBytes,
            long totalRows) {
        public FinalizeResult {
            producerSetChecksum = producerSetChecksum.clone();
            partitions = List.copyOf(partitions);
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }
    }

    public record CleanupCommand(String queryId, ExecutionOwnerIdentity owner) {}

    public record CleanupLease(String owner, long nowMs, long expiresAtMs) {
        public CleanupLease {
            if (owner == null || owner.isBlank() || nowMs <= 0 || expiresAtMs <= nowMs) {
                throw new IllegalArgumentException("cleanup lease is invalid");
            }
        }
    }

    public record CleanupPlan(
            String queryId,
            String leaseOwner,
            long partitionCount,
            long bytes) {
        public CleanupPlan {
            if (queryId == null || queryId.isBlank()
                    || leaseOwner == null || leaseOwner.isBlank()
                    || partitionCount < 0 || bytes < 0) {
                throw new IllegalArgumentException("cleanup plan is invalid");
            }
        }
    }

    public record CleanupCursor(
            String shuffleId,
            int producerPartitionId,
            int producerAttemptId,
            int outputPartitionId,
            long batchSequence) {}

    public record CleanupObjectPage(List<String> objectKeys, CleanupCursor nextCursor) {
        public CleanupObjectPage {
            objectKeys = List.copyOf(objectKeys);
            if (objectKeys.isEmpty() != (nextCursor == null)) {
                throw new IllegalArgumentException("cleanup object page cursor is inconsistent");
            }
        }
    }

    public record CleanupShufflePage(List<String> shuffleIds, String nextShuffleId) {
        public CleanupShufflePage {
            shuffleIds = List.copyOf(shuffleIds);
            if (shuffleIds.isEmpty() != (nextShuffleId == null)) {
                throw new IllegalArgumentException("cleanup shuffle page cursor is inconsistent");
            }
        }
    }
}
