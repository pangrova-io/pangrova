package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.security.MessageDigest;
import java.util.List;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Internal immutable state exchanged between the service and durable metadata stores. */
final class ShuffleServiceModel {

    private ShuffleServiceModel() {}

    record AllocationIdentity(
            String shuffleId,
            String queryId,
            int stageId,
            long planEpoch) {}

    record AllocationShape(
            int partitionCount,
            int expectedProducerCount,
            byte[] producerSetChecksum) {
        AllocationShape {
            producerSetChecksum = producerSetChecksum.clone();
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }
    }

    record Allocation(
            AllocationIdentity identity,
            ExecutionOwnerIdentity owner,
            AllocationShape shape,
            long capabilityExpiresAtMs,
            long cleanupExpiresAtMs,
            long metadataVersion,
            ShuffleAllocationState state) {}

    record RegisterCommand(
            String requestedShuffleId,
            String queryId,
            int stageId,
            long planEpoch,
            ExecutionOwnerIdentity owner,
            AllocationShape shape,
            long capabilityExpiresAtMs) {}

    record BatchIdentity(
            String shuffleId,
            int producerPartitionId,
            int producerAttemptId,
            int outputPartitionId,
            long batchSequence) {
        String objectKey() {
            return producerPrefix(shuffleId, producerPartitionId, producerAttemptId)
                    + "output-" + outputPartitionId + "/batch-" + batchSequence + ".bin";
        }

        static String producerPrefix(String shuffleId, int producerPartitionId, int producerAttemptId) {
            return shuffleId + "/producer-" + producerPartitionId + "/attempt-" + producerAttemptId + "/";
        }
    }

    record StoredBatch(
            BatchIdentity identity,
            TaskAttemptId producerAttempt,
            String objectKey,
            long sizeBytes,
            long rowCount,
            byte[] checksum) {
        StoredBatch {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    record ReadBatch(long outputSequence, String objectKey, long sizeBytes, long rowCount, byte[] checksum) {
        ReadBatch {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    record PublishedPartition(
            long planEpoch, int committedProducerCount, byte[] producerSetChecksum, PartitionPublication publication) {
        PublishedPartition {
            producerSetChecksum = producerSetChecksum.clone();
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }

        FinalizedPartition partition() {
            return publication.partition();
        }
    }

    record PartitionExpectation(
            int partitionId,
            long expectedLastSequence,
            long totalBytes,
            long totalBatches,
            long totalRows,
            byte[] checksum) {
        PartitionExpectation {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    record CommitCommand(
            String shuffleId,
            TaskAttemptId attempt,
            List<PartitionExpectation> partitions) {
        CommitCommand {
            partitions = List.copyOf(partitions);
        }
    }

    record CommitResult(int acceptedPartitions, long totalBytes, long fencingToken) {}

    record FinalizedPartition(
            int partitionId,
            long totalBytes,
            long totalRows,
            long committedLastSequence,
            byte[] checksum) {
        FinalizedPartition {
            checksum = checksum.clone();
        }

        @Override
        public byte[] checksum() {
            return checksum.clone();
        }
    }

    record PartitionPublication(FinalizedPartition partition, byte[] manifestRoot) {
        PartitionPublication {
            long total = Math.addExact(partition.committedLastSequence(), 1);
            if (total < 0 || manifestRoot.length != ShufflePartitionManifest.rootSize(total)) {
                throw new IllegalArgumentException("manifest root does not match finalized partition");
            }
            manifestRoot = manifestRoot.clone();
        }

        @Override
        public byte[] manifestRoot() {
            return manifestRoot.clone();
        }
    }

    record FinalizeFence(
            long planEpoch,
            ExecutionOwnerIdentity owner,
            int expectedProducerCount,
            byte[] producerSetChecksum,
            long metadataVersion) {
        FinalizeFence {
            producerSetChecksum = producerSetChecksum.clone();
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }

        void requireBinding(Allocation allocation) {
            if (!allocation.owner().equals(owner)
                    || allocation.identity().planEpoch() != planEpoch
                    || allocation.shape().expectedProducerCount() != expectedProducerCount
                    || !MessageDigest.isEqual(allocation.shape().producerSetChecksum(), producerSetChecksum)) {
                throw new SecurityException("shuffle finalization fence does not match allocation");
            }
        }
    }

    record FinalizeCommand(
            String shuffleId,
            FinalizeFence fence,
            List<PartitionPublication> partitions) {
        FinalizeCommand {
            partitions = List.copyOf(partitions);
        }
    }

    record FinalizeResult(
            int committedProducerCount,
            byte[] producerSetChecksum,
            List<FinalizedPartition> partitions,
            long totalBytes,
            long totalRows) {
        FinalizeResult {
            producerSetChecksum = producerSetChecksum.clone();
            partitions = List.copyOf(partitions);
        }

        @Override
        public byte[] producerSetChecksum() {
            return producerSetChecksum.clone();
        }
    }

    record CleanupCommand(String queryId, ExecutionOwnerIdentity owner) {}

    record CleanupLease(String owner, long nowMs, long expiresAtMs) {
        CleanupLease {
            if (owner == null || owner.isBlank() || nowMs <= 0 || expiresAtMs <= nowMs) {
                throw new IllegalArgumentException("cleanup lease is invalid");
            }
        }
    }

    record CleanupPlan(
            String queryId,
            String leaseOwner,
            long partitionCount,
            long bytes) {
        CleanupPlan {
            if (queryId == null || queryId.isBlank()
                    || leaseOwner == null || leaseOwner.isBlank()
                    || partitionCount < 0 || bytes < 0) {
                throw new IllegalArgumentException("cleanup plan is invalid");
            }
        }
    }

    record CleanupCursor(
            String shuffleId,
            int producerPartitionId,
            int producerAttemptId,
            int outputPartitionId,
            long batchSequence) {}

    record CleanupObjectPage(List<String> objectKeys, CleanupCursor nextCursor) {
        CleanupObjectPage {
            objectKeys = List.copyOf(objectKeys);
            if (objectKeys.isEmpty() != (nextCursor == null)) {
                throw new IllegalArgumentException("cleanup object page cursor is inconsistent");
            }
        }
    }

    record CleanupShufflePage(List<String> shuffleIds, String nextShuffleId) {
        CleanupShufflePage {
            shuffleIds = List.copyOf(shuffleIds);
            if (shuffleIds.isEmpty() != (nextShuffleId == null)) {
                throw new IllegalArgumentException("cleanup shuffle page cursor is inconsistent");
            }
        }
    }
}
